package ELSA::Indexer;
use strict;
use warnings;
use Data::Dumper;
use Date::Manip;
use Socket qw(inet_aton);
use Time::HiRes qw(sleep time);
use IO::Socket::UNIX;
use Storable qw(freeze);
$Storable::canonical = 1;
use Fcntl qw(:flock);
use Sphinx::Config;
use File::Temp;
use File::Copy qw(mv);
use File::Find;
use Sys::Info; 
use Sys::Info::Constants qw( :device_cpu );
use Sys::MemInfo qw( freemem totalmem freeswap totalswap );
use Parallel::ForkManager;
use JSON;
use Net::DNS::Resolver;

use ELSA;
require Exporter;
our @ISA = qw(Exporter ELSA);
use ELSA::Exceptions;

use constant CRITICAL_LOW_MEMORY_LIMIT => 100 * 1024 * 1024;

our $Missing_table_error_limit = 4;
our $Timeout = 30;
our $Run = 1;
our $Sphinx_agent_query_timeout = 300;
our @Sphinx_extensions = qw( spp sph spi spl spm spa spk spd );
our $Index_retry_limit = 3;
our $Index_retry_time = 5;

sub new {
	my $class = shift;
	my $config_file_name = shift;
	my $id = 0; # optional id indicates that this is a forked worker Indexer using POE
	if (@_){
		$id = sprintf("%d", shift);
	}
	throw_params param => 'config_file_name', value => $config_file_name unless $config_file_name;
	my $self = $class->SUPER::new($config_file_name);
	$self->{_MISSING_TABLE_ERRORS} = 0;
	$self->{_BUFFER_COUNTER} = 0;
	$self->{_MESSAGE_IDS} = {};
	$self->{_RUN} = 1;
	$self->{_LOCKS} = {};
		
	bless ($self, $class);
	
	if ($id){
		$self->log->debug("Fork with id $id and pid $$");
		$self->{_ID} = $id;
		Log::Log4perl::NDC->remove();
		Log::Log4perl::NDC->push("WorkerID: $id");
	}
	
	$self->init_db();
	$self->init_classes();
	$self->init_cache();
	$self->{_FIELD_CONVERSIONS} = $self->get_field_conversions();
	
	# Find number of CPU's
	my $info = Sys::Info->new;
	my $cpuinfo = $info->device( CPU => () );
	$self->{_NUM_CPUS} = $cpuinfo->count;
	
	$self->log->debug("Inited in pid $$");
	
	return $self;
}



sub get_current_log_size {
	my $self = shift;
	
	my ($query, $sth);
	
	# Find current size of logs in database
	$query = "SELECT SUM(index_length+data_length) AS total_bytes\n" .
		"FROM INFORMATION_SCHEMA.tables\n" .
		"WHERE table_schema LIKE \"syslog\_%\"";
	$sth = $self->db->prepare($query);
	$sth->execute();
	my $row = $sth->fetchrow_hashref;
	my $db_size = $row->{total_bytes};
	$self->log->debug("Current size of logs in database is $db_size");
	
	# Find current size of Sphinx indexes
	my $index_size = 0;
	find(sub { $index_size += -s $File::Find::name; }, 
		$self->conf->get('sphinx/index_path'));
	$self->log->debug("Found index size of $index_size");
	
	return $db_size + $index_size;
}

sub get_current_archive_size {
	my $self = shift;
	
	my ($query, $sth);
	
	# Find current size of logs in database
	$query = "SELECT SUM(index_length+data_length) AS total_bytes\n" .
		"FROM INFORMATION_SCHEMA.tables\n" .
		"WHERE table_schema=? AND table_name LIKE \"syslogs\_archive\_%\"";
	$sth = $self->db->prepare($query);
	$sth->execute($ELSA::Data_db_name);
	my $row = $sth->fetchrow_hashref;
	my $db_size = $row->{total_bytes};
	$self->log->debug("Current size of archived logs in database is $db_size");
	
	return $db_size;
}

sub get_current_index_size {
	my $self = shift;
	
	my ($query, $sth);
	
	# Find current size of logs in database
	$query = "SELECT SUM(index_length+data_length) AS total_bytes\n" .
		"FROM INFORMATION_SCHEMA.tables\n" .
		"WHERE table_schema=? AND table_name LIKE \"syslogs\_index\_%\"";
	$sth = $self->db->prepare($query);
	$sth->execute($ELSA::Data_db_name);
	my $row = $sth->fetchrow_hashref;
	my $db_size = $row->{total_bytes};
	$self->log->debug("Current size of indexed logs in database is $db_size");
	
	# Find current size of Sphinx indexes
	my $index_size = 0;
	find(sub { $index_size += -s $File::Find::name; }, 
		$self->conf->get('sphinx/index_path'));
	$self->log->debug("Found index size of $index_size");
	
	return $db_size + $index_size;
}

# Generic log rotate command for external use
sub rotate_logs {
	my $self = shift;
	
	# Delete oldest logs as per our policy
	$self->_oversize_log_rotate();
	
	# Delete buffers that are finished
	my ($query, $sth);
	if ($self->conf->get('archive/percentage')){
		$query = sprintf('SELECT filename FROM %s.buffers WHERE archive_complete=1 AND index_complete=1', $ELSA::Meta_db_name);
	}
	else {
		$query = sprintf('SELECT filename FROM %s.buffers WHERE index_complete=1', $ELSA::Meta_db_name);
	}
	$sth = $self->db->prepare($query);
	$sth->execute();
	
	my @files;
	while (my $row = $sth->fetchrow_hashref){
		push @files, $row->{filename};
	}
	$query = sprintf('DELETE FROM %s.buffers WHERE filename=?', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	
	foreach my $file (@files){
		unlink $file;
		$self->log->debug('Deleted ' . $file);
		$sth->execute($file);
	}
}

sub initial_validate_directory {
	my $self = shift;
	my ($query, $sth);
	
	# Delete any in-progress permanent indexes
	$query = sprintf('DELETE FROM %s.indexes WHERE last_id-first_id > ? AND NOT ISNULL(locked_by)', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($self->conf->get('sphinx/perm_index_size'));
	
	# Remove any locks
	$query = sprintf("UPDATE %s.indexes SET locked_by=NULL",
		$ELSA::Meta_db_name);
	$self->db->do($query);
	
	$query = sprintf("UPDATE %s.tables SET table_locked_by=NULL",
		$ELSA::Meta_db_name);
	$self->db->do($query);
	
	# Delete finished buffers
	if ($self->conf->get('archive/percentage') and $self->conf->get('sphinx/perm_index_size')){
		$query = sprintf('DELETE FROM %s.buffers WHERE NOT ISNULL(pid) AND index_complete=1 AND archive_complete=1', $ELSA::Meta_db_name);
		$self->db->do($query);
	}
	elsif ($self->conf->get('archive/percentage')){
		$query = sprintf('DELETE FROM %s.buffers WHERE NOT ISNULL(pid) AND archive_complete=1', $ELSA::Meta_db_name);
		$self->db->do($query);
	}
	elsif ($self->conf->get('sphinx/perm_index_size')){
		$query = sprintf('DELETE FROM %s.buffers WHERE NOT ISNULL(pid) AND index_complete=1', $ELSA::Meta_db_name);
		$self->db->do($query);
	}
	else {
		$self->log->warn('Not doing archiving or indexing for some reason!');
	}
		
	$query = sprintf('UPDATE %s.buffers SET pid=NULL WHERE NOT ISNULL(pid)', $ELSA::Meta_db_name);
	$self->db->do($query);
	
	# Find any buffer files that aren't in the directory
	opendir(DIR, $self->conf->get('buffer_dir'));
	my @files;
	while (my $short_file = readdir(DIR)){
		my $file = $self->conf->get('buffer_dir') . '/' . $short_file;
		# Strip any double slashes
		$file =~ s/\/{2,}/\//g;
		push @files, $file;
	}
	closedir(DIR);
	
	# Remove any references in the database to buffers that no longer exist
	$query = sprintf('SELECT filename FROM %s.buffers', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	my @to_delete;
	while (my $row = $sth->fetchrow_hashref){
		unless (-f $row->{filename}){
			$self->log->error('File ' . $row->{filename} . ' not found');
			push @to_delete, $row->{filename};
		}
	}
	$query = sprintf('DELETE FROM %s.buffers WHERE filename=?', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	foreach my $file (@to_delete){
		$sth->execute($file);
	}
	
	$query = sprintf('SELECT pid FROM %s.buffers WHERE filename=?', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$query = sprintf('INSERT INTO %s.buffers (filename, pid) VALUES (?,?)', $ELSA::Meta_db_name);
	my $ins_sth = $self->db->prepare($query);
	$self->log->debug('files: ' . Dumper(\@files));
	my $first_id = $self->get_max_id() + 1;
	my $last_id = 0;
	foreach my $file (@files){
		$self->log->debug('considering file ' . $file);
		next if -z $file or $file =~ /\./;
		my $mtime = (stat $file)[9];
		next if ((CORE::time() - $mtime) < (2 * $self->conf->get('sphinx/index_interval') ) );
		$sth->execute($file);
		my $row = $sth->fetchrow_hashref;
		next if $row;
		eval {
			$ins_sth->execute($file, $$);
		};
		if ($@){
			my $e = $@;
			$self->log->warn('Unable to lock file ' . $file . ': ' . $e);
			next;
		}
		$self->log->debug('Found old file ' . $file . ' with mtime ' . scalar localtime($mtime));
	}
	$ins_sth->finish();
	
	$self->validate_directory();
	
	return 1;
}

sub validate_directory {
	my $self = shift;
	my ($query, $sth);
	
	# DEFINITELY going to need a directory lock for this
	$self->get_lock('directory');
	
	# Validate that all real tables are accounted for in the directory
	$query = sprintf("INSERT INTO %s.tables (table_name, start, end, min_id, max_id, table_type_id) VALUES (?,?,?,?,?," .
		"(SELECT id FROM table_types WHERE table_type=?))",
		$ELSA::Meta_db_name);
	my $ins_tables_sth = $self->db->prepare($query);
	
	$query = sprintf("SELECT CONCAT(t1.table_schema, \".\", t1.table_name) AS real_table,\n" .
		"t2.table_name AS recorded_table\n" .
		"FROM INFORMATION_SCHEMA.TABLES t1\n" .
		"LEFT JOIN %s.tables t2 ON (CONCAT(t1.table_schema, \".\", t1.table_name)=t2.table_name)\n" .
		"WHERE t1.table_schema=\"%s\" HAVING ISNULL(recorded_table)",
		$ELSA::Meta_db_name, $ELSA::Data_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	
	while (my $needed_row = $sth->fetchrow_hashref){
		my $full_table = $needed_row->{real_table};
		my $table_type = 'index';
		if ($full_table =~ /archive/){
			$table_type = 'archive';
		}
		$self->log->debug("Directory is missing table $full_table");
		
		my ($start, $end, $min, $max);
		if ($table_type eq 'index'){		
			# Find our start,end,min_id,max_id
			$query = sprintf("SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM %s",
				$full_table);
			$sth = $self->db->prepare($query);
			$sth->execute();
			my $row = $sth->fetchrow_hashref;
			($min, $max) = ($row->{min_id}, $row->{max_id});
			$query = sprintf("SELECT FROM_UNIXTIME(timestamp) AS timestamp FROM %s WHERE id=?",
				$full_table);
			$sth = $self->db->prepare($query);
			$sth->execute($min);
			$row = $sth->fetchrow_hashref;
			$start = $row->{timestamp};
			$sth->execute($max);
			$row = $sth->fetchrow_hashref;
			$end = $row->{timestamp};
		}
		else { #archive
			$query = sprintf("SELECT MIN(id) AS min_id, MAX(id) AS max_id, " .
				"FROM_UNIXTIME(MIN(timestamp)) AS start, FROM_UNIXTIME(MAX(timestamp)) AS end FROM %s",
				$full_table);
			$sth = $self->db->prepare($query);
			$sth->execute();
			my $row = $sth->fetchrow_hashref;
			($min, $max, $start, $end) = ($row->{min_id}, $row->{max_id}, $row->{start}, $row->{end});
		}
		
		# Finally, insert into tables
		$self->log->debug("Adding $full_table with start $start, end $end, min $min, max $max");
		$ins_tables_sth->execute($full_table, $start, $end, $min, $max, $table_type);
	}
	$ins_tables_sth->finish();
	
	$query = sprintf("DELETE FROM %s.tables WHERE table_name=?", 
		$ELSA::Meta_db_name);
	my $del_sth = $self->db->prepare($query);

	# Validate that all tables in the directory are real	
	#TODO We could probably do this in one big DELETE ... SELECT statement
	$query = sprintf("SELECT t1.table_name AS recorded_table,\n" .
		"CONCAT(t2.table_schema, \".\", t2.table_name) AS real_table\n" .
		"FROM %s.tables t1\n" .
		"LEFT JOIN INFORMATION_SCHEMA.TABLES t2 ON (CONCAT(t2.table_schema, \".\", t2.table_name)=t1.table_name)\n" .
		"HAVING ISNULL(real_table)",
		$ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	while (my $row = $sth->fetchrow_hashref){
		$self->log->error("Found directory entry for non-existent table: " .
			$row->{recorded_table});
		$del_sth->execute($row->{recorded_table});
	}
	
	# Validate that no tables overlap
	$query = sprintf('SELECT t1.id, t1.table_name, t1.min_id, t1.max_id, t2.min_id AS trim_to_max' . "\n" .
		'FROM %1$s.tables t1, %1$s.tables t2' . "\n" .
		'WHERE t1.table_type_id=(SELECT id FROM %1$s.table_types WHERE table_type="index")' . "\n" .
		'AND t2.table_type_id=(SELECT id FROM %1$s.table_types WHERE table_type="index")' . "\n" .
		'AND t1.table_name!=t2.table_name' . "\n" .
		'AND t1.max_id BETWEEN t2.min_id AND t2.max_id', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	while (my $row = $sth->fetchrow_hashref){
		$self->log->error('Found duplicate IDs from ' . $row->{trim_to_max} . ' to ' . $row->{max_id});
		
		# Delete the older of the duplicate ID's
		$query = sprintf('DELETE FROM %s WHERE id >= ?', $row->{table_name});
		my $sth = $self->db->prepare($query);
		$sth->execute($row->{trim_to_max});
		$sth->finish();
		
		# Update the directory
		$query = sprintf('UPDATE %s.tables SET max_id=? WHERE id=?', $ELSA::Meta_db_name);
		$sth = $self->db->prepare($query);
		$sth->execute(($row->{trim_to_max} - 1), $row->{id});
		$sth->finish();
	}
	
	# Validate that index tables still have an index pointing to them
	$query = sprintf("SELECT table_name FROM %s.v_directory WHERE table_type=\"index\" AND ISNULL(id)",
		$ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	
	while (my $row = $sth->fetchrow_hashref){
		$self->log->error("Found index directory entry for unindexed table: " .
			$row->{table_name});
		$del_sth->execute($row->{table_name});
		$self->log->error('Dropping unindexed index table ' . $row->{table_name});
		$self->db->do('DROP TABLE ' . $row->{table_name});
	}
	
	$del_sth->finish();
	
	# Explicitly index the dummy index entries for non-existent indexes
	$query = sprintf("SELECT id, type FROM %s.indexes", $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	my %existing;
	while (my $row = $sth->fetchrow_hashref){
		$existing{ $self->get_index_name($row->{type}, $row->{id}) } = 1;
	}
	
	for (my $i = 1 ; $i <= $self->conf->get('num_indexes'); $i++){
		foreach my $type qw(temporary permanent){
			my $index_name = $self->get_index_name($type, $i);
			unless ($existing{ $index_name }){
				$self->log->debug('Wiping via index ' . $index_name);
				$self->_sphinx_index( $index_name );
			}
		}
	}
	
	$self->log->trace('Finished wiping indexes');

	# Find tables which are not referred to by any index
	$query = sprintf('SELECT t1.table_name AS full_table FROM %1$s.tables t1 ' .
		'LEFT JOIN %1$s.v_directory t2 ON (t1.id=t2.table_id) ' .
		'WHERE ISNULL(t2.table_id)', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	$self->log->trace('sth executed');
	$query = sprintf('DELETE FROM %s.tables WHERE table_name=?', $ELSA::Meta_db_name);
	$del_sth = $self->db->prepare($query);
	while (my $row = $sth->fetchrow_hashref){
		$self->log->info('Dropping unindexed table ' . $row->{full_table});
		$self->db->do('DROP TABLE ' . $row->{full_table});
		$del_sth->execute($row->{full_table});
	}
	
	$self->release_lock('directory');
			
	return 1;
}

sub _oversize_log_rotate {
	my $self = shift;
	
	my ($query, $sth);
	
	my $archive_size_limit = $self->conf->get('log_size_limit') * $self->conf->get('archive/percentage') * .01;
	while ($self->get_current_archive_size() > $archive_size_limit){
		$self->get_lock('directory');
		
		# Get our latest entry
		$query = sprintf("SELECT id, first_id, last_id, type, table_type, table_name FROM %s.v_directory\n" .
			"WHERE table_type=\"archive\" AND ISNULL(locked_by)\n" .
			"ORDER BY start ASC LIMIT 1", $ELSA::Meta_db_name);
		$sth = $self->db->prepare($query);
		$sth->execute();
		my $entry = $sth->fetchrow_hashref;
		
		my $full_table = $entry->{table_name};
		$self->log->debug("Dropping table $full_table");
		$query = sprintf("DROP TABLE %s", $full_table);
		$self->db->do($query);
		$query = sprintf("DELETE FROM %s.tables WHERE table_name=?", $ELSA::Meta_db_name);
		$sth = $self->db->prepare($query);
		$sth->execute($full_table);
		$self->release_lock('directory');
	}
	
	# Drop indexed data
	while ($self->get_current_index_size() > ($self->{_LOG_SIZE_LIMIT} - $archive_size_limit)){
		$self->get_lock('directory');
		
		# Get our latest entry
		$query = sprintf("SELECT id, first_id, last_id, type, table_type, table_name FROM %s.v_directory\n" .
			"WHERE table_type=\"index\" AND ISNULL(locked_by)\n" .
			"ORDER BY start ASC LIMIT 1", $ELSA::Meta_db_name);
		$sth = $self->db->prepare($query);
		$sth->execute();
		my $entry = $sth->fetchrow_hashref;
		
		$self->log->debug("Dropping old entries because current log size larger than " 
			. ($self->{_LOG_SIZE_LIMIT} - $archive_size_limit));
		unless ($entry){
			$self->log->error("no entries, current log size: " . $self->get_current_index_size());
			#$self->db->rollback();
			$self->release_lock('directory');
			last;
		}
		
		$query = sprintf('UPDATE %s.indexes SET locked_by=? WHERE id=? AND type=?', $ELSA::Meta_db_name);
		$sth = $self->db->prepare($query);
		$sth->execute($$, $entry->{id}, $entry->{type});
		$self->release_lock('directory');
		
		$self->log->debug("Dropping index " . $entry->{id});
		# drop_indexes drops the table as necessary
		$self->drop_indexes($entry->{type}, [$entry->{id}]);
	}
			
	return 1;
}

sub load_records {
	my $self = shift;
	my $args = shift;
	
	throw_params param => 'args', value => $args
		unless $args and ref($args) eq 'HASH';
	throw_params param => 'file', value => $args->{file}
		unless $args->{file} and $args->{file};
	
	my $load_only = 0;
	if ($args->{load_only}){
		$load_only = 1;
	}
	$self->log->debug("args: " . Dumper($args));
	
	$self->get_lock('directory') or throw_e error => 'Unable to obtain lock';
		
	# Create table
	my $full_table = $self->create_table($args);
	my ($db, $table) = split(/\./, $full_table);
	
	my ($query, $sth);
	
	# Re-verify that this file still exists (some other process may have swiped it out from under us)
	unless (-f $args->{file}){
		$self->log->error('File ' . $args->{file} . ' does not exist, not loading.');
		return 0;
	}
	
	# Update the database to show that this child is working on it
	$query = sprintf('UPDATE %s.buffers SET pid=? WHERE filename=?', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($$, $args->{file});
	
	$query = sprintf('UPDATE %s.tables SET table_locked_by=? WHERE table_name=?', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($$, $full_table);
	
	$self->release_lock('directory');
	
	my $load_start = time();
	# CONCURRRENT allows the table to be open for reading whilst the LOAD DATA occurs so that queries won't stack up
	$query = sprintf('LOAD DATA CONCURRENT LOCAL INFILE "%s" INTO TABLE %s', $args->{file}, $full_table);
	$sth = $self->db->prepare($query);
	$sth->execute();
	my $records = $sth->rows();
	my $load_time = time() - $load_start;
	my $rps = $records / $load_time;

	$query = sprintf("SELECT MAX(id) AS max_id FROM %s", $full_table);
	$sth = $self->db->prepare($query);
	$sth->execute() or throw_e error => $self->db->errstr;
	my $row = $sth->fetchrow_hashref;
	my $last_id = $row->{max_id};
	my $first_id = $row->{max_id} - $records + 1;
	
#	$self->log->debug("Found max_id of $row->{max_id}, should have $args->{last_id}");
	$self->log->debug("Loaded $records records in $load_time seconds ($rps per second)");
	
	# Find out what our min/max timestamps are by getting the records at min/max id
	$query = sprintf('SELECT timestamp FROM %s WHERE id=?', $full_table);
	$sth = $self->db->prepare($query);
	$sth->execute($first_id);
	$row = $sth->fetchrow_hashref;
	my $start = 0;
	if ($row){
		$start = $row->{timestamp};
	}
	else {
		throw_e error => 'Unable to get a start timestamp from table ' . $full_table . ' with row id ' . $first_id;
	}
	$sth->execute($last_id);
	$row = $sth->fetchrow_hashref;
	my $end = 0;
	if ($row){
		$end = $row->{timestamp};
	}
	else {
		throw_e error => 'Unable to get an end timestamp from table ' . $full_table . ' with row id ' . $first_id;
	}
	
	$self->get_lock('directory') or throw_e error => 'Unable to obtain lock';
	
	# Update the directory with our new buffer start (if it is earlier than what's already there)
	$query = sprintf('SELECT UNIX_TIMESTAMP(start) AS start, UNIX_TIMESTAMP(end) AS end FROM %s.tables WHERE table_name=?', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($full_table);
	$row = $sth->fetchrow_hashref;
	if ($row->{start} > $start){
		$query = sprintf('UPDATE %s.tables SET start=FROM_UNIXTIME(?) WHERE table_name=?', $ELSA::Meta_db_name);
		$sth = $self->db->prepare($query);
		$sth->execute($start, $full_table);
		$self->log->debug('Updated table to have start ' . $start);
	}
	if ($row->{end} < $end){
		$query = sprintf('UPDATE %s.tables SET end=FROM_UNIXTIME(?) WHERE table_name=?', $ELSA::Meta_db_name);
		$sth = $self->db->prepare($query);
		$sth->execute($end, $full_table);
		$self->log->debug('Updated table to have end ' . $end)
	}
	$query = sprintf("UPDATE %s.tables SET max_id=?, table_locked_by=NULL WHERE table_name=?", $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($last_id, $full_table);
	
	$self->log->debug('Updated table to have end ' . $end . ', max_id ' . $last_id . ' table_name ' . $full_table);
	
	# Mark load complete
	$query = sprintf('UPDATE %s.buffers SET index_complete=1 WHERE filename=?', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($args->{file});
	
	# Record the load stats
	$query = sprintf('INSERT INTO stats (type, bytes, count, time) VALUES("load", ?,?,?)', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute((-s $args->{file}), $records, $load_time);
	
	$self->release_lock('directory');
	
	return { first_id => $first_id, last_id => $last_id };
}

sub archive_records {
	my $self = shift;
	my $args = shift;
	
	throw_params param => 'args', value => $args
		unless $args and ref($args) eq 'HASH';
	throw_params param => 'file', value => $args->{file}
		unless $args->{file} and $args->{file};
		
	my $override = 0;
	if ($args->{override}){
		$override = 1;
	}
	$args->{archive} = 1;
	
	$self->log->debug("args: " . Dumper($args));
		
	# Create table
	my $full_table = $self->create_table($args);
	my ($db, $table) = split(/\./, $full_table);
	
	my ($query, $sth);
	
	# Re-verify that this file still exists (some other process may have swiped it out from under us)
	unless (-f $args->{file}){
		$self->log->error('File ' . $args->{file} . ' does not exist, not loading.');
		return 0;
	}
	
	my $load_start = time();
	$query = sprintf('LOAD DATA CONCURRENT LOCAL INFILE "%s" INTO TABLE %s', $args->{file}, $full_table);
	$sth = $self->db->prepare($query);
	$sth->execute();
	my $records = $sth->rows();
	my $load_time = time() - $load_start;
	my $rps = $records / $load_time;

	$query = sprintf('SELECT id FROM %s LIMIT 1', $full_table);
	$sth = $self->db->prepare($query);
	$sth->execute();
	my $row = $sth->fetchrow_hashref;
	my $first_id = $self->conf->get('peer_id_multiplier') * $self->conf->get('manager/server_id');
	if ($row){
		$first_id = $row->{id};
	}
	
	$query = 'SELECT table_rows FROM INFORMATION_SCHEMA.tables WHERE table_schema=? AND table_name=?';
	$sth = $self->db->prepare($query);
	$sth->execute($db, $table);
	$row = $sth->fetchrow_hashref;
	my $last_id = $first_id;
	if ($row){
		$last_id = $first_id + $row->{table_rows};
	}

#	$self->log->debug("Found max_id of $row->{max_id}, should have $args->{last_id}");
	$self->log->debug("Loaded $records records in $load_time seconds ($rps per second)");
	#TODO find an efficient but correct way of finding this out
	my $end = CORE::time();
	
	# Update the directory with our new buffer start (if it is earlier than what's already there)
	$query = sprintf("UPDATE %s.tables SET end=FROM_UNIXTIME(?), max_id=? WHERE table_name=?", $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($end, $last_id, $full_table);
	
	$self->log->debug('Updated table to have end ' . $end . ', max_id ' . $last_id . ' table_name ' . $full_table);
	
	# Mark archiving complete
	$query = sprintf('UPDATE %s.buffers SET archive_complete=1 WHERE filename=?', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($args->{file});
	
	# Record the load stats
	$query = sprintf('INSERT INTO stats (type, bytes, count, time) VALUES("archive", ?,?,?)', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute((-s $args->{file}), $records, $load_time);
	
	return { first_id => $first_id, last_id => $last_id };
}

sub get_max_id {
	my $self = shift;
	my ($query, $sth, $row);
	
	# Find db's current max id
	$query = sprintf("SELECT MAX(max_id) AS max_id FROM %s.tables", $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	$row = $sth->fetchrow_hashref;
	my $max_id = $row->{max_id};
	$max_id = 0 unless $max_id;
	
	# Validate this is with the correct range for this node
	my $min_id = $self->conf->get('manager/server_id') * $self->conf->get('peer_id_multiplier');
	unless ($max_id > $min_id){
		$self->log->warn('Found max_id of ' . $max_id . ' which was smaller than min_id of ' . $min_id . ', setting to ' . $min_id);
		$max_id = $min_id;
	}
	
	return $max_id;
}

sub create_table {
	my $self = shift;
	my $args = shift;
	throw_params param => 'args', value => Dumper($args)
		unless $args and ref($args) eq 'HASH';
			
	my ($query, $sth, $row);
			
	my $needed = {};
	my $start_time = 0;
	my $end_time = 0;
	
	# See if the tables already exist
	$args = $self->get_table($args);
	my $needed_table = $args->{table_name};
	
	my ($db, $table) = split(/\./, $needed_table);
	# Get list of current tables for our db
	$query = "SELECT table_name FROM INFORMATION_SCHEMA.tables WHERE table_schema=? AND table_name=?";
	$sth = $self->db->prepare($query);
	$sth->execute($db, $table);
	$row = $sth->fetchrow_hashref;
	if ($row){
		# We don't need to create a table
		$self->log->trace("Table $needed_table exists");
		return $needed_table;
	}
		
	$self->log->debug("Creating table $needed_table");
	
	# Find the max id currently in the directory and use that to determine the autoinc value
	my $current_max_id = $self->get_max_id();
	eval {
		$query = sprintf("INSERT INTO %s.tables (table_name, start, end, min_id, max_id, table_type_id)\n" .
			"VALUES( ?, FROM_UNIXTIME(?), FROM_UNIXTIME(?), ?, ?, (SELECT id FROM %1\$s.table_types WHERE table_type=?) )",
			$ELSA::Meta_db_name);
		$sth = $self->db->prepare($query);
		$sth->execute( $needed_table, $args->{start}, $args->{end}, $current_max_id + 1, $current_max_id + 1, $args->{table_type});
		my $id = $self->db->{mysql_insertid};
		$self->log->debug(sprintf("Created table id %d with start %s, end %s, first_id %lu, last_id %lu", 
			$id, epoch2iso($args->{start}), epoch2iso($args->{end}), $args->{first_id}, $args->{last_id} ));	
		
		$query = sprintf("CREATE TABLE IF NOT EXISTS %s LIKE %s.syslogs_template",
				$needed_table, $ELSA::Meta_db_name);
		$self->log->debug("Creating table: $query");
		$self->db->do($query);
		
		$query = sprintf('ALTER TABLE %s AUTO_INCREMENT=%lu', $needed_table, $current_max_id + 1);
		if ($args->{archive}){
			$query .= ' ENGINE=ARCHIVE';
		}
		$self->db->do($query);
	};
	if ($@){
		my $e = $@;
		if ($e->sql_error =~ /Duplicate entry/){
			# This is fine
			$e->caught();
			return $needed_table;
		}
		else {
			$e->throw(); # whatever it is, it's not cool
		}
		
	}
	return $needed_table;
}

sub consolidate_indexes {
	my $self = shift;
	my $args = shift;
	throw_params param => 'args', value => Dumper($args)
		unless $args and ref($args) eq 'HASH';
		
	my ($query, $sth, $row);
	
	my ($first_id, $last_id, $table);
	
	$self->get_lock('directory') or throw_e error => 'Unable to obtain lock';
	
	$self->log->debug("Consolidating indexes with args " . Dumper($args));
	if ($args->{table}){
		$table = $args->{table};
		$self->log->debug('Consolidating table ' . $table);
		$query = sprintf('SELECT min_id, max_id FROM %s.tables WHERE table_name=?', $ELSA::Meta_db_name);
		$sth = $self->db->prepare($query);
		$sth->execute($table);
		$row = $sth->fetchrow_hashref;
		if ($row){
			$self->log->debug('Consolidating indexes from ' . $row->{min_id} . ' to ' . $row->{max_id});
			$first_id = $row->{min_id};
			$last_id = $row->{max_id};	
		}
		else {
			$self->release_lock('directory');
			throw_e error => 'Unable to find rows to index for table ' . $args->{table};
		}
	}
	elsif ($args->{first_id} and $args->{last_id}){
		$self->log->debug('Consolidating indexes from ' . $args->{first_id} . ' to ' . $args->{last_id});
		$first_id = $args->{first_id};
		$last_id = $args->{last_id};	
	}
	else {
		$self->release_lock('directory');
		throw_params param => 'args', value => Dumper($args);
	}
	
	$query = sprintf('SELECT COUNT(*) AS count FROM %s.v_directory ' . "\n" .
			'WHERE table_type="index" AND min_id >= ? AND max_id <= ?', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($first_id, $last_id);
	$row = $sth->fetchrow_hashref;
	if ($row->{count} == 1){
		$self->log->warn('Attempting to consolidate table that is already being consolidated');
		$self->release_lock('directory');
		return 0;
	}
	
	$query = sprintf('SELECT table_name, table_locked_by FROM %s.v_directory ' . "\n" .
			'WHERE table_type="index" AND min_id >= ? AND max_id <= ?', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($first_id, $last_id);
	$row = $sth->fetchrow_hashref;
	unless ($row){
		$self->log->warn('Rows not found');
		$self->release_lock('directory');
		return 0;
	}
	$table = $row->{table_name};
	
	$self->release_lock('directory');
	
	# Do the indexing
	my $replaced = $self->index_records({first_id => $first_id, last_id => $last_id});
	
	$self->get_lock('directory') or throw_e error => 'Unable to obtain lock';
	
	# Unlock the table we're consolidating
	$query = sprintf('UPDATE %s.tables SET table_locked_by=NULL WHERE table_name=?', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($table);
	$self->log->debug('Unlocked table ' . $table);
	
	$self->release_lock('directory');
		
	# Validate our directory after this to be sure there's nothing left astray
	$self->validate_directory();
}

sub get_lock {
	my $self = shift;
	my $lock_name = shift;
	my $lock_timeout = shift;
	$lock_timeout ||= 120;
	
	my $ok;
	my $lockfile = $self->conf->get('lockfile_dir') . '/' . $lock_name;
	eval {
		open($self->{_LOCKS}->{$lock_name}, $lockfile) or die('Unable to open ' . $lockfile . ': ' . $!);
		$ok = flock($self->{_LOCKS}->{$lock_name}, LOCK_EX);
	};
	if ($@){
		$self->log->error('locking error: ' . $@);
	}
	unless ($ok){
		return 0;
	}
	$self->log->trace('Locked ' . $lock_name);
	return 1;
}

sub release_lock {
	my $self = shift;
	my $lock_name = shift;
	
	my $ok;
	my $lockfile = $self->conf->get('lockfile_dir') . '/' . $lock_name;
	eval {
		open($self->{_LOCKS}->{$lock_name}, $lockfile) or die('Unable to open ' . $lockfile . ': ' . $!);
		$ok = flock($self->{_LOCKS}->{$lock_name}, LOCK_UN);
		close($self->{_LOCKS}->{$lock_name});
	};
	if ($@){
		$self->log->error('locking error: ' . $@);
	}
	unless ($ok){
		throw_e error => 'Unable to release lock';
	}
	$self->log->trace('Unlocked ' . $lock_name);
	return 1;
}

sub index_records {
	my $self = shift;
	my $args = shift;
	throw_params param => 'args', value => Dumper($args)
		unless $args and ref($args) eq 'HASH';
	throw_params param => 'first_id', value => Dumper($args)
		unless $args->{first_id};
	throw_params param => 'last_id', value => Dumper($args)
		unless $args->{last_id};
		
	$self->log->debug("Indexing with args " . Dumper($args));
	
	my ($query, $sth, $row);
	
	$self->get_lock('directory') or throw_e error => 'Unable to obtain lock';
	
	# Verify these records are unlocked
	$query = sprintf("SELECT locked_by\n" .
		"FROM %s.v_directory\n" .
		"WHERE table_type=\"index\" AND (? BETWEEN first_id AND last_id\n" .
		"OR ? BETWEEN first_id AND last_id\n" .
		"OR (first_id > ? AND last_id < ?))\n" .
		"ORDER BY id ASC",
		$ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($args->{first_id}, $args->{last_id}, $args->{first_id}, $args->{last_id});
	while ($row = $sth->fetchrow_hashref){
		if ($row->{locked_by} and $row->{locked_by} != $$){
			$self->release_lock('directory');
			throw_e error => 'Cannot do this indexing because index or table is locked: ' . Dumper($row);
		}
	}
	
	# Check to see if this will replace any smaller indexes (this happens during index consolidation)
	$query = sprintf("SELECT id, first_id, last_id, start, end, type FROM %s.v_directory\n" .
		"WHERE first_id >= ? and last_id <= ?",
		$ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($args->{first_id}, $args->{last_id});
	my %replaced;
	while (my $row = $sth->fetchrow_hashref){
		unless ($replaced{ $row->{type} }){
			$replaced{ $row->{type} } = {};
		}
		$replaced{ $row->{type} }->{ $row->{id} } = 1;
		$self->log->debug("Replacing " . $row->{type} . " index " . $row->{id});
	}
	
	# Check to see if ram limitations dictate that these should be small permanent tables since they consume no ram
	my $index_type = 'temporary';
	if ($self->_over_mem_limit()){
		$self->log->warn('Resources overlimit, using permanent index for this emergency');
		$index_type = 'permanent';
	}
	elsif (scalar keys %replaced or ($args->{last_id} - $args->{first_id}) > $self->conf->get('sphinx/perm_index_size')){
		$self->log->debug('Size dictates permanent index');
		$index_type = 'permanent';
	}
	
	my $next_index_id = $self->_get_next_index_id($index_type);
	
	# Lock these indexes to make sure a different process does not try to replace them
	$query = sprintf("UPDATE %s.indexes SET locked_by=?\n" .
		"WHERE first_id >= ? and last_id <= ? AND ISNULL(locked_by)", 
		$ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($$, $args->{first_id}, $args->{last_id});
	$self->log->trace('Locked indexes between ' . $args->{first_id} . ' and ' . $args->{last_id});
	
	# Find the table(s) we'll be indexing
	my $table;
	$query = sprintf("SELECT DISTINCT table_id AS id, table_name, IF(min_id < ?, ?, min_id) AS min_id,\n" .
		"IF(max_id > ?, ?, max_id) AS max_id\n" .
		"FROM %s.v_directory\n" .
		"WHERE table_type=\"index\" AND (? BETWEEN min_id AND max_id\n" .
		"OR ? BETWEEN min_id AND max_id\n" .
		"OR (min_id > ? AND max_id < ?))\n" .
		"ORDER BY id ASC",
		$ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($args->{first_id}, $args->{first_id},
	 	$args->{last_id},$args->{last_id},
		$args->{first_id}, 
		$args->{last_id}, 
		$args->{first_id}, $args->{last_id});
	my @tables_needed;
	while ($row = $sth->fetchrow_hashref){
		push @tables_needed, $row;
	}
	$self->log->trace("Tables needed: " . Dumper(\@tables_needed));
	
	# There should be exactly one table
	if (scalar @tables_needed > 1){		
		# Recursively do each table in a separate run
		foreach my $row (@tables_needed){
			$self->index_records({ first_id => $row->{min_id}, last_id => $row->{max_id} });
		}
		
		return 1;
	}
	elsif (scalar @tables_needed == 1) {
		$table = $tables_needed[0]->{table_name};
		$self->log->debug("Indexing rows from table $table");
	}
	else {
		$query = sprintf("UPDATE %s.indexes SET locked_by=NULL WHERE locked_by=?",
			$ELSA::Meta_db_name);
		$sth = $self->db->prepare($query);
		$sth->execute($$);
		
		$query = sprintf("SELECT * FROM %s.tables", $ELSA::Meta_db_name);
		$sth = $self->db->prepare($query);
		$sth->execute();
		my $tmp_hash = $sth->fetchall_hashref('id');
		$self->release_lock('directory');
		throw_e error => "No tables found for first_id $args->{first_id} and last_id $args->{last_id}" .
		 ", tables in database: " . Dumper($tmp_hash);
	}
	
	my ($count, $start, $end);
	$count = ($args->{last_id} - $args->{first_id});
	# This will be much faster than finding the timestamps above since timestamp is not indexed
	$query = sprintf("SELECT timestamp FROM %s WHERE id=?", $table);
	$sth = $self->db->prepare($query);
	$sth->execute($args->{first_id});
	$row = $sth->fetchrow_hashref;
	$start = $row->{timestamp};
	$sth->execute($args->{last_id});
	$row = $sth->fetchrow_hashref;
	$end = $row->{timestamp};
	
	$self->log->debug("Data table info: $count, $start, $end");
	#unless ($count > 0){
	unless ($args->{last_id} >= $args->{first_id}){
		$self->release_lock('directory');
		
		throw_e error => "Unable to find rows we're about to index, only got $count rows " .
			"from table $table " .
			"with ids $args->{first_id} and $args->{last_id} a difference of " . ($args->{last_id} - $args->{first_id});
	}
	
	# Update the index table
	$query = sprintf("REPLACE INTO %1\$s.indexes (id, start, end, first_id, last_id, table_id, type, locked_by)\n" .
		"VALUES(?, ?, ?, ?, ?, (SELECT id FROM %1\$s.tables WHERE table_name=?), ?, ?)", 
		$ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($next_index_id, $start, $end, $args->{first_id}, $args->{last_id}, 
		$table, $index_type, $$);
	$self->release_lock('directory');
	
	$self->log->debug("Inserted into indexes: " . join(", ", $next_index_id, $start, $end, $args->{first_id}, $args->{last_id}, $table, $index_type, $$));
			
	# Now actually perform the indexing
	my $start_time = time();
	
	my $index_name = $self->get_index_name($index_type, $next_index_id);
	
	my $stats = $self->_sphinx_index($index_name);

	# Delete the replaced indexes
	foreach my $type (keys %replaced){
		$self->log->debug("Dropping indexes " . join(", ", sort keys %{ $replaced{$type} }));
		$self->drop_indexes($type, [ sort keys %{ $replaced{$type} } ]);
	}
	
	$self->get_lock('directory') or throw_e error => 'Unable to obtain lock';
		
	# Unlock the indexes we were working on
	$query = sprintf("UPDATE %s.indexes SET locked_by=NULL WHERE locked_by=?",
		$ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($$);
	$self->log->trace('Unlocked indexes between ' . $args->{first_id} . ' and ' . $args->{last_id});
	
	# Update the stats table
	if ($stats and ref($stats) and ref($stats) eq 'HASH'){
		$query = sprintf('INSERT INTO %s.stats (type, bytes, count, time) VALUES ("index", ?,?,?)', $ELSA::Meta_db_name);
		$sth = $self->db->prepare($query);
		$sth->execute($stats->{bytes}, $stats->{docs}, (time() - $start_time));
	}
	
	$self->release_lock('directory');
	
	return \%replaced;
}

sub _over_num_index_limit {
	my $self = shift;
	my ($query, $sth);
	# Find the percentage of indexes that are temporary
	$query = sprintf('SELECT COUNT(*) AS count FROM %s.indexes WHERE type="temporary" and ISNULL(locked_by)',
		$ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	my $row = $sth->fetchrow_hashref;
	my $num_temps = 0;
	if ($row){
		$num_temps = $row->{count};
	}
	my $percent_temp = int($num_temps / $self->conf->get('num_indexes') * 100);
	if ($percent_temp > $self->conf->get('sphinx/allowed_temp_percent') ){
		 $self->log->warn('percent of temporary indexes is ' . $percent_temp . ' which is greater than '
			. $self->conf->get('sphinx/allowed_temp_percent'));
		return 1;
	}
	return 0;
}

sub _over_mem_limit {
	my $self = shift;
	
	my ($query, $sth);
	
	# Find out how much memory we've got in comparison with how much Sphinx is using
	#my $total_used = $self->_get_mem_used_by_sphinx();
	my $total_mem = totalmem() + totalswap();
	my $total_free = freemem() + freeswap();
	
	my $index_sizes = $self->_get_sphinx_index_sizes();
	$query = sprintf("SELECT id, type\n" .
		"FROM %s.indexes WHERE ISNULL(locked_by) AND type=\"temporary\"\n",
		$ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	my $total_temp_size = 0;
	while (my $row = $sth->fetchrow_hashref){
		my $index_name = $self->get_index_name($row->{type}, $row->{id});
		$total_temp_size += $index_sizes->{$index_name};
	}
			
	# Check if we're over anything
	if ( (($total_temp_size / $total_mem) * 100) > $self->conf->get('sphinx/allowed_mem_percent')){
		$self->log->warn('Total mem used: ' . $total_temp_size . ' of ' . $total_mem 
			. ', which is greater than ' . $self->conf->get('sphinx/allowed_mem_percent') . ' allowed percent');
		return 1;
	}
	elsif ($total_free < CRITICAL_LOW_MEMORY_LIMIT){
		$self->log->warn('system only has ' . $total_free . ' memory available');
		return 1;
	}
	
	return 0;
}

sub _get_sphinx_index_sizes {
	my $self = shift;
	# Find the size of all .spa (attributes) and .spi (dictionary) files
	opendir(DIR, $self->conf->get('sphinx/index_path'));
	my $sizes = {};
	while (my $file = readdir(DIR)){
		if ($file =~ /\.sp(a|i)$/){
			my @stat = stat($self->conf->get('sphinx/index_path') . '/' . $file);
			my @tokens = split(/\./, $file);
			my $prefix = $tokens[0];
			$sizes->{$prefix} += $stat[7];
		}
	}
	return $sizes;
}

sub _sphinx_index {
	my $self = shift;
	my $index_name = shift;
	
	my $start_time = time();
	my $cmd = sprintf("%s --config %s --rotate %s 2>&1", 
		$self->conf->get('sphinx/indexer'), $self->conf->get('sphinx/config_file'), $index_name);
	my @output = qx/$cmd/;
	$self->log->debug('num of output lines: ' . scalar @output);
	$self->log->debug('output: ' . join("\n", @output));
	my $collected = 0;
	my $bytes = 0;
	my $retries = 0;
	$self->log->trace('ran cmd: ' . $cmd);
	TRY_LOOP: while (!$collected){
		LINE_LOOP: foreach (@output){
			chomp;
			$self->log->trace('output: ' . $_);
			#if (/collected\s+(\d+)\s+docs/){
			if (/^total (\d+) docs, (\d+) bytes$/){
				$collected = $1;
				$bytes = $2;
				last TRY_LOOP;
			}
			elsif (/FATAL: failed to lock/){
				$self->log->warn("Indexing error: $_, retrying in $Index_retry_time seconds");
				sleep $Index_retry_time;
				@output = qx/$cmd/;
				last LINE_LOOP;
			}
		}
		$retries++;
		if ($retries > $Index_retry_limit){
			$self->log->error("Hit retry limit of $Index_retry_limit");
			last TRY_LOOP;
		}
	}
	
	my $index_time = (time() - $start_time);
	unless ($collected){
		$self->log->error("Indexing didn't work for $index_name, output: " . Dumper(\@output));
	}
	
	$self->log->debug(sprintf("Indexed %d rows in %.5f seconds (%.5f rows/sec)", 
		$collected, $index_time, $collected / (time() - $start_time), #Nah, this will never be zero, right?
	));
	return {
		docs => $collected,
		bytes => $bytes,
	};
}

sub drop_indexes {
	my $self = shift;
	my $type = shift;
	my $ids = shift;
	
	throw_params param => 'ids', value => Dumper($ids)
		unless $ids and ref($ids) eq 'ARRAY';
		
	my ($query, $sth);
	
	my $sphinx_dir = $self->conf->get('sphinx/index_path');
	
	$self->get_lock('directory') or throw_e error => 'Unable to obtain lock';
	
	# Delete from database
	foreach my $id (@$ids){
		$self->log->debug("Deleting index $id from DB");
		
		$query = sprintf("SELECT first_id, last_id, table_name FROM %s.v_directory WHERE id=? AND type=?", $ELSA::Meta_db_name);
		$sth = $self->db->prepare($query);
		$sth->execute($id, $type);
		#$self->log->trace('executed');
		my $row = $sth->fetchrow_hashref;
		if ($row){
			my $full_table = $row->{table_name};
			$query = sprintf("DELETE FROM %s.indexes WHERE id=? AND locked_by=? AND type=?", $ELSA::Meta_db_name);
			$sth = $self->db->prepare($query);
			$sth->execute($id, $$, $type);
			#$self->log->trace('executed id ' . $id);
			unless ($sth->rows){
				$self->log->warn('id ' . $id . ' was not found or locked by a different pid');
				next;
			}
			
			# Drop the table if necessary.  This query returns nothing if no indexes refer to a given table.
			$self->log->debug("Checking if we need to drop $full_table");
			$query = sprintf("SELECT * FROM %s.v_directory WHERE table_name=? AND NOT ISNULL(id)", $ELSA::Meta_db_name);
			$sth = $self->db->prepare($query);
			$sth->execute($full_table);
			#$self->log->trace('executed full_table ' . $full_table);
			$row = $sth->fetchrow_hashref;
			if ($row){
				$self->log->debug('At least one entry still exists for table '. $full_table . ': ' . Dumper($row));
			}
			else {
				$self->log->debug("Dropping table $full_table");
				$query = sprintf("DROP TABLE %s", $full_table);
				$self->db->do($query);
				$query = sprintf("DELETE FROM %s.tables WHERE table_name=?", $ELSA::Meta_db_name);
				$sth = $self->db->prepare($query);
				$sth->execute($full_table);
			}
		}
		else {
			$self->log->error("Unknown index $id");
		}
		
		$self->log->trace('committed');
		
		my $index_name = $self->get_index_name($type, $id);
		$self->_sphinx_index($index_name);
		$self->log->trace('done dropping index id ' . $id . ' with name ' . $index_name);
	}
	
	$self->log->trace('about to release lock on directory');
	$self->release_lock('directory');
		
	$self->log->debug("Finished deleting files");
		
	return 1; 
}

sub get_sphinx_conf {
	my $self = shift;
	my $template = shift;
	open(FH, $template) or throw_e error => 'Error opening template: ' . $!;
	my @lines;
	while (<FH>){
		chomp;
		push @lines, $_;
	}
	close(FH);
	
	my $perm_template = <<EOT
source perm_%1\$d : permanent {
        sql_query_pre = SELECT table_name INTO \@src_table FROM $ELSA::Meta_db_name.v_directory WHERE id=%1\$d AND type="permanent"
        sql_query_pre = SELECT IF(NOT ISNULL(\@src_table), \@src_table, "$ELSA::Meta_db_name.init") INTO \@src_table FROM dual
        sql_query_pre = SELECT IF((SELECT first_id FROM $ELSA::Meta_db_name.v_directory WHERE id=%1\$d AND type="permanent"), (SELECT first_id FROM $ELSA::Meta_db_name.v_directory WHERE id=%1\$d AND type="permanent"), 1), IF((SELECT last_id FROM $ELSA::Meta_db_name.v_directory WHERE id=%1\$d AND type="permanent"), (SELECT last_id FROM $ELSA::Meta_db_name.v_directory WHERE id=%1\$d AND type="permanent"), 1) INTO \@first_id, \@last_id FROM dual
        sql_query_pre = SET \@sql = CONCAT("SELECT id, timestamp, CAST(timestamp/86400 AS unsigned) AS day, CAST(timestamp/3600 AS unsigned) AS hour, CAST(timestamp/60 AS unsigned) AS minute, host_id, host_id AS host, program_id, class_id, msg, i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5,i0 AS attr_i0, i1 AS attr_i1, i2 AS attr_i2, i3 AS attr_i3, i4 AS attr_i4, i5 AS attr_i5 FROM ", \@src_table, " WHERE id >= ", \@first_id, " AND id <= ", \@last_id)
        sql_query_pre = PREPARE stmt FROM \@sql
        sql_query = EXECUTE stmt 
}
index perm_%1\$d : permanent {
        path = /data/sphinx/perm_%1\$d
        source = perm_%1\$d
}
EOT
;

	my $temp_template = <<EOT
source temp_%1\$d : temporary {
        sql_query_pre = SELECT table_name INTO \@src_table FROM $ELSA::Meta_db_name.v_directory WHERE id=%1\$d AND type="temporary"
        sql_query_pre = SELECT IF(NOT ISNULL(\@src_table), \@src_table, "$ELSA::Meta_db_name.init") INTO \@src_table FROM dual
        sql_query_pre = SELECT IF((SELECT first_id FROM $ELSA::Meta_db_name.v_directory WHERE id=%1\$d AND type="temporary"), (SELECT first_id FROM $ELSA::Meta_db_name.v_directory WHERE id=%1\$d AND type="temporary"), 1), IF((SELECT last_id FROM $ELSA::Meta_db_name.v_directory WHERE id=%1\$d AND type="temporary"), (SELECT last_id FROM $ELSA::Meta_db_name.v_directory WHERE id=%1\$d AND type="temporary"), 1) INTO \@first_id, \@last_id FROM dual
        sql_query_pre = SET \@sql = CONCAT("SELECT id, timestamp, CAST(timestamp/86400 AS unsigned) AS day, CAST(timestamp/3600 AS unsigned) AS hour, CAST(timestamp/60 AS unsigned) AS minute, host_id, host_id AS host, program_id, class_id, msg, i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5,i0 AS attr_i0, i1 AS attr_i1, i2 AS attr_i2, i3 AS attr_i3, i4 AS attr_i4, i5 AS attr_i5 FROM ", \@src_table, " WHERE id >= ", \@first_id, " AND id <= ", \@last_id)
        sql_query_pre = PREPARE stmt FROM \@sql
        sql_query = EXECUTE stmt 
}
index temp_%1\$d : temporary {
        path = /data/sphinx/temp_%1\$d
        source = temp_%1\$d
}
EOT
;

	# Create the distributed config
	
	# Split all indexes into four evenly distributed groups
	my @index_groups;
	for (my $i = 1; $i <= $self->conf->get('num_indexes'); $i++){
		unshift @{ $index_groups[ $i % $self->{_NUM_CPUS} ] }, 
			$self->get_index_name('temporary', $i), $self->get_index_name('permanent', $i);
	}
	
	my $sphinx_port = $self->conf->get('sphinx/port');
	my @local_index_arr;
	for (my $i = 0; $i < $self->{_NUM_CPUS}; $i++){
		if ($index_groups[$i] and @{ $index_groups[$i] }){
			push @local_index_arr, "localhost:$sphinx_port:" . join(',', @{ $index_groups[$i] });
		}
	}

	my $timeout = $Timeout * 1000;
	my $agent_timeout = $Sphinx_agent_query_timeout * 1000;

	push @lines, 
		'index distributed_local {',
		"\t" . 'type = distributed',
		"\t" . 'agent_connect_timeout = ' . $timeout,
		"\t" . 'agent_query_timeout = ' . $agent_timeout;
	
	foreach my $line (@local_index_arr){
		push @lines, "\t" . 'agent = ' . $line;
	}
	
	push @lines, '}';

	# Now tack on our meta
	push @lines,
		'index distributed_meta {',
		"\t" . 'type = distributed',
		"\t" . 'agent = localhost:' . $sphinx_port . ':distributed_local';
	if ($self->conf->get('peers')){
		foreach my $peer (keys %{ $self->conf->get('peers') }){
			push @lines, "\t" . 'agent = ' . $peer . ':' . $sphinx_port . ':distributed_local';
		}
	}
	push @lines,
		"\t" . 'agent_connect_timeout = ' . $timeout,
		"\t" . 'agent_query_timeout = ' . $agent_timeout,
		'}'; 

	for (my $i = 1; $i <= $self->conf->get('num_indexes'); $i++){
		push @lines, sprintf($perm_template, $i);
		push @lines, sprintf($temp_template, $i);
	}

	# Create the individual remote indexes
	
	my $remote_template = <<EOT
index distributed_%1\$s_%2\$d {
        type = distributed
        agent = localhost:$sphinx_port:%1\$s_%2\$d
%3\$s
        agent_connect_timeout = $timeout
        agent_query_timeout = $agent_timeout
}
EOT
;
	if ($self->conf->get('peers')){
		for (my $i = 1; $i <= $self->conf->get('num_indexes'); $i++){
			foreach my $type qw(temp perm){
				my @peer_lines;
				foreach my $peer (keys %{ $self->conf->get('peers') }){
					push @peer_lines, "\t" . 'agent = ' . $peer . ':' . $sphinx_port . ':' . $type . '_' . $i;
				}
				push @lines, sprintf($remote_template, $type, $i, join("\n", @peer_lines));
			}
		}
	}

	return join("\n", @lines);
	
}

sub _get_next_index_id {
	my $self = shift;
	my $type = shift;
		
	my ($query, $sth);
	
	# Try to find an unused id
	$query = sprintf('SELECT id, type, start, locked_by FROM %s.indexes WHERE type=?', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($type);
	my $ids = $sth->fetchall_hashref('id') or return 1;
	for (my $i = 1; $i <= $self->conf->get('num_indexes'); $i++){
		unless ($ids->{$i}){
			return $i;
		}
	}
	
	# Since we were unable to find an unusued id, we'll have to find the oldest unlocked one
	foreach my $id (sort { $ids->{$a}->{start} <=> $ids->{$b}->{start} } keys %{$ids}){
		unless ($ids->{$id}->{locked_by}){
			$self->log->warn("Overwriting " . $ids->{$id}->{type} . " index $id");
			return $id;
		}
	}
		
	throw_e error => 'All indexes were locked: ' . Dumper($ids);
}

sub update_indexes {
	my $self = shift;
	my $node = shift;
	my $new_indexes = shift;
	
	unless ($node and $new_indexes and ref($new_indexes) eq 'HASH'){
		$self->log->error('Invalid new_indexes: ' . Dumper($new_indexes));
		return { ok => 1 };
	}
	
	# Node must be able to INET_ATON(), so the label for the node in the config must be an IP address
	unless ($node =~ /^\d+\.\d+\.\d+\.\d+$/){
		throw_params param => 'node', value => $node; 
	} 
	
	$self->log->debug('Updating indexes with node ' . $node . ' which has ' . (scalar keys %$new_indexes) . ' new indexes');
	$self->{_CURRENT_INDEXES}->{$node} = $new_indexes;
	#$self->log->debug('_CURRENT_INDEXES: ' . Dumper($self->{_CURRENT_INDEXES}));
	
	# Save the indexes to the database so that any other forked web agents will have access to it
	
	my ($query, $sth);
	$query = sprintf('REPLACE INTO %s.current_indexes (node, indexes) VALUES (INET_ATON(?), ?)', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	my $rows = $sth->execute($node, encode_json($self->{_CURRENT_INDEXES}->{$node}));
	
	return 1;
}

sub current_indexes {
	my $self = shift;
	my ($query, $sth);
	$query = sprintf('SELECT INET_NTOA(node) AS node, indexes, UNIX_TIMESTAMP(timestamp) AS timestamp FROM %s.current_indexes', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	my $ret = {};
	while (my $row = $sth->fetchrow_hashref){
		#$self->log->debug('got row: '. Dumper($row));
		eval {
			$row->{indexes} = decode_json($row->{indexes});
		};
		if ($@){
			my $errmsg = 'Corrupt stored indexes: ' . $@ . Dumper($row);
			$self->log->error($errmsg);
			throw_e error => $errmsg;
		}
		#$self->log->trace('Index timestamp: ' . scalar localtime($row->{timestamp}));
		$ret->{ $row->{node} } = $row->{indexes};
	}
	
	return $ret;
}

sub compile_indexes {
	my $self = shift;
	my $current_indexes = $self->current_indexes();
	
	my $compiled_indexes = {};
	# Find the widest times for each index per-node
	foreach my $node (keys %$current_indexes){
		my $node_indexes = $current_indexes->{$node};
		foreach my $index_name (keys %$node_indexes){
			if ($compiled_indexes->{$index_name}){
				# Update the indexes if this node has wider time values
				if ($node_indexes->{$index_name}->{start} < $compiled_indexes->{$index_name}->{start}){
					$compiled_indexes->{$index_name}->{start} = $node_indexes->{$index_name}->{start};
				}
				if ($node_indexes->{$index_name}->{end} > $compiled_indexes->{$index_name}->{end}){
					$compiled_indexes->{$index_name}->{end} = $node_indexes->{$index_name}->{end};
				}
			}
			else {
				$compiled_indexes->{$index_name} = $node_indexes->{$index_name};
			}
		}
	}
	return $compiled_indexes;
}

1;

__END__
