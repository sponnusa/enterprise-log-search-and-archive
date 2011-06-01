package ELSA;
use strict;
use warnings;
use Data::Dumper;
use Getopt::Std;
use DBI;
use integer;
use Time::HiRes qw(sleep time);
use Log::Log4perl;
#TODO Some day it would be nice to wrap Dumper statements, but it messes up the log message by overwriting the orig sub name
#use Log::Log4perl::DataDumper;
use Config::JSON;

use ELSA::Exceptions;
use ELSA::Stats;

our @Table_types = qw(syslogs);
our $Meta_db_name = 'syslog';
our $Data_db_name = 'syslog_data';
our $Buffer_set_id = 1;
our $DB_reconnect_interval = 5;

our @Tables_to_sync = qw( hosts programs );

our $Proto_map = {
	'ICMP' => 1,
	'icmp' => 1,
	'TCP' => 6,
	'tcp' => 6,
	'UDP' => 17,
	'udp' => 17,
};

our $Inverse_proto_map = {
	1 => 'ICMP',
	6 => 'TCP',
	17 => 'UDP',
};

require Exporter;
our @ISA = qw( Exporter );
our @EXPORT_OK = qw( epoch2iso iso2epoch );
our @EXPORT = qw( epoch2iso iso2epoch
	FIELD_TS FIELD_HOST FIELD_PROGRAM FIELD_CLASS_ID
	FIELD_MSG FIELD_I0 FIELD_I1 FIELD_I2 FIELD_I3 FIELD_I4 FIELD_I5
	FIELD_S0 FIELD_S1 FIELD_S2 FIELD_S3 FIELD_S4 FIELD_S5
	TUPLE_SEPARATOR
);

use constant FIELD_TS => 0;
use constant FIELD_HOST => 1;
use constant FIELD_PROGRAM => 2;
use constant FIELD_CLASS_ID => 3;
#use constant FIELD_RULE_ID => 4;
use constant FIELD_MSG => 5;
use constant FIELD_I0 => 6;
use constant FIELD_I1 => 7;
use constant FIELD_I2 => 8;
use constant FIELD_I3 => 9;
use constant FIELD_I4 => 10;
use constant FIELD_I5 => 11;
use constant FIELD_S0 => 12;
use constant FIELD_S1 => 13;
use constant FIELD_S2 => 14;
use constant FIELD_S3 => 15;
use constant FIELD_S4 => 16;
use constant FIELD_S5 => 17;

use constant TUPLE_SEPARATOR => ':';

use constant ERROR => -1;
use constant REVALIDATE => -2;

our $Field_order_to_attr = {
	0 => 'timestamp',
	100 => 'minute',
	101 => 'hour',
	102 => 'day',
	1 => 'host_id',
	2 => 'program_id',
	3 => 'class_id',
	#4 => 'rule_id',
	
	6 => 'attr_i0',
	7 => 'attr_i1',
	8 => 'attr_i2',
	9 => 'attr_i3',
	10 => 'attr_i4',
	11 => 'attr_i5',
};

our $Field_order_to_field = {
	1 => 'host',
	5 => 'msg',
	6 => 'i0',
	7 => 'i1',
	8 => 'i2',
	9 => 'i3',
	10 => 'i4',
	11 => 'i5',
	12 => 's0',
	13 => 's1',
	14 => 's2',
	15 => 's3',
	16 => 's4',
	17 => 's5',
};

our $Field_to_order = {
	'timestamp' => 0,
	'minute' => 100,
	'hour' => 101,
	'day' => 102,
	'host' => 1,
	'program' => 2,
	'class' => 3,
	#'rule' => 4,
	'msg' => 5,
	'i0' => 6,
	'i1' => 7,
	'i2' => 8,
	'i3' => 9,
	'i4' => 10,
	'i5' => 11,
	's0' => 12,
	's1' => 13,
	's2' => 14,
	's3' => 15,
	's4' => 16,
	's5' => 17,
};

sub new {
	my $class = shift;
	my $config_file_name = shift;
	
	my $config = new Config::JSON($config_file_name);
	throw_params param => 'config', value => $config_file_name 
		unless $config and ref($config) eq 'Config::JSON';

	my $self = {
		'_CONFIG_FILE' => $config_file_name,
		'_CONFIG' => $config,
		'_DEBUG_LEVEL' => ($config->get('debug_level') or 'TRACE'),
		'_STDERR_DEBUG_LEVEL' => ($config->get('stderr_debug_level') or 'TRACE'),
		'_TABLE_INTERVAL' => ($config->get('table_interval') or 3600),
		'_STATS_FILE' => ($config->get('stats_file') or '/usr/local/syslog-ng/var/syslog-ng.ctl'),
		'_LOG_SIZE_LIMIT' => ($config->get('log_size_limit') or 100_000_000_000),
		'_STATS' => new ELSA::Stats(),
	};
	bless($self, $class);
	
	# Setup logger
	my $logdir = $self->conf->get('logdir');
	my $conf = qq(
		log4perl.category.ELSA       = $self->{_DEBUG_LEVEL}, File
		log4perl.appender.File			 = Log::Log4perl::Appender::File
		log4perl.appender.File.filename  = $logdir/node.log
		log4perl.appender.File.syswrite = 1
		log4perl.appender.File.recreate = 1
		log4perl.appender.File.layout = Log::Log4perl::Layout::PatternLayout
		log4perl.appender.File.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %x%n%m%n
		log4perl.filter.ScreenLevel               = Log::Log4perl::Filter::LevelRange
  		log4perl.filter.ScreenLevel.LevelMin  = $self->{_STDERR_DEBUG_LEVEL}
  		log4perl.filter.ScreenLevel.LevelMax  = ERROR
  		log4perl.filter.ScreenLevel.AcceptOnMatch = true
  		log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
		log4perl.appender.Screen.Filter = ScreenLevel 
		log4perl.appender.Screen.stderr  = 1
		log4perl.appender.Screen.layout = Log::Log4perl::Layout::PatternLayout
		log4perl.appender.Screen.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %x%n%m%n
	);
	
	Log::Log4perl::init( \$conf ) or die("Unable to init logger\n");
	$self->{_LOGGER} = Log::Log4perl::get_logger("ELSA") or die("Unable to init logger\n");
	
	return $self;
}

sub stats {
	my $self = shift;
	return $self->{_STATS};
}

sub init_cache {
	my $self = shift;
	
	my ($query, $sth);
	$query = sprintf('SELECT id, program FROM %s.programs', $Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	$self->{_CACHE} = {};
	while (my $row = $sth->fetchrow_hashref){
		$self->{_CACHE}->{ $row->{program} } = $row->{id};
	}
	
	return 1;
}

sub init_classes {
	my $self = shift;
	
	# Make handy class hash for later use
	$self->{_CLASSES} = $self->get_classes();

	return 1;
}

sub conf {
	my $self = shift;	
	return $self->{_CONFIG};
}

sub init_db {
	my $self = shift;
	
	$self->{'_DSN'} = ($self->conf->get('database/dsn') or 'dbi:mysql:database=syslog;');
	$self->{'_DB_USER'} = ($self->conf->get('database/username') or 'root');
	$self->{'_DB_PASS'} = ($self->conf->get('database/password') or '');
	
	eval {
		if ($self->{_DBH}){
			$self->log->debug('disconnecting _DBH ' . $self->{_DBH});
			$self->{_DBH}->disconnect();
			$self->{_DBH} = undef;
		}
		$self->{_DBH} = DBI->connect($self->{'_DSN'}, $self->{'_DB_USER'}, 
			$self->{'_DB_PASS'}, {RaiseError => 0}) or throw_e error => 'connection failed ' . $! . ' ' . $DBI::errstr;
		my ($query, $sth);
		
		# There's been a lot of issues with POE::Component::PreforkDispatch and DB handles, so this code is very pedantic
		$query = 'SELECT CONNECTION_ID() AS id';
		$sth = $self->{_DBH}->prepare($query);
		$sth->execute();
		my $row = $sth->fetchrow_hashref;
		my $conn_id = $row->{id};
		open(FH, "/tmp/parent_pid");
		my $parent_pid = <FH>;
		close(FH);
		chomp($parent_pid);
		$self->log->debug('my pid: ' . $$ . ' parent_pid: ' . $parent_pid . ' using connection_id ' . $conn_id);
		if ($$ eq $parent_pid){
			open(FH, "> /tmp/parent_dbh");
			print FH scalar $self->{_DBH};
			close(FH);
		}
		else {
			open(FH, "/tmp/parent_dbh");
			my $parent_dbh = <FH>;
			close(FH);
			chomp($parent_dbh);
			if (scalar $self->{_DBH} eq $parent_dbh){
				throw_e error => 'I am using my parent\'s DBH! (' . scalar $self->{_DBH} . '=' . $parent_dbh . ')';
			}
		}
		
	};
	if ($@){
		my $e = $@;
		$self->log->error('sql error: ' . Dumper($e));
		sql_error_handler($e->error(), $self->{_DBH});
	}
	$self->{_DBH}->{mysql_auto_reconnect} = 1; # we will auto-reconnect on disconnect
	$self->{_DBH}->{HandleError} = \&sql_error_handler;
	$self->log->debug('_DBH: ' . $self->{_DBH});

	return 1;
}

sub sql_error_handler {
	my $errstr = shift;
	my $dbh = shift;
	my $query = $dbh->{Statement};
	
	my $logger = Log::Log4perl::get_logger("ELSA");
	my $e = ELSA::Exception::SQL->new(sql_error => $errstr, query => $query, args => undef);
	$errstr = sprintf("SQL ERROR: %s\nQuery: %s\nTrace: %s\n", 
			$e->sql_error, $e->query, $e->trace->as_string);
	$logger->error($errstr);
	
	# Check to see if this is a simple transaction-already-in-progress error
	if ($errstr =~ /begin_work failed: Already in a transaction/){
		$logger->warn("Got a SQL error indicating transaction was already in progress, ignoring");
		$e->caught();
	}
	elsif ($errstr =~ /Too many connections/){
		$e->caught();
		$logger->error("Number of failed reconnects: " . $dbh->{auto_reconnects_failed});
		# sleep awhile and try again
		sleep $DB_reconnect_interval;
	}
	elsif ($errstr =~ /server has gone away/){
		$e->caught();
		$logger->error("Number of failed reconnects: " . $dbh->{auto_reconnects_failed});
		# sleep awhile and try again
		sleep $DB_reconnect_interval;
	}
	else {
		throw_sql sql_error => $errstr, query => $query, args => undef;
		$logger->debug("Threw sql exception");	
	}
	
	return 1; # Stops default RaiseError from happening
}

sub log {
	my $self = shift;
	return $self->{_LOGGER};
}

sub db {
	my $self = shift;
	return $self->{_DBH};
}

sub get_table {
	my $self = shift;
	my $args = shift;
	throw_params param => 'args', value => Dumper($args)
		unless $args and ref($args) eq 'HASH';
	
	my $table_type = 'index';
	if ($args->{archive}){
		$table_type = 'archive';
	}
	$args->{table_type} = $table_type;
	
	my ($query, $sth, $row);
	
	$query = sprintf('SELECT table_name, min_id, max_id, table_locked_by, locked_by' . "\n" .
		'FROM %1$s.tables' . "\n" .
		'LEFT JOIN %1$s.indexes ON (tables.table_locked_by=indexes.locked_by)' . "\n" .
		'WHERE table_type_id=(SELECT id FROM %1$s.table_types WHERE table_type=?)' . "\n" .
		"ORDER BY tables.id DESC LIMIT 1", $Meta_db_name);
	my $error = 0;
	do {
		eval {
			$sth = $self->db->prepare($query);
			$sth->execute($table_type) or throw_e error => $self->db->errstr;
			$row = $sth->fetchrow_hashref;
		};
		if ($@){
			my $e = $@;
			$self->log->error($e);
			$error = 1;
		}
		else {
			$error = 0;
		}
	} while ($error);
	if ($row){
		# Is it time for a new index?
		my $size = $self->conf->get('sphinx/perm_index_size');
		if ($table_type eq 'archive'){
			$size = $self->conf->get('archive/perm_index_size');
		}
		# See if the table is too big or is being consolidated
		if (($row->{max_id} - $row->{min_id}) >= $size){
			my $new_id = $row->{max_id} + 1;
			$self->log->debug("suggesting new table with id $new_id");
			$args->{table_name} = sprintf("%s.syslogs_%s_%d", $Data_db_name, $table_type, $new_id);
			return $args;
		}
		else {
			$self->log->debug("using current table $row->{table_name}");
			$args->{table_name} = $row->{table_name};
			return $args;
		}
	}
	else {
		# This is the first table
		$args->{table_name} = sprintf("%s.syslogs_%s_%d", $Data_db_name, $table_type, 1);
		return $args;
	}
}

sub get_classes {
	my $self = shift;
	
	# Get our unique classes	
	my $query = "SELECT id, class FROM classes";
	my $sth = $self->db->prepare($query);
	$sth->execute();
	
	my %class_ids;
	while (my $row = $sth->fetchrow_hashref){
		$class_ids{ $row->{id} } = $row->{class};
	}
	return \%class_ids;
}

sub get_classes_by_name {
	my $self = shift;
	
	# Get our unique classes	
	my $query = "SELECT id, class FROM classes";
	my $sth = $self->db->prepare($query);
	$sth->execute();
	
	my %class_ids;
	while (my $row = $sth->fetchrow_hashref){
		$class_ids{ lc($row->{class}) } = $row->{id};
	}
	return \%class_ids;
}

sub epoch2iso {
	my $epochdate = shift;
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($epochdate);
	my $date = sprintf("%04d-%02d-%02d %02d:%02d:%02d", 
		$year + 1900, $mon + 1, $mday, $hour, $min, $sec);
	return $date;
}

sub get_min_max {
	my $self = shift;
	my $type = shift;
	$type ||= 'index';
	my ($query, $sth);
	$query = sprintf("SELECT MIN(start) AS start,\n" .
		"IF(MAX(end), MAX(end), NOW()) AS end\n" .
		"FROM %s.v_directory\n" .
		"WHERE table_type=?", $Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($type);
	my $row = $sth->fetchrow_hashref;
	if ($row){
		return $row
	}
	else {
		$self->log->warn("No min/max found");
		return { start => '0000-00-00 00:00:00', end => '0000-00-00 00:00:00' };
	}
}

sub get_min_max_indexes {
	my $self = shift;
	my ($query, $sth);
	$query = sprintf("SELECT MIN(index_start) AS start, MAX(index_end) AS end, MIN(index_start_int) AS start_int, MAX(index_end_int) AS end_int\n" .
		"FROM %s.v_directory", $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	my $row = $sth->fetchrow_hashref;
	if ($row){
		return $row;
	}
	else {
		return { start_int => time(), end_int => time(), start => '0000-00-00 00:00:00', end => '0000-00-00 00:00:00' };
	}
}

sub get_programs {
	my $self = shift;
	my ($query, $sth);
	$query = sprintf('SELECT t1.program, t2.program_id, t2.class_id FROM %1$s.programs t1' . "\n" .
		'JOIN %1$s.class_program_map t2 ON (t1.id=t2.program_id)', $Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	my %programs;
	while (my $row = $sth->fetchrow_hashref){
		$programs{ $row->{class_id} } ||= {};
		$programs{ $row->{class_id} }->{ $row->{program} } = $row->{program_id};
	}
	return \%programs;
}

sub get_distinct_syslog_fields {
	my $self = shift;
	my $table = shift;
	my ($query, $sth);
	my $field_map = {
		'hosts' => 'host',
		'classes' => 'class',
		'programs' => 'program', 
	};
	unless ($table and $field_map->{$table}){
		throw_params param => 'table', value => $table;
	}
	$query = sprintf("SELECT DISTINCT %s AS field, id FROM %s.%s ORDER BY %s ASC", 
		$field_map->{$table}, $Meta_db_name, $table, $field_map->{$table});
	$sth = $self->db->prepare($query);
	$sth->execute();
	my %fields;
	while (my $row = $sth->fetchrow_hashref){
		$fields{ $row->{field} } = $row->{id};
	}
	return \%fields;
}

sub get_fields {
	my $self = shift;
	my ($query, $sth);
	$query = sprintf("SELECT DISTINCT field, class, field_type, input_validation, field_id, class_id, field_order,\n" .
		"IF(class!=\"\", CONCAT(class, \".\", field), field) AS fqdn_field\n" .
		"FROM %s.fields\n" .
		"JOIN %1\$s.fields_classes_map t2 ON (fields.id=t2.field_id)\n" .
		"JOIN %1\$s.classes t3 ON (t2.class_id=t3.id)\n",
		#"WHERE class_id!=0", 
		$Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	my @fields;
	while (my $row = $sth->fetchrow_hashref){
		push @fields, {
			'fqdn_field' => $row->{fqdn_field},
			'class' => $row->{class}, 
			'value' => $row->{field}, 
			'text' => uc($row->{field}),
			'field_id' => $row->{field_id},
			'class_id' => $row->{class_id},
			'field_order' => $row->{field_order},
			'field_type' => $row->{field_type},
			'input_validation' => $row->{input_validation},
		};
	}
	return \@fields;
}

sub get_fields_by_name {
	my $self = shift;
	my ($query, $sth);
	$query = sprintf("SELECT DISTINCT field, field_id, class_id, field_order, pattern_type FROM %s.fields\n" .
		"JOIN %1\$s.fields_classes_map t2 ON (fields.id=t2.field_id)", $Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	my %fields;
	while (my $row = $sth->fetchrow_hashref){
		$fields{ $row->{field} } ||= []; 
		push @{ $fields{ $row->{field} } }, {
			'value' => $row->{field}, 
			'text' => uc($row->{field}),
			'field_id' => $row->{field_id},
			'class_id' => $row->{class_id},
			'field_order' => $row->{field_order},
			'pattern_type' => $row->{pattern_type},
		};
	}
	return \%fields;
}

sub get_field {
	my $self = shift;
	my $raw_field = shift;
	
	my ($query, $sth);
	my %fields;
	
	# Account for FQDN fields which come with the class name
	my ($class, $field) = split(/\./, $raw_field);
	
	if ($field){
		# We were given an FQDN, so there is only one class this can be
		$query = sprintf('SELECT DISTINCT field, field_id, class_id, field_order, field_type FROM %s.fields' . "\n" .
			'JOIN %1$s.fields_classes_map t2 ON (fields.id=t2.field_id)' . "\n" .
			'JOIN %1$s.classes t3 ON (t2.class_id=t3.id)' . "\n" .
			'WHERE field=? AND class=?', $Meta_db_name);
		$sth = $self->db->prepare($query);
		$sth->execute($field, $class);
		my $row = $sth->fetchrow_hashref;
		$fields{ $row->{class_id} } = { 
			'value' => $row->{field}, 
			'text' => uc($row->{field}),
			'field_id' => $row->{field_id},
			'class_id' => $row->{class_id},
			'field_order' => $row->{field_order},
			'field_type' => $row->{field_type},
		};
		return \%fields;
	}

	# Was not FQDN
	$field = $raw_field;
	$class = 0;
	
	# Could also be a meta-field/attribute
	if (defined $Field_to_order->{$field}){
		$fields{$class} = { 
			value => $field, 
			text => uc($field), 
			field_id => $Field_to_order->{$field},
			class_id => $class, 
			field_order => $Field_to_order->{$field}
		};
	}
	
	$query = sprintf("SELECT DISTINCT field, field_id, class_id, field_order, field_type FROM %s.fields\n" .
		"JOIN %1\$s.fields_classes_map t2 ON (fields.id=t2.field_id)\n" .
		"WHERE field=?", $Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($field);
	
	while (my $row = $sth->fetchrow_hashref){
		$fields{ $row->{class_id} } = { 
			'value' => $row->{field}, 
			'text' => uc($row->{field}),
			'field_id' => $row->{field_id},
			'class_id' => $row->{class_id},
			'field_order' => $row->{field_order},
			'field_type' => $row->{field_type},
		};
	}
	
	return \%fields;
}

sub get_fields_arr_by_order {
	my $self = shift;
	my $class_id = sprintf("%d", shift);
	throw_params param => 'class_id', value => $class_id
		unless defined $class_id;
	my ($query, $sth);
	if ($class_id){
		$query = sprintf("SELECT field, field_id, field_order FROM %s.fields\n" .
			"JOIN %1\$s.fields_classes_map t2 ON (fields.id=t2.field_id)\n" .
			"WHERE class_id=?", $Meta_db_name);
			$sth = $self->db->prepare($query);
			$sth->execute($class_id);
	}
	else {
		# All
		$query = sprintf("SELECT field, field_id, field_order FROM %s.fields\n" .
			"JOIN %1\$s.fields_classes_map t2 ON (fields.id=t2.field_id)", $Meta_db_name);
		$sth = $self->db->prepare($query);
		$sth->execute();
	}
		
	my %fields;
	while (my $row = $sth->fetchrow_hashref){
		$fields{ $row->{field_order} } = { 
			'value' => $row->{field}, 
			'text' => uc($row->{field}),
			'field_id' => $row->{field_id},
			'field_order' => $row->{field_order},
		};
	}
	return \%fields;
}

sub get_meta_table_from_field {
	my $self = shift;
	my $field = shift;
	
	my $map = {
		'host' => $Meta_db_name . '.hosts',
		'program' => $Meta_db_name . '.programs',
		'facility' => $Meta_db_name . '.facilities',
	};
	
	return $map->{$field};
}

sub log_error {
	my $e = shift;
	
	my $logger = Log::Log4perl::get_logger("ELSA");
	if (ref($e)){
		if (ref($e) eq 'ELSA::Exception::Param'){
			$logger->error($e->description() . ": " . $e->param() . "=" . $e->value() . " " 
				. "\n" . $e->trace());
		}
		elsif (ref($e) eq 'ELSA::Exception::SQL'){
			$logger->error($e->description() . ": " . $e->sql_error() .  "\n" 
				. $e->query() . "\n"
				. $e->args() . "\n"
				. $e->trace());
		}
		elsif (ref($e) eq 'ELSA::Exception::Parse'){
			$logger->error($e->description() . ": error: " . $e->parse_error() . "text: " . $e->text() . "\n");
		}
		elsif (ref($e) and $e->isa('Exception::Class::Base')) {
			$logger->error($e->description() . ": " . $e->message() . "\n" . $e->trace());
		}
		else {
			$logger->error(Dumper($e));
		}
	}
	else {
		$logger->error(Dumper($e));
	}
} 

sub get_attr_conversions {
	my $self = shift;
	
	my $attrs = {};
	
	my ($query, $sth);
	foreach my $item qw( program ){	
		$query = sprintf("SELECT %s, id FROM %s.%ss", $item, $Meta_db_name, $item);
		$sth = $self->db->prepare($query);
		$sth->execute();
		while (my $row = $sth->fetchrow_hashref){
			$attrs->{$item . '_id'}->{ $row->{$item} } = $row->{id};
		}
	}
	return $attrs;	
}

sub get_field_conversions {
	my $self = shift;
	my ($query, $sth);
	
	my $fields = {};
	
	# Find IPv4 fields
	$query = sprintf("SELECT class_id, field_order, field\n" .
		"FROM %1\$s.fields\n" .
		"JOIN %1\$s.fields_classes_map ON (fields.id=fields_classes_map.field_id)\n" .
		"WHERE pattern_type=\"IPv4\"", $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	while (my $row = $sth->fetchrow_hashref){
		unless ( $fields->{ $row->{class_id} }){
			$fields->{ $row->{class_id} } = {};
			$fields->{ $row->{class_id} }->{'IPv4'} = {};
		}		
		$fields->{ $row->{class_id} }->{'IPv4'}->{ $row->{field_order} } = $row->{field};
	}
	
	# Find protocol fields
	$query = sprintf("SELECT class_id, field_order, field\n" .
		"FROM %1\$s.fields\n" .
		"JOIN %1\$s.fields_classes_map ON (fields.id=fields_classes_map.field_id)\n" .
		"WHERE field=\"proto\" AND pattern_type=\"QSTRING\"", $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	while (my $row = $sth->fetchrow_hashref){
		unless ( $fields->{ $row->{class_id} }){
			$fields->{ $row->{class_id} } = {};
			$fields->{ $row->{class_id} }->{'PROTO'} = {};
		}
		unless ($fields->{ $row->{class_id} }->{'PROTO'}){
			$fields->{ $row->{class_id} }->{'PROTO'} = {};
		}
		$fields->{ $row->{class_id} }->{'PROTO'}->{ $row->{field_order} } = $row->{field};
	}
	
	# Find country_code fields
	$query = sprintf("SELECT class_id, field_order, field\n" .
		"FROM %1\$s.fields\n" .
		"JOIN %1\$s.fields_classes_map ON (fields.id=fields_classes_map.field_id)\n" .
		"WHERE field=\"country_code\" AND pattern_type=\"NUMBER\"", $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute();
	while (my $row = $sth->fetchrow_hashref){
		unless ( $fields->{ $row->{class_id} }){
			$fields->{ $row->{class_id} } = {};
			$fields->{ $row->{class_id} }->{'COUNTRY_CODE'} = {};
		}
		unless ($fields->{ $row->{class_id} }->{'COUNTRY_CODE'}){
			$fields->{ $row->{class_id} }->{'COUNTRY_CODE'} = {};
		}
		$fields->{ $row->{class_id} }->{'COUNTRY_CODE'}->{ $row->{field_order} } = $row->{field};
	}

	return $fields;
}

sub get_index_name {
	my $self = shift;
	my $type = shift;
	my $id = shift;
	if ($type eq 'permanent'){
		return sprintf('perm_%d', $id);
	}
	elsif ($type eq 'temporary'){
		return sprintf('temp_%d', $id);
	}
	else {
		throw_e error => 'Unknown index type: ' . $type;
	}
}

sub get_peers {
	my $self = shift;
	return $self->conf->get('peers');
}

1;

__END__
