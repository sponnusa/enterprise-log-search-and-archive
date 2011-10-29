#!/usr/bin/perl
use strict;
use Data::Dumper;
use IO::Handle;
use POSIX;
use Config::JSON;
use Getopt::Std;
use String::CRC32;
use Log::Log4perl;
use DBI;
use Socket qw(inet_aton);
use FindBin;

# Include the directory this script is in
use lib $FindBin::Bin;

use Indexer;

my %Opts;
getopts('onc:', \%Opts);

$| = 1;
my $pipes     = {};
my $conf_file = $Opts{c} ? $Opts{c} : '/etc/elsa.conf';
my $Conf = Config::JSON->new( $conf_file );
$Conf = $Conf->{config}; # native hash is 10x faster than using Config::JSON->get()

# Setup logger
my $logdir = $Conf->{logdir};
my $debug_level = $Conf->{debug_level};
my $l4pconf = qq(
	log4perl.category.ELSA       = $debug_level, File
	log4perl.appender.File			 = Log::Log4perl::Appender::File
	log4perl.appender.File.filename  = $logdir/node.log
	log4perl.appender.File.syswrite = 1
	log4perl.appender.File.recreate = 1
	log4perl.appender.File.layout = Log::Log4perl::Layout::PatternLayout
	log4perl.appender.File.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %m%n
	log4perl.filter.ScreenLevel               = Log::Log4perl::Filter::LevelRange
	log4perl.filter.ScreenLevel.LevelMin  = $debug_level
	log4perl.filter.ScreenLevel.LevelMax  = ERROR
	log4perl.filter.ScreenLevel.AcceptOnMatch = true
	log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
	log4perl.appender.Screen.Filter = ScreenLevel 
	log4perl.appender.Screen.stderr  = 1
	log4perl.appender.Screen.layout = Log::Log4perl::Layout::PatternLayout
	log4perl.appender.Screen.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %m%n
);
Log::Log4perl::init( \$l4pconf ) or die("Unable to init logger\n");
my $Log = Log::Log4perl::get_logger("ELSA") or die("Unable to init logger\n");

my $Dbh = DBI->connect(($Conf->{database}->{dsn} or 'dbi:mysql:database=syslog;'), 
	$Conf->{database}->{username}, 
	$Conf->{database}->{password}, 
	{
		InactiveDestroy => 1, 
		PrintError => 0,
		mysql_auto_reconnect => 1, 
		HandleError => \&_sql_error_handler,
	}) 
	or die 'connection failed ' . $! . ' ' . $DBI::errstr;

my $num_children = $Conf->{num_indexers} or die("undefined config for num_indexers");
my $continue     = 1;
my $Run          = 1;
my $Missing_field_tolerance = 1;
my $Default_class_id = 1;

my $Proto_map = {
	'ICMP' => 1,
	'icmp' => 1,
	'TCP' => 6,
	'tcp' => 6,
	'UDP' => 17,
	'udp' => 17,
};

use constant FIELD_TIMESTAMP => 0;
use constant FIELD_HOST => 1;
use constant FIELD_PROGRAM => 2;
use constant FIELD_CLASS_ID => 3;
use constant FIELD_MSG => 4;
use constant FIELD_I0 => 5;
use constant FIELD_I1 => 6;
use constant FIELD_I2 => 7;
use constant FIELD_I3 => 8;
use constant FIELD_I4 => 9;
use constant FIELD_I5 => 10;
use constant FIELD_S0 => 11;
use constant FIELD_S1 => 12;
use constant FIELD_S2 => 13;
use constant FIELD_S3 => 14;
use constant FIELD_S4 => 15;
use constant FIELD_S5 => 16;

my $Class_info = _get_class_info();
my $Cache = {};
_init_cache();

unless (-f $Conf->{sphinx}->{config_file}){
	_create_sphinx_conf();
}

unless ($Opts{n}){
	print "Validating directory...\n";
	my $indexer = new Indexer(log => $Log, conf => Config::JSON->new( $conf_file ), class_info => $Class_info);
	$indexer->initial_validate_directory();
}

if ($Opts{o}){
	print "Running once\n";
	$Run = 0;
}

$SIG{TERM} = sub { $Run = 0; warn 'Shutting down' };
$SIG{CHLD} = 'IGNORE'; # will do the wait() so we don't create zombies

my $total_processed = 0;
do {
	$Log->debug("Starting process_batch");
	eval {
		my $num_processed = _process_batch();
		$total_processed += $num_processed;
		$Log->debug("Processed $num_processed records");
		sleep 1 unless $num_processed; # avoid batch-bombing if our parent handle closes
	};
	if ($@) {
		my $e = $@;
		$Log->error($e);
		sleep 1 if $Run;                                # to avoid errmsg flooding
	}
	$Conf = Config::JSON->new( $conf_file )->{config} if $Run; # reload the config in case it has changed on disk
} while ($Run);

$Log->info('Exiting after processing ' . $total_processed . ' records');
exit;

sub _sql_error_handler {
	my $errstr = shift;
	my $dbh = shift;
	my $query = $dbh->{Statement};
	my $full_errstr = 'SQL_ERROR: ' . $errstr . ', query: ' . $query; 
	$Log->error($full_errstr);
	#return 1; # Stops RaiseError
	die($full_errstr);
}

sub _create_sphinx_conf {
	my $indexer = new Indexer(log => $Log, conf => Config::JSON->new( $conf_file ), class_info => $Class_info);
	open(FH, '>' . $Conf->{sphinx}->{config_file}) or die("Cannot open config file for writing: $!");
	print FH $indexer->get_sphinx_conf($Conf->{sphinx}->{config_template_file});
	close(FH);
	print 'Wrote new config file using template at ' . $Conf->{sphinx}->{config_template_file} . ' to file ' . $Conf->{sphinx}->{config_file} . "\n";
}

sub _process_batch {
	my $filename = shift;
	
	my $args = { run => 1 };
	
	my $fh = \*STDIN;
	if ($filename){
		open($fh, $filename) or die 'Unable to open file: ' . $!;
		$Log->debug('Reading from file ' . $filename);
		$args->{offline_processing} = 1;
		$args->{offline_processing_start} = time();
		$args->{offline_processing_end} = 0;
	}
	$fh->autoflush(1);
	
	die "Non-existent buffer_dir: " . $Conf->{buffer_dir}
		unless -d $Conf->{buffer_dir};
		
#	$Log->debug("Starting up with batch_id: $batch_id, first_id: $first_id");
	
	my $start_time = Time::HiRes::time();
		
	my $tempfile = File::Temp->new( DIR => $Conf->{buffer_dir}, UNLINK => 0 );
	unless ($tempfile){
		$Log->error('Unable to create tempfile: ' . $!);
		return 0;
	}
	$tempfile->autoflush(1);
	my $batch_counter = 0;
	my $error_counter = 0;
	
	# Reset the miss cache
	$args->{cache_add} = {};
	
	# End the loop after table_interval seconds
	local $SIG{ALRM} = sub {
		$Log->trace("ALARM");
		$args->{run} = 0;
	};
	unless ($args->{offline_processing}){
		alarm $Conf->{sphinx}->{index_interval};
	}
	
	while (<$fh>){	
		eval { 
			$tempfile->print(join("\t", @{ _parse_line($args, $_) }) . "\n");
			$batch_counter++;
		};
		if ($@){
			my $e = $@;
			$error_counter++;
			if ($Conf->{log_parse_errors}){
				$Log->error($e) 
			}
		}
		last unless $args->{run};
	}
		
	# Update args to be results
	$args->{file} = $tempfile->filename();
	$args->{start} = $args->{offline_processing} ? $args->{offline_processing_start} : $start_time;
	$args->{end} = $args->{offline_processing} ? $args->{offline_processing_end} : Time::HiRes::time();
	$args->{total_processed} = $batch_counter;
	$args->{total_errors} = $error_counter;
	
	# Report back that we've finished
	$Log->debug("Finished job process_batch with cache hits: $batch_counter and " . (scalar keys %{ $args->{cache_add} }) . ' new programs');
	$Log->debug('Total errors: ' . $error_counter . ' (%' . (($error_counter / $batch_counter) * 100) . ')' ) if $batch_counter;
	
	my ($query, $sth);
	if (scalar keys %{ $args->{cache_add} }){
		$Log->trace('Adding programs: ' . Dumper($args->{cache_add}));
		$query = 'INSERT INTO programs (id, program) VALUES(?,?) ON DUPLICATE KEY UPDATE id=?';
		$sth = $Dbh->prepare($query);
		$query = 'REPLACE INTO class_program_map (class_id, program_id) VALUES(?,?)';
		my $sth_map = $Dbh->prepare($query);
		foreach my $program (keys %{ $args->{cache_add} }){
			$sth->execute($args->{cache_add}->{$program}->{id}, $program, $args->{cache_add}->{$program}->{id});
			if ($sth->rows){ # this was not a duplicate, proceed with the class map insert
				$sth_map->execute($args->{cache_add}->{$program}->{class_id}, $args->{cache_add}->{$program}->{id});
			}
			else {
				$Log->error('Duplicate CRC found for ' . $program . ' with CRC ' . $args->{cache_add}->{$program}->{id});
			}
		}
		$args->{cache_add} = {};
	}
	
	if ($batch_counter){
		$query = 'INSERT INTO buffers (filename) VALUES (?)';
		$sth = $Dbh->prepare($query);
		$sth->execute($args->{file});
		$Log->trace('inserted filename ' . $args->{file} . ' with batch_counter ' . $batch_counter);
	}
		
	# Reset the run marker
	$args->{run} = 1;
	
	# Fork our post-batch processor
	return $batch_counter unless $batch_counter;
	my $pid = fork();
	if ($pid){
		# Parent
		return $batch_counter;
	}
	# Child
	$Log->trace('Child started');
	eval {
		my $indexer = new Indexer(log => $Log, conf => Config::JSON->new( $conf_file ), class_info => $Class_info);
		$indexer->load_buffers();
	};
	if ($@){
		$Log->error('Child encountered error: ' . $@);
	}
	$Log->trace('Child finished');
	exit; # done with child
}

sub _parse_line {
	my $args = shift;
	my $raw_line = shift;
		
	chomp($raw_line);
		
	my @line = split(/\t/, $raw_line);
	
	# Fix class_id for "unknown"
    if ($line[FIELD_CLASS_ID] eq 'unknown'){
    	$line[FIELD_CLASS_ID] = $Default_class_id;
    }
    		        
	# If we're configured to do so, we'll tolerate missing a missing field and make up a default
	if ($Missing_field_tolerance){
		my $missing_fields = 0;
		# Make sure that we've got the basics--things we don't want to fake
		unless ($line[FIELD_HOST] and $line[FIELD_MSG]){
			die "Unable to parse log line: $raw_line.  Only parsed into:\n" . Dumper(\@line);
		}
		unless ($line[FIELD_TIMESTAMP]){
			$line[FIELD_TIMESTAMP] = time();
			$Log->warn('Missing required field timestamp') if $Conf->{log_parse_errors};
			$missing_fields++;
		}
		unless ($line[FIELD_PROGRAM]){
			# Check to see if this is a dumb situation in which Cisco put program in msg
			$line[FIELD_PROGRAM] = 'unknown';
			$Log->warn('Missing required field program') if $Conf->{log_parse_errors};
			$missing_fields++;
		}
		unless ($line[FIELD_CLASS_ID]){
			$line[FIELD_CLASS_ID] = '1';
			$Log->warn('Missing required field class id') if $Conf->{log_parse_errors};
			$missing_fields++;
		}
					
		if ($missing_fields > $Missing_field_tolerance){
			die "Unable to parse log line $raw_line: not enough fields.  Only parsed into:\n" . Dumper(\@line);
		}
	}
	else {
		# No tolerance for any missing fields
		unless ($line[FIELD_TIMESTAMP] and $line[FIELD_CLASS_ID] and $line[FIELD_HOST] and
			$line[FIELD_PROGRAM] and $line[FIELD_MSG]){
			die "Unable to parse log line $raw_line: no tolerance for missing fields.  Only parsed into:\n" . Dumper(\@line);
		}
	}
    
    unless ($Class_info->{classes_by_id}->{ $line[FIELD_CLASS_ID] }){
		die "Unable to parse valid class id from log line $raw_line.  Only parsed into:\n" . Dumper(\@line);
	}
	
	# Fix weird programs that may be wrong
	if ($line[FIELD_PROGRAM] =~ /^\d+$/){
#		$Log->debug("ALL NUMBER PROG: " . $line[FIELD_PROGRAM] . ", raw_line: $raw_line");
		$line[FIELD_PROGRAM] = 'unknown';
	}
	
	# Escape any backslashes in MSG
	$line[FIELD_MSG] =~ s/\\/\\\\/g;
	
	# Normalize program name to be all lowercase
	$line[FIELD_PROGRAM] = lc($line[FIELD_PROGRAM]);
	
	# Normalize program name to swap any weird chars with underscores
	#$line[FIELD_PROGRAM] =~ s/[^a-zA-Z0-9\_\-]/\_/g;
	
	# Host gets the int version of itself
	$line[FIELD_HOST] = unpack('N*', inet_aton($line[FIELD_HOST]));
	
	# Perform a crc32 conversion of the program and store it in the cache for later recording
	if ($Cache->{ $line[FIELD_PROGRAM] }){
		$line[FIELD_PROGRAM] = $Cache->{ $line[FIELD_PROGRAM] };
	}
	else {
		my $program = $line[FIELD_PROGRAM];
		$line[FIELD_PROGRAM] = crc32( $program );
		$args->{cache_add}->{ $program } = { id => $line[FIELD_PROGRAM], class_id => $line[FIELD_CLASS_ID] };
		$Cache->{ $program } = $line[FIELD_PROGRAM];
	}
	
	if ($line[FIELD_CLASS_ID] ne 1){ #skip default since there aren't any fields
		# Convert any IP fields as necessary
		foreach my $field_order (keys %{ $Class_info->{field_conversions}->{ $line[FIELD_CLASS_ID] }->{'IPv4'} }){
			$line[$field_order] = unpack('N', inet_aton($line[$field_order]));
		}
		
		# Convert any proto fields as necessary
		foreach my $field_order (keys %{ $Class_info->{field_conversions}->{ $line[FIELD_CLASS_ID] }->{PROTO} }){
			$line[$field_order] = $Proto_map->{ $line[$field_order] };
		}
	}
	
	# Update start/end times if necessary
	if ($args->{offline_processing}){
		if ($line[FIELD_TIMESTAMP] < $args->{offline_processing_start}){
			$args->{offline_processing_start} = $line[FIELD_TIMESTAMP];
		}
		if ($line[FIELD_TIMESTAMP] > $args->{offline_processing_end}){
			$args->{offline_processing_end} = $line[FIELD_TIMESTAMP];
		}
	}
		
	# Push our auto-inc dummy val on
	unshift(@line, '0');
	
	return \@line;
}


sub _get_class_info {
	my $ret = { classes => {}, classes_by_id => {}, fields => [], field_conversions => {}, };
	my ($query, $sth);	
	# Get classes
	$query = "SELECT id, class FROM classes";
	$sth = $Dbh->prepare($query);
	$sth->execute;
	while (my $row = $sth->fetchrow_hashref){
		$ret->{classes}->{ $row->{id} } = $row->{class};
	}
		
	# Get fields
	$query = "SELECT DISTINCT field, class, field_type, input_validation, field_id, class_id, field_order,\n" .
		"IF(class!=\"\", CONCAT(class, \".\", field), field) AS fqdn_field, pattern_type\n" .
		"FROM fields\n" .
		"JOIN fields_classes_map t2 ON (fields.id=t2.field_id)\n" .
		"JOIN classes t3 ON (t2.class_id=t3.id)\n";
	$sth = $Dbh->prepare($query);
	$sth->execute;
	while (my $row = $sth->fetchrow_hashref){
		push @{ $ret->{fields} }, {
			fqdn_field => $row->{fqdn_field},
			class => $row->{class}, 
			value => $row->{field}, 
			text => uc($row->{field}),
			field_id => $row->{field_id},
			class_id => $row->{class_id},
			field_order => $row->{field_order},
			field_type => $row->{field_type},
			input_validation => $row->{input_validation},
			pattern_type => $row->{pattern_type},
		};
	}
	
	# Find unique classes;
	foreach my $class_id (keys %{ $ret->{classes} }){
		$ret->{classes_by_id}->{$class_id} = $ret->{classes}->{$class_id};
		$ret->{classes}->{ $ret->{classes}->{$class_id} } = $class_id;
	}
	
	# Find unique field conversions
	foreach my $field_hash (@{ $ret->{fields} }){
		$ret->{field_conversions}->{ $field_hash->{class_id} } ||= {};
		if ($field_hash->{pattern_type} eq 'IPv4'){
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{IPv4} ||= {};
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{IPv4}->{ $field_hash->{field_order} } = $field_hash->{field};
		}
		elsif ($field_hash->{field} eq 'proto' and $field_hash->{pattern_type} eq 'QSTRING'){
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{PROTO} ||= {};
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{PROTO}->{ $field_hash->{field_order} } = $field_hash->{field};
		}
		elsif ($field_hash->{field} eq 'country_code' and $field_hash->{pattern_type} eq 'QSTRING'){
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{COUNTRY_CODE} ||= {};
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{COUNTRY_CODE}->{ $field_hash->{field_order} } = $field_hash->{field};
		}
	}
			
	# Find fields by arranged by order
	$ret->{fields_by_order} = {};
	foreach my $field_hash (@{ $ret->{fields} }){
		$ret->{fields_by_order}->{ $field_hash->{class_id} } ||= {};
		$ret->{fields_by_order}->{ $field_hash->{class_id} }->{ $field_hash->{field_order} } = $field_hash;
	}
	
	# Find fields by arranged by short field name
	$ret->{fields_by_name} = {};
	foreach my $field_hash (@{ $ret->{fields} }){
		$ret->{fields_by_name}->{ $field_hash->{field} } ||= [];
		push @{ $ret->{fields_by_name}->{ $field_hash->{field} } }, $field_hash;
	}
	
	return $ret;
}

sub _init_cache {
	my ($query, $sth);
	$query = 'SELECT id, program FROM programs';
	$sth = $Dbh->prepare($query);
	$sth->execute();
	while (my $row = $sth->fetchrow_hashref){
		$Cache->{ $row->{program} } = $row->{id};
	}
}

__END__
for my $i ( 0 .. ( $num_children - 1 ) ) {
	my $fh;
	my $pid = open( $fh, "|-" );    # fork and send to child's STDIN
	$fh->autoflush(1);
	die("Couldn't fork: $!") unless defined $pid;
	if ($pid) {
		$pipes->{$i} = { pid => $pid, fh => $fh, counter => 0 };
		$SIG{TERM} = sub {
			$Run = 0;
			#$Log->info("indexer.pl is shutting down");

			# Shut down all children
			foreach my $i ( keys %{$pipes} ) {
				my $pid = $pipes->{$i}->{pid};
				#$Log->debug("Sending SIGALRM to $pid");
				kill SIGALRM, $pid;    # send SIGALRM to each so they finish up
				                  # Send SIGTERM so they stop their continue loops
				#$Log->debug("Sending SIGTERM to $pid");
				kill SIGTERM, $pid;
				exit;
			}
			$SIG{CHLD} = sub{
				#$Log->debug("Got SIGCHLD");
				exit;	
			}
			
		};
	}
	else {
		# child worker
		$Log = Log::Log4perl::get_logger("ELSA") or die("Unable to init logger\n"); # get a new logger to not conflict with parent
		$pipes = undef;           # to avoid any confusion
		$SIG{TERM} = sub {
			$Log->info("Worker $$ is shutting down");
			$continue = 0;
		};

		OUTER_LOOP: while ($continue) {
			eval {
				INNER_LOOP: while ($continue) {
					$Log->debug("Starting process_batch");
					#my $num_processed = $kid_writer->process_batch();
					my $num_processed = _process_batch();
					$Log->debug("Processed $num_processed records");
					sleep 1 unless $num_processed; # avoid batch-bombing if our parent handle closes
				}
			};
			if ($@) {
				my $e = $@;
				$Log->error($e);
				sleep 1;                                # to avoid errmsg flooding
			}
		}
		exit;
	}
}

my $status_check_limit = 10_000;

# Still parent down here
while (<>) {
	last unless $Run;

	# child id will be line number (total records read) modulo number of sweatshop workers
	$pipes->{ ( $. % $num_children ) }->{fh}->print($_);
	$pipes->{ ( $. % $num_children ) }->{counter}++;
	if ( $. % $status_check_limit == 0 ) {
		foreach my $kid ( sort keys %{$pipes} ) {
			printf( "Worker %d processed %d logs\n", $kid, $pipes->{$kid}->{counter} );
		}
	}
}
foreach my $kid ( sort keys %{$pipes} ) {
	printf( "Worker %d processed %d logs\n", $kid, $pipes->{$kid}->{counter} );
}
