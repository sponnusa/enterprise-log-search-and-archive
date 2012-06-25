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
use FindBin;

# Include the directory this script is in
use lib $FindBin::Bin;

use Indexer;
use Reader;
use Writer;

my %Opts;
getopts('onlc:f:', \%Opts);

$| = 1;
my $pipes     = {};
my $Conf_file = $Opts{c} ? $Opts{c} : '/etc/elsa_node.conf';
my $Config_json = Config::JSON->new( $Conf_file );
my $Conf = $Config_json->{config}; # native hash is 10x faster than using Config::JSON->get()

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

my $num_children = $Conf->{num_log_readers} or die("undefined config for num_log_readers");
my $continue     = 1;
my $Run          = 1;
my $Class_info = Reader::get_class_info($Dbh);
my $Cache = {};
_init_cache();

unless (-f $Conf->{sphinx}->{config_file}){
	_create_sphinx_conf();
}

unless ($Opts{n}){
	print "Validating directory...\n";
	my $indexer = new Indexer(log => $Log, conf => $Config_json, class_info => $Class_info);
	$indexer->initial_validate_directory();
}

if ($Opts{l}){
	print "Loading existing buffers\n";
	my $indexer = new Indexer(log => $Log, conf => $Config_json, class_info => $Class_info);
	$indexer->load_buffers();
	exit;
}

if ($Opts{f}){
	print "Processing file $Opts{f}...\n";
	_process_batch($Opts{f});
	exit;
}

if ($Opts{o}){
	print "Running once\n";
	$Run = 0;
}

$SIG{TERM} = sub { $Run = 0; $Log->warn('Shutting down'); };
$SIG{CHLD} = 'IGNORE'; # will do the wait() so we don't create zombies

my $total_processed = 0;
my $Realtime_enabled = defined $Conf->{realtime};
my $Realtime = $Realtime_enabled ? $Conf->{realtime} : undef;
my $rt_indexed = 0;
do {
	if ($Realtime_enabled){
		my ($num_processed, $current_id) = _realtime_process();
		$total_processed += $num_processed;
		$rt_indexed += $num_processed;
		
		# Check to see if we need to exit realtime mode
		if (($num_processed / $Conf->{sphinx}->{index_interval}) > $Realtime->{rate}){
			$Log->info('Leaving realtime processing because rate was ' . ($num_processed / $Conf->{sphinx}->{index_interval}));
			$Realtime_enabled = 0;
		}
	}
	else {
		$Log->debug("Starting process_batch");
		eval {
			my $num_processed = _process_batch();
			$total_processed += $num_processed;
			$Log->debug("Processed $num_processed records");
			# Check to see if we need to enter realtime mode
			if ($Realtime and $Realtime->{rate} and $Conf->{sphinx}->{index_interval} and 
				($num_processed / $Conf->{sphinx}->{index_interval}) < $Realtime->{rate}){
				$Log->info('Entering realtime processing because rate was ' . ($num_processed / $Conf->{sphinx}->{index_interval}));
				$Realtime_enabled = 1;
			}
			sleep 1 unless $num_processed; # avoid batch-bombing if our parent handle closes
		};
		if ($@) {
			my $e = $@;
			$Log->error($e);
			sleep 1 if $Run;                                # to avoid errmsg flooding
		}
	}
	$Conf = $Config_json->{config} if $Run; # reload the config in case it has changed on disk
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
	my $indexer = new Indexer(log => $Log, conf => $Config_json, class_info => $Class_info);
	open(FH, '>' . $Conf->{sphinx}->{config_file}) or die("Cannot open config file for writing: $!");
	print FH $indexer->get_sphinx_conf();
	close(FH);
	print 'Wrote new config to file ' . $Conf->{sphinx}->{config_file} . "\n";
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
	$fh->blocking(1);
	
	die "Non-existent buffer_dir: " . $Conf->{buffer_dir}
		unless -d $Conf->{buffer_dir};
		
	$args->{start_time} = Time::HiRes::time();
		
	$args->{tempfile} = File::Temp->new( DIR => $Conf->{buffer_dir}, UNLINK => 0 );
	unless ($args->{tempfile}){
		$Log->error('Unable to create tempfile: ' . $!);
		return 0;
	}
	$args->{tempfile}->autoflush(1);
	$args->{batch_counter} = 0;
	$args->{error_counter} = 0;
	
	# Reset the miss cache
	$args->{cache_add} = {};
	
	# End the loop after index_interval seconds
	local $SIG{ALRM} = sub {
		$Log->trace("ALARM");
		$args->{run} = 0;
		# safety in case we don't receive any logs, we'll still do post_proc and restart loop
		$fh->blocking(0); 
	};
	unless ($args->{offline_processing}){
		alarm $Conf->{sphinx}->{index_interval};
	}
	
	my $reader = new Reader(log => $Log, conf => $Config_json, cache => $Cache, offline_processing => $args->{offline_processing});
	
	while (<$fh>){	
		eval { 
			$args->{tempfile}->print(join("\t", 0, @{ $reader->parse_line($_) }) . "\n"); # tack on zero for auto-inc value
			$args->{batch_counter}++;
		};
		if ($@){
			my $e = $@;
			$args->{error_counter}++;
			if ($Conf->{log_parse_errors}){
				$Log->error($e) 
			}
		}
		last unless $args->{run};
	}
	
	# Update args to be results
	$args->{file} = $args->{tempfile}->filename();
	$args->{start} = $args->{offline_processing} ? $reader->offline_processing_times->{start} : $args->{start_time};
	$args->{end} = $args->{offline_processing} ? $reader->offline_processing_times->{end} : Time::HiRes::time();
	$args->{total_processed} = $args->{batch_counter};
	$args->{total_errors} = $args->{error_counter};
	
	# Report back that we've finished
	$Log->debug("Finished job process_batch with cache hits: $args->{batch_counter} and " . (scalar keys %{ $args->{cache_add} }) . ' new programs');
	$Log->debug('Total errors: ' . $args->{error_counter} . ' (%' . (($args->{error_counter} / $args->{batch_counter}) * 100) . ')' ) if $args->{batch_counter};
	
	my ($query, $sth);
	if (scalar keys %{ $reader->to_add }){
		my $indexer = new Indexer(log => $Log, conf => $Config_json, class_info => $Class_info);
		$indexer->add_programs($reader->to_add);
		$reader->to_add({});
	}
	
	if ($args->{batch_counter}){
		$query = 'INSERT INTO buffers (filename) VALUES (?)';
		$sth = $Dbh->prepare($query);
		$sth->execute($args->{file});
		$Log->trace('inserted filename ' . $args->{file} . ' with batch_counter ' . $args->{batch_counter});
	}
		
	# Reset the run marker
	$args->{run} = 1;
	
	# Fork our post-batch processor
	return $args->{batch_counter} unless $args->{batch_counter};
	my $pid = fork();
	if ($pid){
		# Parent
		return $args->{batch_counter};
	}
	# Child
	$Log->trace('Child started');
	eval {
		my $indexer = new Indexer(log => $Log, conf => $Config_json);
		$indexer->load_buffers();
	};
	if ($@){
		$Log->error('Child encountered error: ' . $@);
	}
	$Log->trace('Child finished');
	exit; # done with child
}

sub _realtime_process {
	my $args = { run => 1, start_time => Time::HiRes::time() };
	
	my $fh = \*STDIN;
	$fh->autoflush(1);
	$fh->blocking(1);
	
	# End the loop after index_interval seconds
	local $SIG{ALRM} = sub {
		$Log->trace("ALARM");
		$args->{run} = 0;
		# check to see if we need to stop processing
		$fh->blocking(0);
	};
	alarm $Conf->{sphinx}->{index_interval};
			
	my $reader = new Reader(log => $Log, conf => $Config_json, cache => $Cache);
	my $writer = new Writer(log => $Log, conf => $Config_json);
	
	while (<$fh>){	
		eval { 
			$writer->write($reader->parse_line($_));
			$args->{batch_counter}++;
		};
		if ($@){
			my $e = $@;
			$args->{error_counter}++;
			if ($Conf->{log_parse_errors}){
				$Log->error($e) 
			}
		}
		last unless $args->{run};
	}
	
	# Disable the alarm if we haven't already
	alarm 0;
	
	# Insert any that are pending
	$writer->realtime_batch_insert();
	
	return ($args->{batch_counter}, $writer->current_id);
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
