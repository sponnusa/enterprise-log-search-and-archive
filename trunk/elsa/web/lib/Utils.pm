package Utils;
use Moose::Role;
with 'MooseX::Log::Log4perl';
use Config::JSON;
use DBI;
use JSON;
use IO::Handle;
use IO::File;
use Digest::HMAC_SHA1;

our $Db_timeout = 3;
our $Bulk_dir = '/tmp';

has 'log' => ( is => 'ro', isa => 'Log::Log4perl::Logger', required => 1 );
has 'conf' => (is => 'rw', isa => 'Object', required => 1);
has 'db' => (is => 'rw', isa => 'Object', required => 1);
has 'json' => (is => 'ro', isa => 'JSON', required => 1);
#has 'bulk_dir' => (is => 'rw', isa => 'Str', required => 1, default => $Bulk_dir);
has 'db_timeout' => (is => 'rw', isa => 'Int', required => 1, default => $Db_timeout);

around BUILDARGS => sub {
	my $orig = shift;
	my $class = shift;
	my %params = @_;
		
	if ($params{config_file}){
		$params{conf} = new Config::JSON ( $params{config_file} ) or die("Unable to open config file");
	}		
	
	my $log_level = 'DEBUG';
	if ($ENV{DEBUG_LEVEL}){
		$log_level = $ENV{DEBUG_LEVEL};
	}
	elsif ($params{conf}->get('debug_level')){
		$log_level = $params{conf}->get('debug_level');
	}
	my $logdir = $params{conf}->get('logdir');
	my $logfile = 'web.log';
	if ($params{conf}->get('logfile')){
		$logfile = $params{conf}->get('logfile');
	}
	
	my $log_conf = qq(
		log4perl.category.App       = $log_level, File
		log4perl.appender.File			 = Log::Log4perl::Appender::File
		log4perl.appender.File.filename  = $logdir/$logfile 
		log4perl.appender.File.layout = Log::Log4perl::Layout::PatternLayout
		log4perl.appender.File.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %x%n%m%n
		log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
		log4perl.appender.Screen.stderr  = 1
		log4perl.appender.Screen.layout = Log::Log4perl::Layout::PatternLayout
		log4perl.appender.Screen.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %x%n%m%n
		log4perl.appender.Syncer            = Log::Log4perl::Appender::Synchronized
		log4perl.appender.Syncer.appender   = File
	);
		
	Log::Log4perl::init( \$log_conf ) or die("Unable to init logger\n");
	$params{log} = Log::Log4perl::get_logger('App')
	  or die("Unable to init logger\n");
	
	if ($params{conf}->get('db/timeout')){
		$Db_timeout = $params{conf}->get('db/timeout');
	}
	
	$params{db} = DBI->connect(
		$params{conf}->get('meta_db/dsn'),
		$params{conf}->get('meta_db/username'),
		$params{conf}->get('meta_db/password'),
		{ 
			PrintError => 0,
			HandleError => \&_dbh_error_handler,
			AutoCommit => 1,
			mysql_connect_timeout => $Db_timeout,
			mysql_auto_reconnect => 1, # we will auto-reconnect on disconnect
		}
	) or die($DBI::errstr);
	
	if ($params{conf}->get('debug_level') eq 'DEBUG' or $params{conf}->get('debug_level') eq 'TRACE'){
		$params{json} = JSON->new->pretty->allow_nonref->allow_blessed->convert_blessed;	
	}
	else {
		$params{json} = JSON->new->allow_nonref->allow_blessed->convert_blessed;
	}
	
	if ($params{conf}->get('bulk_dir')){
		$Bulk_dir = $params{conf}->get('bulk_dir');
	}
	
	return $class->$orig(%params);
};

sub _dbh_error_handler {
	my $errstr = shift;
	my $dbh    = shift;
	my $query  = $dbh->{Statement};

	$errstr .= " QUERY: $query";
	Log::Log4perl::get_logger('App')->error($errstr);
	foreach my $sth (grep { defined } @{$dbh->{ChildHandles}}){
		$sth->rollback; # in case there was an active transaction
	}
	
	confess($errstr);
}

sub freshen_db {
	my $self = shift;
	$self->db(
		DBI->connect_cached(
			$self->conf->get('meta_db/dsn'),
			$self->conf->get('meta_db/username'),
			$self->conf->get('meta_db/password'),
			{ 
				PrintError => 0,
				HandleError => \&_dbh_error_handler,
				#RaiseError => 1,
				AutoCommit => 1,
				mysql_connect_timeout => $Db_timeout,
				mysql_auto_reconnect => 1, # we will auto-reconnect on disconnect
			})
	);
}

sub epoch2iso {
	my $epochdate = shift;
	my $use_gm_time = shift;
	
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst);
	if ($use_gm_time){
		($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime($epochdate);
	}
	else {
		($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($epochdate);
	}
	my $date = sprintf("%04d-%02d-%02d %02d:%02d:%02d", 
		$year + 1900, $mon + 1, $mday, $hour, $min, $sec);
	return $date;
}

sub get_hash {
	my ($self, $data) = shift;
	my $digest = new Digest::HMAC_SHA1($self->conf->get('link_key'));
	$digest->add($data);
	return $digest->hexdigest();
}

1;
