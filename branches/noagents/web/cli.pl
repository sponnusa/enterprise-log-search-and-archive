#!/usr/bin/perl
use strict;
use Data::Dumper;
use Log::Log4perl;
use Config::JSON;
use JSON -convert_blessed_universally;
use DBI;
use JSON;
use Getopt::Std;
use FindBin;
use lib $FindBin::Bin . '/lib';
use API;

my $config_file = -f '/etc/elsa.conf' ? '/etc/elsa.conf' : '/usr/local/elsa/etc/elsa.conf';
if ($ENV{ELSA_CONF}){
	$config_file = $ENV{ELSA_CONF};
}

my %opts;
getopts('f:q:c:', \%opts);
if ($opts{c}){
	$config_file = $opts{c};
}

my $conf = new Config::JSON ( $config_file ) or die("Unable to open config file");
my $log_level = 'DEBUG';
if ($conf->get('debug_level')){
	$log_level = $conf->get('debug_level');
}
my $logdir = $conf->get('logdir');
	my $log_conf = qq(
		log4perl.category.Web       = $log_level, File
		log4perl.appender.File			 = Log::Log4perl::Appender::File
		log4perl.appender.File.filename  = $logdir/web.log 
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
my $logger = Log::Log4perl::get_logger('Web')
  or die("Unable to init logger\n");
my $json;
if ($log_level eq 'DEBUG' or $log_level eq 'TRACE'){
	$json = JSON->new->pretty->allow_nonref->allow_blessed;	
}
else {
	$json = JSON->new->allow_nonref->allow_blessed;
}

my $dbh = DBI->connect(
	$conf->get('meta_db/dsn'),
	$conf->get('meta_db/username'),
	$conf->get('meta_db/password'),
	{ 
		RaiseError => 1,
		HandleError => sub { warn shift },
		mysql_auto_reconnect => 1, # we will auto-reconnect on disconnect
	}
) or die($DBI::errstr);

my $api = API->new(conf => $conf, log => $logger, json => $json, db => $dbh);
my $user_info = $api->get_user_info('system');

#print Dumper($api->query({query => {'srcip' => $opts{q}}, user_info => $user_info}));

my $result = $api->query({query_string => $opts{q}, user_info => $user_info});
exit unless $result and ref($result) eq 'HASH' and $result->{results} and ref($result->{results}) eq 'ARRAY';
$result->{format} = $opts{f} ? $opts{f} : 'tsv'; 
print $api->format_results($result) . "\n";