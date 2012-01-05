#!/usr/bin/perl
use strict;
use Getopt::Std;
use Time::HiRes qw(time);
use FindBin;
use Log::Log4perl::Level;

use lib $FindBin::Bin . '/lib';

use API;

my %opts;
getopts('c:', \%opts);

my $config_file;
if ($opts{c}){
	$config_file = $opts{c};
}
elsif ($ENV{ELSA_CONF}){
	$config_file = $ENV{ELSA_CONF};
}
else {
	$config_file = '/etc/elsa.conf';
}
die('Cannot find config file, specify with -c or env variable ELSA_CONF') unless -f $config_file;
my $api = API->new(config_file => $config_file) or die('Unable to start from given config file.');
$api->log->level($ERROR); # we don't want to fill our logs up with automated query logs
my $start = time();
my $num_run = $api->run_schedule();
my $duration = time() - $start;
print "Ran $num_run queries in $duration seconds.\n";
print "Running archive queries...\n";
$api->run_archive_queries();
print "done.\n";


