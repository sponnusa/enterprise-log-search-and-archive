#!/usr/bin/perl
use strict;
use Data::Dumper;
use Time::HiRes qw(time);
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

my $start = time();
my $api = API->new(config_file => $config_file);
my $user = User->new(conf => $api->conf, username => 'system');

my $q = $api->query({query_string => $opts{q}, user => $user});
my $duration = time() - $start;
exit unless $q->results->total_records;
my $format = $opts{f} ? $opts{f} : 'tsv';
if ($q->has_warnings){
	foreach (@{ $q->warnings }){
		print "$_\n";
	}
}
print $api->format_results({ results => $q->results->results, format => $format }) . "\n";
print "Finished in $duration seconds.\n";