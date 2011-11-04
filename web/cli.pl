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
my $user_info = $api->get_user_info('system');

my $result = $api->query({query_string => $opts{q}, user_info => $user_info});
my $duration = time() - $start;
exit unless $result and ref($result) eq 'HASH' and $result->{results} and ref($result->{results}) eq 'ARRAY';
$result->{format} = $opts{f} ? $opts{f} : 'tsv';
if ($result->{errors}){
	foreach (@{ $result->{errors} }){
		print "$_\n";
	}
}
print $api->format_results($result) . "\n";
print "Finished in $duration seconds.\n";