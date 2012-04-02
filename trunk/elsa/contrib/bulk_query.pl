#!/usr/bin/perl
use strict;
use Data::Dumper;
use Time::HiRes qw(time);
use Getopt::Std;
use FindBin;
use lib $FindBin::Bin . '/../web/lib', $FindBin::Bin . '/lib';
use API;
use LWP::UserAgent;
use Date::Manip;
my %Opts;
getopts('c:f:s:e:ht', \%Opts);

if ($Opts{h} or not $Opts{f}){
	print <<EOT
Usage:
perl bulk_query.pl -f <file containing terms> [ -c <config file> ] [ -s <start time> ] [ -e <end time> ] [ -t (format TSV) ]
EOT
;
}

my @terms;
open(FH, $Opts{f}) or die($!);
while (<FH>){
	chomp;
	push @terms, $_;
}
close(FH);

my $config_file = -f '/etc/elsa_web.conf' ? '/etc/elsa_web.conf' : '/usr/local/elsa/web/etc/elsa.conf';
if ($ENV{ELSA_CONF}){
	$config_file = $ENV{ELSA_CONF};
}
elsif ($Opts{c}){
	$config_file = $Opts{c};
}

my $start = time() - 86400;
if ($Opts{s}){
	$start = UnixDate(ParseDate($Opts{s}), '%s');
}
my $end = time() - 60;
if ($Opts{e}){
	$end = UnixDate(ParseDate($Opts{e}), '%s');
}

my $api = API->new(config_file => $config_file);
my $user_info = $api->get_user_info('system');

my $stats_start = time();
for (my $i = 0; $i < @terms; $i += 30){
	my $query_string = join(' ', @terms[$i..($i+30)]);
	print $query_string . "\n";
	my $result = $api->query({query_string => $query_string, 
		query_meta_params => { start => $start, end => $end }, 
		user_info => $user_info});
	my $duration = time() - $stats_start;
	next unless $result and ref($result) eq 'HASH' and $result->{results} and ref($result->{results});
	$result->{format} = $Opts{t} ? 'tsv' : 'json';
	if ($result->{errors}){
		foreach (@{ $result->{errors} }){
			print "$_\n";
		}
	}
	print $api->format_results($result) . "\n" if $result->{totalRecords};
}

