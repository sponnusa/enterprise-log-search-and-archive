#!/usr/bin/perl
use strict;
use Getopt::Std;
use Net::Server::Daemonize qw(daemonize);
use FindBin;

use lib $FindBin::Bin . '/lib';
use Janus;

my %opts;
getopts('Ddc:', \%opts);

my $config_file_name = -f '/etc/elsa.conf' ? '/etc/elsa.conf' : '/usr/local/elsa/etc/elsa.conf';
if ($opts{c}){
	$config_file_name = $opts{c};
}

my $janus = new Janus($config_file_name);

unless ($opts{D}){
	daemonize($janus->conf->get('Janus/user'), $janus->conf->get('Janus/group'), $janus->conf->get('Janus/pidfile'));
}


$janus->run();

__END__
