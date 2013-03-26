#!/usr/bin/perl
use strict;
use Getopt::Std;
use Time::HiRes qw(time);
use FindBin;

use lib $FindBin::Bin . '/../node/';
use lib $FindBin::Bin . '/lib';

use Indexer;
use API;
use User;

my %opts;
getopts('c:n:', \%opts);

my $config_file;
if ($opts{c}){
	$config_file = $opts{c};
}
elsif ($ENV{ELSA_CONF}){
	$config_file = $ENV{ELSA_CONF};
}
else {
	$config_file = '/etc/elsa_web.conf';
}
die('Cannot find config file, specify with -c or env variable ELSA_CONF') unless -f $config_file;

my $node_config_file;
if ($opts{n}){
	$node_config_file = $opts{n};
}
elsif ($ENV{ELSA_NODE_CONF}){
	$node_config_file = $ENV{ELSA_NODE_CONF};
}
else {
	$node_config_file = '/etc/elsa_node.conf';
}
die('Cannot find node config file, specify with -n or env variable ELSA_NODE_CONF') unless -f $node_config_file;

$ENV{DEBUG_LEVEL} = 'ERROR'; # we don't want to fill our logs up with automated query logs

my $indexer = Indexer->new(config_file => $node_config_file);

eval {
	# Handle node activities, like loading buffers
	print "Indexing buffers...\n";
	$indexer->load_buffers() or return;
	print "...finished.\n";
	
	# Attempt to get a lock to ensure there are no other cron.pl's querying right now
	unless($indexer->_get_lock('query', 1)){
		my $msg = 'Another cron.pl script is querying, exiting';
		warn $msg;
		$indexer->log->error($msg);
		exit;
	}
	
	# Handle web activities, like scheduled searches
	my $api = API->new(config_file => $config_file) or die('Unable to start from given config file.');
	my $start = time();
	my $user = User->new(conf => $api->conf, username => 'system');
	my $num_run = $api->run_schedule({user => $user});
	my $duration = time() - $start;
	$api->log->trace("Ran $num_run queries in $duration seconds.");
	
	# Unlock so that the next cron.pl can make schedule queries
	$indexer->_release_lock('query');
	
	# Archive queries are expected to take a long time and can run concurrently
	$api->log->trace("Running archive queries...");
	$api->run_archive_queries({user => $user});
};
if ($@){
	warn('Error: ' . $@);
	$indexer->log->error('Error: ' . $@);
}

$indexer->log->trace('cron.pl finished.');


