#!/usr/bin/perl
use strict;
use Data::Dumper;
use IO::Handle;
use POSIX;
use FindBin;

# Include the directory this script is in
use lib $FindBin::Bin;

use ELSA::Writer;

$| = 1;
my $pipes     = {};
my $conf_file = '/etc/elsa.conf';
my $writer   = new ELSA::Writer( $conf_file );
open( FH, ">> " . $writer->conf->get('logdir') . "/log" );
print FH "starting up\n";
my $num_children = $writer->conf->get('num_indexers') or die("undefined config for num_indexers");
my $continue     = 1;
my $Run          = 1;

# See if we're configured to use Patchlink to assign default class id's
my $Default_classes_by_host = 0;
if ($writer->conf->get('patchlink')){
	my ($dbh, $query, $sth);
	$dbh = DBI->connect($writer->conf->get('patchlink/db/dsn'), 
		$writer->conf->get('patchlink/db/user'), 
		$writer->conf->get('patchlink/db/pass'), { RaiseError => 1 }) or die('Unable to connect to patchlink db');
	$Default_classes_by_host = {};
	foreach my $group_name (keys %{ $writer->conf->get('patchlink/groups') }){
		$query = 'SELECT id FROM classes WHERE class=?';
		$sth = $writer->db->prepare($query);
		$sth->execute($writer->conf->get('patchlink/groups/' . $group_name));
		my $row = $sth->fetchrow_hashref;
		if ($row){
			my $class_id = $row->{id};
			$query = 'SELECT DISTINCT ContactAddress FROM UP_Agents t1' . "\n" .
				'JOIN AgentGroup t2 ON (t1.AgentID_INT=t2.AgentID_INT)' . "\n" .
				'JOIN Groups t3 ON (t2.GroupID=t3.Group_ID)' . "\n" .
				'WHERE t3.Group_Name=?';
			$sth = $dbh->prepare($query);
			$sth->execute($group_name);
			while (my $row = $sth->fetchrow_hashref){
				$Default_classes_by_host->{ $row->{ContactAddress} } = $class_id;
			}
		}
	} 
	$writer->log->debug('Starting up with default classes by host: ' . Dumper($Default_classes_by_host));
}

for my $i ( 0 .. ( $num_children - 1 ) ) {
	my $fh;
	my $pid = open( $fh, "|-" );    # fork and send to child's STDIN
	die("Couldn't fork: $!") unless defined $pid;
	if ($pid) {
		print FH "Forking kid $i with pid $pid\n";
		$pipes->{$i} = { pid => $pid, fh => $fh, counter => 0 };
		$SIG{TERM} = sub {
			$Run = 0;
			print FH "shutting down\n";
			$writer->log->info("indexer.pl is shutting down");

			# Shut down all children
			foreach my $i ( keys %{$pipes} ) {
				my $pid = $pipes->{$i}->{pid};
				$writer->log->debug("Sending SIGALRM to $pid");
				kill SIGALRM, $pid;    # send SIGALRM to each so they finish up
				                  # Send SIGTERM so they stop their continue loops
				$writer->log->debug("Sending SIGTERM to $pid");
				kill SIGTERM, $pid;
			}
			$SIG{CHLD} = sub{
				$writer->log->debug("Got SIGCHLD");
				exit;	
			}
			
		};
	}
	else {
		# child worker
		$pipes = undef;           # to avoid any confusion
		$SIG{TERM} = sub {
			$writer->log->info("Worker $$ is shutting down");
			$continue = 0;
		};

		open( KIDFH, "> " . $writer->conf->get('logdir') . "/log$$" );
		OUTER_LOOP: while ($continue) {
			eval {
				my $kid_writer = new ELSA::Writer( $conf_file, $i + 1 );
				if ($Default_classes_by_host){
					$kid_writer->default_classes($Default_classes_by_host);
				}
				INNER_LOOP: while ($continue) {
					$writer->log->debug("Starting process_batch");
					my $num_processed = $kid_writer->process_batch();
					$writer->log->debug("Processed $num_processed records");
					#last if $batch_id > 10; # uncomment for a quick test
				}
			};
			if ($@) {
				my $e = $@;
				ELSA::log_error($e);
				sleep 1;                                # to avoid errmsg flooding
			}
		}
		print KIDFH "Child exiting\n";
		close(KIDFH);
		exit;
	}
}
close(FH);

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

