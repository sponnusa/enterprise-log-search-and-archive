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
my $conf_file = '/usr/local/elsa/etc/elsa.conf';
my $writer   = new ELSA::Writer( $conf_file );
my $num_children = $writer->conf->get('num_indexers') or die("undefined config for num_indexers");
my $continue     = 1;
my $Run          = 1;

for my $i ( 0 .. ( $num_children - 1 ) ) {
	my $fh;
	my $pid = open( $fh, "|-" );    # fork and send to child's STDIN
	$fh->autoflush(1);
	die("Couldn't fork: $!") unless defined $pid;
	if ($pid) {
		$pipes->{$i} = { pid => $pid, fh => $fh, counter => 0 };
		$SIG{TERM} = sub {
			$Run = 0;
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

		OUTER_LOOP: while ($continue) {
			eval {
				my $kid_writer = new ELSA::Writer( $conf_file, $i + 1 );
				INNER_LOOP: while ($continue) {
					$writer->log->debug("Starting process_batch");
					my $num_processed = $kid_writer->process_batch();
					$writer->log->debug("Processed $num_processed records");
					sleep 1 unless $num_processed; # avoid batch-bombing if our parent handle closes
					#last if $batch_id > 10; # uncomment for a quick test
				}
			};
			if ($@) {
				my $e = $@;
				ELSA::log_error($e);
				sleep 1;                                # to avoid errmsg flooding
			}
		}
		exit;
	}
}

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

