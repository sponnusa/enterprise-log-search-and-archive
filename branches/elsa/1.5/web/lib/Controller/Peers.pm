package Controller::Peers;
use Moose;
extends 'Controller';
use Data::Dumper;
use Log::Log4perl::Level;
use AnyEvent::HTTP;
use URI::Escape qw(uri_escape);
use File::Copy;
use Archive::Extract;
use Digest::MD5;
use IO::File;
use Time::HiRes qw(time);
use Hash::Merge::Simple qw(merge);
use File::Path;
use Try::Tiny;
use Ouch qw(:trytiny);

use lib qw(../);
use Utils;
use QueryParser;

use Import;

sub local_query {
	my ($self, $args, $cb) = @_;
	
	# We may need to recursively redirect from one class to another, so the execution is wrapped in this sub
	my $run_query;
	$run_query = sub {
		my $qp = shift;
		my $cb = pop(@_);
		my $class = shift;
		my $extra_directives = shift;
		my $q = $qp->parse($class);
		
		Log::Log4perl::MDC->put('qid', $q->qid);
		
		try {
			if ($self->conf->get('disallow_sql_search') and $qp->query_class eq 'Query::SQL'){
				my $msg;
				if (scalar keys %{ $qp->stopword_terms }){
					throw(413, 'Cannot execute query, terms too common: ' . join(', ', keys %{ $qp->stopword_terms }), { terms => join(', ', keys %{ $qp->stopword_terms }) });
				}
				else {
					throw(405, 'Query required SQL search which is not enabled', { search_type => 'SQL' });
				}
			}
			if ($extra_directives){
				# These directives were added by another class, not the user
				$self->log->trace('Extra directives: ' . Dumper($extra_directives));
				foreach my $directive (keys %$extra_directives){
					$q->$directive($extra_directives->{$directive});
				}
			}
			my $estimate = $q->estimate_query_time();
			$self->log->trace('Query estimate: ' . Dumper($estimate));
			$q->execute(sub { $cb->($q) });
		}
		catch {
			my $e = shift;
			if (caught(302, $e)){
				my $redirected_class = $e->data->{location};
				my $directives = $e->data->{directives};
				throw(500, 'Redirected, but no class given', { term => $class }) unless $redirected_class;
				if ($class and $redirected_class and $redirected_class eq $class){
					throw(500, 'Class ' . $class . ' was the same as ' . $redirected_class . ', infinite loop.', { term => $class });
				}
				$self->log->info("Redirecting to $redirected_class");
				$run_query->($qp, $redirected_class, $directives, $cb);
			}
			else {
				die($e);
			}
		};
	};
	
	try {
		QueryParser->new(conf => $self->conf, log => $self->log, %$args, on_connect => sub {
			my $qp = shift;
			$run_query->($qp, sub {
				my $q = shift;
				foreach my $warning ($self->all_warnings){
					push @{ $q->warnings }, $warning;
				}
				
				$self->log->info(sprintf("Query " . $q->qid . " returned %d rows", $q->results->records_returned));
				
				$q->time_taken(int((Time::HiRes::time() - $q->start_time) * 1000));
			
				# Apply transforms
				$q->transform_results(sub { 
					$q->dedupe_warnings();
					$cb->($q);
				});
			});
		});
	}
	catch {
		my $e = shift;
		$cb->($e);
	};
}

sub local_info {
	my ($self, $args, $cb) = @_;
	
	try {
		my $ret;
		my $cv = AnyEvent->condvar;
		$cv->begin(sub { $cb->($ret) });
		$self->_get_info(1, sub {
			$ret = shift;
			$cv->end;
		});
	}
	catch {
		my $e = shift;
		$cb->($e);
	};
}

sub local_stats {
	my ($self, $args, $cb) = @_;
	
	try {
		my $ret;
		my $cv = AnyEvent->condvar;
		$cv->begin(sub { $cb->($ret) });
		$self->get_stats($args, sub {
			$ret = shift;
			$cv->end;
		});
	}
	catch {
		my $e = shift;
		$cb->($e);
	};
}

sub stats {
	my ($self, $args, $cb) = @_;
	
	my ($query, $sth);
	my $overall_start = time();
	
	# Execute search on every peer
	my @peers;
	foreach my $peer (keys %{ $self->conf->get('peers') }){
		push @peers, $peer unless $peer eq $args->{from_peer};
	}
	$self->log->trace('Executing global node_info on peers ' . join(', ', @peers));
	
	my %results;
	my $cv = AnyEvent->condvar;
	$cv->begin(sub { $cb->(\%results) });
	my %stats;
	foreach my $peer (@peers){
		$cv->begin;
		my $peer_conf = $self->conf->get('peers/' . $peer);
		my $url = $peer_conf->{url} . 'API/';
		$url .= ($peer eq '127.0.0.1' or $peer eq 'localhost') ? 'local_stats' : 'stats';
		$url .= '?start=' . uri_escape($args->{start}) . '&end=' . uri_escape($args->{end});
		$self->log->trace('Sending request to URL ' . $url);
		my $start = time();
		my $headers = { 
			Authorization => $self->_get_auth_header($peer),
		};
		$results{$peer} = http_get $url, headers => $headers, sub {
			my ($body, $hdr) = @_;
			try {
				my $raw_results = $self->json->decode($body);
				$stats{$peer}->{total_request_time} = (time() - $start);
				$results{$peer} = { %$raw_results }; #undef's the guard
				# Touch up nodes to have the correct label
				foreach my $node (keys %{ $results{$peer}->{nodes} }){
					if (($peer eq 'localhost' or $peer eq '127.0.0.1') and $args->{peer_label}){
						$results{$peer}->{nodes}->{ $args->{peer_label} } = delete $results{$peer}->{nodes}->{$node};
					}
					elsif ($node eq 'localhost' or $node eq '127.0.0.1'){
						$results{$peer}->{nodes}->{$peer} = delete $results{$peer}->{nodes}->{$node};
					}
				}
			}
			catch {
				my $e = catch_any(shift);
				$self->log->error($e->message . "\nHeader: " . Dumper($hdr) . "\nbody: " . Dumper($body));
				$self->add_warning(502, 'peer ' . $peer . ': ' . $e->message, { http => $peer });
				delete $results{$peer};
			};
			$cv->end;
		};
	}
	$cv->end;
	$stats{overall} = (time() - $overall_start);
	$self->log->debug('stats: ' . Dumper(\%stats));
	
	$self->log->debug('merging: ' . Dumper(\%results));
	my $overall_final = merge values %results;
	
	return $overall_final;
}

sub upload {
	my ($self, $args, $cb) = @_;
	
	$self->log->info('Received file ' . $args->{upload}->basename . ' with size ' . $args->{upload}->size 
		. ' from client ' . $args->{client_ip_address});
	my ($query, $sth);
	
	my $syslog_db_name = 'syslog';
	if ($self->conf->get('syslog_db_name')){
		$syslog_db_name = $self->conf->get('syslog_db_name');
	}
	
	my $ret = { ok => 1 };
	
	try {
	
		# See if this is a Zip file
		open(FH, $args->{upload}->path) or throw(500, 'Unable to read file ' . $args->{upload}->path . ': ' . $!, { file => $args->{upload}->path });
		my $buf;
		read(FH, $buf, 2);
		my $is_zipped = 0;
		# Check for zip or gz magic
		if ($buf eq 'PK' or $buf eq pack('C2', 0x1f, 0x8b)){
			$self->log->trace('Detected that file upload is an archive');
			$is_zipped = 1;
		}
		close(FH);
		
		my $file;
		
		if ($is_zipped){
			my $ae = Archive::Extract->new( archive => $args->{upload}->path ) or throw(500, 'Error extracting file ' . $args->{upload}->path . ': ' . $!, { file => $args->{upload}->path });
			my $id = $args->{client_ip_address} . '_' . $args->{md5};
			# make a working dir for these files
			my $working_dir = $self->conf->get('buffer_dir') . '/' . $id;
			mkdir($working_dir) or throw(500, "Unable to create working_dir $working_dir", { working_dir => $working_dir });
			$ae->extract( to => $working_dir ) or throw(500, $ae->error, { working_dir => $working_dir });
			my $files = $ae->files;
			if (scalar @$files > 2){
				$self->log->warn('Received more than 2 files in zip file, there should be at most one file and an optional programs file in a single zip file.');
			}
			foreach my $unzipped_file_shortname (@$files){
				my $unzipped_file = $working_dir . '/' . $unzipped_file_shortname;
				my $zipped_file = $self->conf->get('buffer_dir') . '/' . $id . '_' . $unzipped_file_shortname;
				move($unzipped_file, $zipped_file);
				
				if ($unzipped_file_shortname =~ /programs/){
					$self->log->info('Loading programs file ' . $zipped_file);
					$query = 'LOAD DATA LOCAL INFILE ? INTO TABLE ' . $syslog_db_name . '.programs FIELDS ESCAPED BY \'\'';
					$sth = $self->db->prepare($query);
					$sth->execute($zipped_file);
					unlink($zipped_file);
					next;
				}
				else {
					$file = $zipped_file;
				}
			}
			rmtree($working_dir);
		}
		else {
			$file = $args->{upload}->path;
			$file =~ /\/([^\/]+)$/;
			my $shortname = $1;
			my $destfile = $self->conf->get('buffer_dir') . '/' . $shortname;
			move($file, $destfile) or throw(500, $!, { file => $file, destfile => $destfile });
			$self->log->debug('moved file ' . $file . ' to ' . $destfile);
			$file = $destfile;
		}
		$args->{size} = -s $file;
		
		# Check md5
		my $md5 = new Digest::MD5;
		my $upload_fh = new IO::File($file);
		$md5->addfile($upload_fh);
		my $local_md5 = $md5->hexdigest;
		close($upload_fh);
		unless ($local_md5 eq $args->{md5}){
			my $msg = 'MD5 mismatch! Calculated: ' . $local_md5 . ' client said it should be: ' . $args->{md5};
			$self->log->error($msg);
			unlink($file);
			return [ 400, [ 'Content-Type' => 'text/plain' ], [ $msg ] ];
		}
		
		if ($args->{description} or $args->{name}){
			# We're doing an import
			$args->{host} = $args->{client_ip_address};
			delete $args->{start};
			delete $args->{end};
			my $importer = new Import(log => $self->log, conf => $self->conf, db => $self->db, infile => $file, %$args);
			if (not $importer->id){
				return [ 500, [ 'Content-Type' => 'application/javascript' ], [ $self->json->encode({ error => 'Import failed' }) ] ];
			}
			$ret->{import_id} = $importer->id;
		}
		else {
			unless ($args->{start} and $args->{end}){
				my $msg = 'Did not receive valid start/end times';
				$self->log->error($msg);
				unlink($file);
				return [ 400, [ 'Content-Type' => 'text/plain' ], [ $msg ] ];
			}
			
			# Record our received file in the database
			$query = 'INSERT INTO ' . $syslog_db_name . '.buffers (filename, start, end) VALUES (?,?,?)';
			$sth = $self->db->prepare($query);
			$sth->execute($file, $args->{start}, $args->{end});
			$ret->{buffers_id} = $self->db->{mysql_insertid};
			
			$args->{batch_time} ||= 60;
			$args->{total_errors} ||= 0;
			
			# Record the upload
			$query = 'INSERT INTO ' . $syslog_db_name . '.uploads (client_ip, count, size, batch_time, errors, start, end, buffers_id) VALUES(INET_ATON(?),?,?,?,?,?,?,?)';
			$sth = $self->db->prepare($query);
			$sth->execute($args->{client_ip_address}, $args->{count}, $args->{size}, $args->{batch_time}, 
				$args->{total_errors}, $args->{start}, $args->{end}, $ret->{buffers_id});
			$ret->{upload_id} = $self->db->{mysql_insertid};
			$sth->finish;
		}
	}
	catch {
		my $e = shift;
		$self->add_warning(500, $e);
		$cb->({ error => $e });
	};
		
	$cb->($ret);
}

sub local_result {
	my ($self, $args, $cb) = @_;
	
	$self->get_saved_result($args, $cb);
}

sub result {
	my ($self, $args, $cb) = @_;
	
	my ($query, $sth);
	my $overall_start = time();
	
	# Execute search on every peer that has a foreign qid
	$query = 'SELECT * FROM foreign_queries WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{qid});
	my %peers;
	while (my $row = $sth->fetchrow_hashref){
		$peers{ $row->{peer} } = $row->{foreign_qid};
	}
	
	$self->log->trace('Foreign query results on peers ' . Dumper(\%peers));
	
	my %results;
	my %stats;
	my $cv = AnyEvent->condvar;
	$cv->begin(sub { 
		$stats{overall} = (time() - $overall_start);
		$self->log->debug('stats: ' . Dumper(\%stats));
		$self->log->debug('merging: ' . Dumper(\%results));
		my $overall_final = merge values %results;
		$cb->($overall_final); 
	});
	
	
	foreach my $peer (sort keys %peers){
		$cv->begin;
		my $peer_conf = $self->conf->get('peers/' . $peer);
		my $url = $peer_conf->{url} . 'API/';
		if ($peer eq '127.0.0.1' or $peer eq 'localhost'){
			$url .= 'local_result?qid=' . int($peers{$peer}) . '&hash=' . $self->_get_hash($peers{$peer});
		}
		else {
			$url .= 'result?qid=' . int($peers{$peer});
		}
		$self->log->trace('Sending request to URL ' . $url);
		my $start = time();
		my $headers = { 
			Authorization => $self->_get_auth_header($peer),
		};
		$results{$peer} = http_get $url, headers => $headers, sub {
			my ($body, $hdr) = @_;
			try {
				my $raw_results = $self->json->decode($body);
				if ($raw_results and not $raw_results->{error}){
					my $num_results = $raw_results->{totalRecords} ? $raw_results->{totalRecords} : $raw_results->{recordsReturned};
					# Update any entries necessary
					$query = 'SELECT * FROM foreign_queries WHERE ISNULL(completed) AND qid=? AND peer=?';
					$sth = $self->db->prepare($query);
					$sth->execute($args->{qid}, $peer);
					if (my $row = $sth->fetchrow_hashref){
						$query = 'UPDATE foreign_queries SET completed=UNIX_TIMESTAMP() WHERE qid=? AND peer=?';
						$sth = $self->db->prepare($query);
						$sth->execute($args->{qid}, $peer);
						$self->log->trace('Set foreign_query ' . $args->{qid} . ' on peer ' . $peer . ' complete');
						
						if ($num_results){
							$query = 'UPDATE query_log SET num_results=num_results + ? WHERE qid=?';
							$sth = $self->db->prepare($query);
							$sth->execute($num_results, $args->{qid});
							$self->log->trace('Updated num_results for qid ' . $args->{qid} 
								. ' with ' . $num_results . ' additional records.');
						}
					}
				}
				$stats{$peer}->{total_request_time} = (time() - $start);
				$results{$peer} = { %$raw_results }; #undef's the guard
			}
			catch {
				my $e = catch_any(shift);
				$self->log->error($e->message . "\nHeader: " . Dumper($hdr) . "\nbody: " . Dumper($body));
				$self->add_warning(502, 'peer ' . $peer . ': ' . $e->message, { peer => $peer });
				delete $results{$peer};
			};
			$cv->end;
		};
	}
	$cv->end;
}

sub info {
	my $self = shift;
	my $args = shift;
	my $cb = shift;
	
	my ($query, $sth);
	my $overall_start = time();
	
	# Execute search on every peer
	my @peers;
	foreach my $peer (keys %{ $self->conf->get('peers') }){
		push @peers, $peer;
	}
	$self->log->trace('Executing global node_info on peers ' . join(', ', @peers));
	
	my %results;
	my %stats;
	my $cv = AnyEvent->condvar;
	$cv->begin(sub { 
		my $overall_final = $self->_merge_node_info(\%results);
		$stats{overall} = (time() - $overall_start);
		$self->log->debug('stats: ' . Dumper(\%stats));
		$cb->($overall_final);
	});
	
	foreach my $peer (@peers){
		$cv->begin;
		my $peer_conf = $self->conf->get('peers/' . $peer);
		my $url = $peer_conf->{url} . 'API/';
		$url .= ($peer eq '127.0.0.1' or $peer eq 'localhost') ? 'local_info' : 'info';
		$self->log->trace('Sending request to URL ' . $url);
		my $start = time();
		my $headers = { 
			Authorization => $self->_get_auth_header($peer),
		};
		$results{$peer} = http_get $url, headers => $headers, sub {
			my ($body, $hdr) = @_;
			eval {
				my $raw_results = $self->json->decode($body);
				$stats{$peer}->{total_request_time} = (time() - $start);
				$results{$peer} = { %$raw_results }; #undef's the guard
			};
			if ($@){
				$self->log->error($@ . "\nHeader: " . Dumper($hdr) . "\nbody: " . Dumper($body));
				$self->add_warning(502, 'peer ' . $peer . ': ' . $@, { http => $peer });
				delete $results{$peer};
			}
			$cv->end;
		};
	}
	$cv->end;
}

sub _merge_node_info {
	my ($self, $results) = @_;
	#$self->log->debug('merging: ' . Dumper($results));
	
	# Merge these results
	my $overall_final = merge values %$results;
	
	# Merge the times and counts
	my %final = (nodes => {});
	foreach my $peer (keys %$results){
		next unless $results->{$peer} and ref($results->{$peer}) eq 'HASH';
		if ($results->{$peer}->{nodes}){
			foreach my $node (keys %{ $results->{$peer}->{nodes} }){
				if ($node eq '127.0.0.1' or $node eq 'localhost'){
					$final{nodes}->{$peer} ||= $results->{$peer}->{nodes};
				}
				else {
					$final{nodes}->{$node} ||= $results->{$peer}->{nodes};
				}
			}
		}
		foreach my $key (qw(archive_min indexes_min)){
			if (not $final{$key} or $results->{$peer}->{$key} < $final{$key}){
				$final{$key} = $results->{$peer}->{$key};
			}
		}
		foreach my $key (qw(archive indexes)){
			$final{totals} ||= {};
			$final{totals}->{$key} += $results->{$peer}->{totals}->{$key};
		}
		foreach my $key (qw(archive_max indexes_max indexes_start_max archive_start_max)){
			if (not $final{$key} or $results->{$peer}->{$key} > $final{$key}){
				$final{$key} = $results->{$peer}->{$key};
			}
		}
	}
	$self->log->debug('final: ' . Dumper(\%final));
	foreach my $key (keys %final){
		$overall_final->{$key} = $final{$key};
	}
	
	return $overall_final;
}


__PACKAGE__->meta->make_immutable;