package API::Peers;
use Moose;
extends 'API';
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
use Ouch qw(:traditional);

use lib qw(../);
use Utils;

use Import;

sub local_query {
	my ($self, $args) = @_;
	
	my $q;
	if (ref($args) eq 'Query'){
		# We were given a query object natively
		$q = $args;
	}
	else {
		unless ($args and ref($args) eq 'HASH'){
			throw(400, 'Invalid query args', { query_string => 1 });
		}
		# Get our node info
		if (not $self->node_info->{updated_at} 
			or ($self->conf->get('node_info_cache_timeout') and 
				((time() - $self->node_info->{updated_at}) >= $self->conf->get('node_info_cache_timeout')))){
			$self->node_info($self->_get_node_info());
		}
		if ($args->{q}){
			if ($args->{qid}){
				$self->log->level($ERROR) unless $self->conf->get('debug_all');
				$q = new Query(conf => $self->conf, permissions => $args->{permissions}, q => $args->{q}, 
					node_info => $self->node_info, qid => $args->{qid}, peer_label => $args->{peer_label});
			}
			else {
				$q = new Query(conf => $self->conf, permissions => $args->{permissions}, q => $args->{q}, 
					node_info => $self->node_info, peer_label => $args->{peer_label});
			}
		}
		elsif ($args->{query_string}){
			$q = new Query(
				conf => $self->conf, 
				node_info => $self->node_info,
				%$args,
			);
		}
		else {
			delete $args->{user};
			$self->log->error('Bad args: ' . Dumper($args));
			throw(400, 'Invalid query args, no q or query_string', { query_string => 1 });
		}
	}
	
#	foreach my $warning (@{ $q->warnings }){
#		push @{ $self->warnings }, $warning;
#	}
	
	Log::Log4perl::MDC->put('qid', $q->qid);

	my ($query, $sth);
	
	# This is local, make sure we're querying localhost (in case we referred to by peer_name)
	$q->nodes->{given}->{'127.0.0.1'} = 1;
	
	# Check for batching
	unless ($q->meta_params->{nobatch} or $q->system or $q->livetail){
		my $is_batch = 0;
		if ($q->analytics or $q->archive){
			# Find estimated query time
			my $estimated_query_time = $self->_estimate_query_time($q);
			$self->log->trace('Found estimated query time ' . $estimated_query_time . ' seconds.');
			my $query_time_batch_threshold = 120;
			if ($self->conf->get('query_time_batch_threshold')){
				$query_time_batch_threshold = $self->conf->get('query_time_batch_threshold');
			}
			if ($estimated_query_time > $query_time_batch_threshold){
				$is_batch = 'Batching because estimated query time is ' . int($estimated_query_time) . ' seconds.';
				$self->log->info($is_batch);
			}
		}
		
		# Batch if we're allowing a huge number of results
		if (not $q->has_groupby and ($q->limit == 0 or $q->limit > $Results::Unbatched_results_limit)){
			$is_batch = q{Batching because an unlimited number or large number of results has been requested.};
			$self->log->info($is_batch);
		}	
			
		if ($is_batch){
			# Check to see if this user is already running an archive query
			$query = 'SELECT qid, uid FROM query_log WHERE archive=1 AND (ISNULL(num_results) OR num_results=-1)';
			$sth = $self->db->prepare($query);
			$sth->execute();
			my $counter = 0;
			while (my $row = $sth->fetchrow_hashref){
				if ($args->{user} and $row->{uid} eq $args->{user}->uid){
					$self->_error('User ' . $args->{user}->username . ' already has an archive query running: ' . $row->{qid});
					return;
				}
				$counter++;
				if ($counter >= $self->conf->get('max_concurrent_archive_queries')){
					#TODO create a queuing mechanism for this
					$self->_error('There are already ' . $counter . ' queries running');
					return;
				}
			}
			
			# Cron job will pickup the query from the query log and execute it from here if it's an archive query.
			$q->batch_message($is_batch . '  You will receive an email with your results.');
			$q->batch(1);
			return $q;
		}
	}
	
	# Execute search
	if ($q->has_import_search_terms){
		my $db = 'syslog';
		if ($self->conf->get('syslog_db_name')){
			$db = $self->conf->get('syslog_db_name');
		}
		my $start = time();
		
		# Handle dates specially
		my %date_terms;
		foreach my $term_hash ($q->all_import_search_terms){
			next unless $term_hash->{field} eq 'date';
			my $boolean = $term_hash->{boolean} eq '+' ? 'and' : $term_hash->{boolean} eq '-' ? 'not' : 'or';
			$date_terms{$boolean} ||= [];
			push @{ $date_terms{$boolean} }, $term_hash;
		}
		
		if (scalar keys %date_terms){
			$query = 'SELECT * from ' . $db . '.imports WHERE ';
			my @clauses;
			my @terms;
			my @values;
			foreach my $term_hash (@{ $date_terms{and} }){
				push @terms, 'imported ' . $term_hash->{op} . ' ?';
				push @values, $term_hash->{value};
			}
			if (@terms){
				push @clauses, '(' . join(' AND ', @terms) . ') ';
			}
			@terms = ();
			foreach my $term_hash (@{ $date_terms{or} }){
				push @terms, 'imported ' . $term_hash->{op} . ' ?';
				push @values, $term_hash->{value};
			}
			if (@terms){
				push @clauses, '(' . join(' OR ', @terms) . ') ';
			}
			@terms = ();
			foreach my $term_hash (@{ $date_terms{not} }){
				push @terms, 'NOT imported ' . $term_hash->{op} . ' ?';
				push @values, $term_hash->{value};
			}
			if (@terms){
				push @clauses, '(' . join(' AND ', @terms) . ') ';
			}
			$query .= join(' AND ', @clauses);
			
			$sth = $self->db->prepare($query);
			$sth->execute(@values);
			my $counter = 0;
			while (my $row = $sth->fetchrow_hashref){
				push @{ $q->id_ranges }, { boolean => 'and', values => [ $row->{first_id}, $row->{last_id} ] };
				$counter++;
			}
			unless ($counter){
				$self->log->trace('No matching imports found for dates given');
				return $q;
			}
		}
		
		# Handle name/description
		foreach my $term_hash ($q->all_import_search_terms){
			next if $term_hash->{field} eq 'date';
			my @values;
			if ($term_hash->{field} eq 'id'){
				$query = 'SELECT * from ' . $db . '.imports WHERE id ' . $term_hash->{op} . ' ?';
				@values = ($term_hash->{value});
			}
			else {
				$query = 'SELECT * from ' . $db . '.imports WHERE ' . $self->_build_sql_regex_term($term_hash);
				@values = ($term_hash->{value}, $term_hash->{value}, $term_hash->{value});
			}
			$self->log->trace('import search query: ' . $query);
			$self->log->trace('import search values: ' . Dumper(\@values));
			$sth = $self->db->prepare($query);
			$sth->execute(@values);
			my $counter = 0;
			while (my $row = $sth->fetchrow_hashref){
				push @{ $q->id_ranges }, { boolean => ($term_hash->{boolean} eq '+' ? 'and' : $term_hash->{boolean} eq '-' ? 'not' : 'or'), 
					values => [ $row->{first_id}, $row->{last_id} ] };
				$counter++;
			}
			if ($term_hash->{op} eq '+' and not $counter){
				$self->log->trace('No matching imports found for ' . $term_hash->{field} . ':' . $term_hash->{value});
				return $q;
			}
		}
		my $taken = time() - $start;
		$q->stats->{import_range_search} = $taken;
	}
	
	if ($q->has_import_search_terms and not $q->index_term_count){
		# Request id's based solely on the import
		$self->_get_ids($q);
	}
	elsif (not $q->datasources->{sphinx}){
		$self->_external_query($q);
	}
	elsif ($q->livetail){
		$self->_livetail_query($q);
	}
	elsif ($q->archive){
		$self->_archive_query($q);
	}
	elsif (not $q->index_term_count){
		# Skip Sphinx, execute a raw SQL search
		$self->log->info('No query terms, executing against raw SQL');
		$q->add_warning(200, 'No query terms, query did not use an index', { indexed => 0 }); 
		$self->_archive_query($q);
	}
	elsif (($q->analytics or ($q->limit > $API::Max_limit)) and not $q->has_groupby){
		$self->_unlimited_sphinx_query($q);
	}
	elsif ($q->has_stopword_terms){
		my @terms = (keys %{ $q->terms->{any_field_terms_sql}->{and} }, keys %{ $q->terms->{any_field_terms_sql}->{not} });
		$q->add_warning(200, 'Some query terms (' . join(', ', @terms) . ') were too common and required post-search filtering', { indexed => 0 });
		$self->_unlimited_sphinx_query($q);
	}
	else {
		$self->_sphinx_query($q);
	}
	
	foreach my $warning ($self->all_warnings){
		push @{ $q->warnings }, $warning;
	}
	
	$self->log->info(sprintf("Query " . $q->qid . " returned %d rows", $q->results->records_returned));
	
	$q->time_taken(int((Time::HiRes::time() - $q->start_time) * 1000)) unless $q->livetail;

	# Apply transforms
	if ($q->has_transforms){	
		$self->transform($q);
	}
	
#	# Send to connectors
#	if ($q->has_connectors){
#		$self->send_to($q);
#	}

	return $q;
}

sub query {
	my ($self, $args) = @_;
	
	my $q;
	if (ref($args) eq 'Query'){
		# We were given a query object natively
		$q = $args;
	}
	else {
		unless ($args and ref($args) eq 'HASH'){
			throw(400, 'Invalid query args', { query_string => 1 });
		}
		# Get our node info
		if (not $self->node_info->{updated_at} 
			or ($self->conf->get('node_info_cache_timeout') and 
				((time() - $self->node_info->{updated_at}) >= $self->conf->get('node_info_cache_timeout')))){
			$self->node_info($self->_get_node_info());
		}
		if ($args->{q}){
			$q = new Query(conf => $self->conf, permissions => $args->{permissions}, q => $args->{q}, 
				node_info => $self->node_info, peer_label => $args->{peer_label}, from_peer => $args->{from_peer});
		}
		elsif ($args->{query_string}){
			$q = new Query(
				conf => $self->conf, 
				node_info => $self->node_info,
				%$args,
			);
		}
		else {
			delete $args->{user};
			$self->log->error('Bad args: ' . Dumper($args));
			throw(400, 'Invalid query args, no q or query_string', { query_string => 1 });
		}
	}
	
	if ($args->{explain}){
		return {
			terms => $q->terms,
			build => $self->_build_query($q),
			highlights => $q->highlights,
		};
	}		
	
	Log::Log4perl::MDC->put('qid', $q->qid);
	
#	foreach my $warning (@{ $q->warnings }){
#		push @{ $self->warnings }, $warning;
#	}
	
	$q = $self->_peer_query($q);
	
	# Send to connectors
	if ($q->has_connectors){
		$self->send_to($q);
	}
	
	$q->dedupe_warnings();
	
	return $q;
}

sub local_info {
	my ($self, $args) = @_;
	
	return $self->_get_node_info(1);
}

sub local_stats {
	my ($self, $args) = @_;
	
	return $self->get_stats($args);
}

sub stats {
	my ($self, $args) = @_;
	
	my ($query, $sth);
	my $overall_start = time();
	
	# Execute search on every peer
	my @peers;
	foreach my $peer (keys %{ $self->conf->get('peers') }){
		push @peers, $peer unless $peer eq $args->{from_peer};
	}
	$self->log->trace('Executing global node_info on peers ' . join(', ', @peers));
	
	my $cv = AnyEvent->condvar;
	$cv->begin;
	my %stats;
	my %results;
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
			};
			if (my $e = catch_any){
				$self->log->error($e->message . "\nHeader: " . Dumper($hdr) . "\nbody: " . Dumper($body));
				$self->add_warning(502, 'peer ' . $peer . ': ' . $e->message, { http => $peer });
				delete $results{$peer};
			}
			$cv->end;
		};
	}
	$cv->end;
	$cv->recv;
	$stats{overall} = (time() - $overall_start);
	$self->log->debug('stats: ' . Dumper(\%stats));
	
	$self->log->debug('merging: ' . Dumper(\%results));
	my $overall_final = merge values %results;
	
	return $overall_final;
}

sub upload {
	my ($self, $args) = @_;
	
	$self->log->info('Received file ' . $args->{upload}->basename . ' with size ' . $args->{upload}->size 
		. ' from client ' . $args->{client_ip_address});
	my ($query, $sth);
	
	my $syslog_db_name = 'syslog';
	if ($self->conf->get('syslog_db_name')){
		$syslog_db_name = $self->conf->get('syslog_db_name');
	}
	
	# See if this is a Zip file
	open(FH, $args->{upload}->path) or throw(500, 'Unable to read file ' . $args->{upload}->path . ': ' . $!, { file => $args->{upload}->path });
	my $buf;
	read(FH, $buf, 2);
	my $is_zipped = 0;
	if ($buf eq 'PK'){
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
				$query = 'LOAD DATA LOCAL INFILE "' . $zipped_file . '" INTO TABLE ' . $syslog_db_name . '.programs';
				$self->db->do($query);
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
		move($file, $destfile) or throw(500, $!, { file => $file });
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
	
	my $ret = { ok => 1 };
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
		
	return $ret;
}

sub local_result {
	my ($self, $args) = @_;
	
	return $self->get_saved_result($args);
}

sub result {
	my ($self, $args) = @_;
	
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
	
	my $cv = AnyEvent->condvar;
	$cv->begin;
	my %stats;
	my %results;
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
			};
			if (my $e = catch_any){
				$self->log->error($e->message . "\nHeader: " . Dumper($hdr) . "\nbody: " . Dumper($body));
				$self->add_warning(502, 'peer ' . $peer . ': ' . $e->message, { peer => $peer });
				delete $results{$peer};
			}
			$cv->end;
		};
	}
	$cv->end;
	$cv->recv;
	$stats{overall} = (time() - $overall_start);
	$self->log->debug('stats: ' . Dumper(\%stats));
	
	$self->log->debug('merging: ' . Dumper(\%results));
	my $overall_final = merge values %results;
	
	return $overall_final;
}

__PACKAGE__->meta->make_immutable;