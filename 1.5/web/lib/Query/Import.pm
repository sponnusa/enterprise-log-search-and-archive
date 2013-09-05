package Query::Import;
use Moose;
use Data::Dumper;
use AnyEvent;
use Try::Tiny;
use Ouch qw(:trytiny);
use Socket;
use String::CRC32;
use Sys::Hostname::FQDN;
use Net::DNS;
use Time::HiRes qw(time);

extends 'Query';

has 'data_db' => (is => 'rw', isa => 'HashRef');

sub execute {
	my $self = shift;
	my $cb = shift;
	
	my $start = time();
	my $counter = 0;
	my $total = 0;
	my $cv = AnyEvent->condvar;
	$cv->begin(sub {
		if (not $self->has_errors){
			$total and $self->results->percentage_complete(100 * $counter / $total);
			if ($self->results->total_records > $self->results->total_docs){
				$self->results->total_docs($self->results->total_records);
			}
		}
		$cb->();
	});
	
	my $timeout_watcher;
	if ($self->timeout){
		$timeout_watcher = AnyEvent->timer(after => ($self->timeout/1000), cb => sub {
			$self->add_warning(503, 'Query timed out', { timeout => $self->timeout });
			$cv->send;
			undef $timeout_watcher;
			return;
		});
	}	
	
	# Get the tables based on our search
	$self->_get_db(sub {
		$self->_find_import_ranges(sub {
			undef $timeout_watcher;
			$cv->end;
		});
	});
}

sub _get_db {
	my $self = shift;
	my $cb = shift;
	my $conf = $self->conf->get('data_db');
	
	my $start = time();
	my $db_name = 'syslog';
	if ($conf->{db}){
		$db_name = $conf->{db};
	}
	
	my $mysql_port = 3306;
	if ($conf->{port}){
		$mysql_port = $conf->{port};
	}
			
	my $ret = {};
	eval {
		$ret = { db => $db_name };
		$ret->{dbh} = SyncMysql->new(log => $self->log, db_args => [
			'dbi:mysql:database=' . $db_name . ';port=' . $mysql_port,  
			$conf->{username}, 
			$conf->{password}, 
			{
				mysql_connect_timeout => $self->db_timeout,
				PrintError => 0,
				mysql_multi_statements => 1,
			}
		]);
	};
	if ($@){
		$self->add_warning(502, $@, { mysql => $self->peer_label });
		$cb->(0);
	}		
	
	$self->log->trace('All connected in ' . (time() - $start) . ' seconds');
	$self->data_db($ret);
	
	$cb->(1);
}

sub _find_import_ranges {
	my $self = shift;
	my $cb = shift;
	
	my $start = time();
	my ($query, $sth);
	my @id_ranges;
		
	# Handle dates specially
	my %date_terms;
	foreach my $term_hash ($self->parser->all_import_search_terms){
		next unless $term_hash->{field} eq 'date';
		$date_terms{ $term_hash->{boolean} } ||= [];
		push @{ $date_terms{ $term_hash->{boolean} } }, $term_hash;
	}
	
	if (scalar keys %date_terms){
		$query = 'SELECT * from ' . $self->data_db->{db} . '.imports WHERE ';
		my @clauses;
		my @terms;
		my @values;
		foreach my $term_hash (@{ $date_terms{and} }){
			if ($term_hash->{value} =~ /^\d{4}\-\d{2}\-\d{2}$/){
				push @terms, 'DATE_FORMAT(imported, "%Y-%m-%d") ' . $term_hash->{op} . ' ?';
			}
			else {
				push @terms, 'imported ' . $term_hash->{op} . ' ?';
			}
			push @values, $term_hash->{value};
		}
		if (@terms){
			push @clauses, '(' . join(' AND ', @terms) . ') ';
		}
		@terms = ();
		foreach my $term_hash (@{ $date_terms{or} }){
			if ($term_hash->{value} =~ /^\d{4}\-\d{2}\-\d{2}$/){
				push @terms, 'DATE_FORMAT(imported, "%Y-%m-%d") ' . $term_hash->{op} . ' ?';
			}
			else {
				push @terms, 'imported ' . $term_hash->{op} . ' ?';
			}
			push @values, $term_hash->{value};
		}
		if (@terms){
			push @clauses, '(' . join(' OR ', @terms) . ') ';
		}
		@terms = ();
		foreach my $term_hash (@{ $date_terms{not} }){
			if ($term_hash->{value} =~ /^\d{4}\-\d{2}\-\d{2}$/){
				push @terms, 'NOT DATE_FORMAT(imported, "%Y-%m-%d") ' . $term_hash->{op} . ' ?';
			}
			else {
				push @terms, 'NOT imported ' . $term_hash->{op} . ' ?';
			}
			push @values, $term_hash->{value};
		}
		if (@terms){
			push @clauses, '(' . join(' AND ', @terms) . ') ';
		}
		$query .= join(' AND ', @clauses);
		
		$self->log->trace('import date search query: ' . $query);
		$self->log->trace('import date search values: ' . Dumper(\@values));
		$sth = $self->db->prepare($query);
		$sth->execute(@values);
		my $counter = 0;
		while (my $row = $sth->fetchrow_hashref){
			push @id_ranges, { boolean => 'and', values => [ $row->{first_id}, $row->{last_id} ], import_info => $row };
			$counter++;
		}
		unless ($counter){
			$self->log->trace('No matching imports found for dates given');
			$cb->([]);
			return;
		}
	}
	
	# Handle name/description
	foreach my $term_hash ($self->parser->all_import_search_terms){
		next if $term_hash->{field} eq 'date';
		my @values;
		if ($term_hash->{field} eq 'id'){
			$query = 'SELECT * from ' . $self->data_db->{db} . '.imports WHERE id ' . $term_hash->{op} . ' ?';
			@values = ($term_hash->{value});
		}
		else {
			$query = 'SELECT * from ' . $self->data_db->{db} . '.imports WHERE ' . lc($term_hash->{field}) . ' RLIKE ?';
			@values = ($self->_term_to_sql_term($term_hash->{value}, $term_hash->{field}));
		}
		$self->log->trace('import search query: ' . $query);
		$self->log->trace('import search values: ' . Dumper(\@values));
		$sth = $self->db->prepare($query);
		$sth->execute(@values);
		my $counter = 0;
		
		while (my $row = $sth->fetchrow_hashref){
			push @id_ranges, { boolean => ($term_hash->{boolean} eq '+' ? 'and' : $term_hash->{boolean} eq '-' ? 'not' : 'or'), 
				values => [ $row->{first_id}, $row->{last_id} ], import_info => $row };
			$counter++;
		}
		if ($term_hash->{op} eq '+' and not $counter){
			$self->log->trace('No matching imports found for ' . $term_hash->{field} . ':' . $term_hash->{value});
			$cb->([]);
			return;
		}
	}
	my $taken = time() - $start;
	$self->stats->{import_range_search} = $taken;
	$self->_get_rows([@id_ranges], $cb);
}

sub _get_rows {
	my $self = shift;
	my $ranges = shift;
	my $cb = shift;
	
	my $start = time();
	my $total_records = 0;
	my $counter = 0;
	my %tables;
	foreach my $range (@$ranges){
		$total_records += $range->{values}->[1] - $range->{values}->[0];
		next if $counter >= $self->limit;
		# Find what tables we need to query to resolve rows
		
		ROW_LOOP: for (my $id = $range->{values}->[0]; $id <= $range->{values}->[1]; $id++){
			foreach my $table_hash (@{ $self->meta_info->{tables}->{tables} }){
				last ROW_LOOP if $counter >= $self->limit;
				next unless $table_hash->{table_type} eq 'import';
				if ($table_hash->{min_id} <= $id and $id <= $table_hash->{max_id}){
					$tables{ $table_hash->{table_name} } ||= [];
					push @{ $tables{ $table_hash->{table_name} } }, $id;
					$counter++;
					next ROW_LOOP;
				}
			}
		}
	}
		
	if (not scalar keys %tables){
		$self->add_warning(500, 'Data not yet indexed, try again shortly.', { mysql => $self->peer_label });
		$self->log->error('No tables found for result. tables: ' . Dumper($self->meta_info->{tables}));
		$cb->();
		return;
	}			
		
	# Go get the actual rows from the dbh
	my @table_queries;
	my @table_query_values;
	foreach my $table (sort keys %tables){
		my $placeholders = join(',', map { '?' } @{ $tables{$table} });
		
		my $table_query = sprintf("SELECT id,\n" .
			"timestamp, INET_NTOA(host_id) AS host, program_id, class_id, msg,\n" .
			"i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5\n" .
			"FROM %1\$s\n" .
			'WHERE timestamp BETWEEN ? AND ? AND id IN (' . $placeholders . ') ', $table);
		push @table_queries, $table_query;
		push @table_query_values, $self->start, $self->end, @{ $tables{$table} };
	}
	
	if (not @table_queries){
		$self->add_warning(500, 'No tables found for import ids', { mysql => $self->peer_label });
		$self->log->error('No tables found for import ids: ' . Dumper(\%tables));
		$cb->();
		return;
	}

	my $table_query = join(' UNION ', @table_queries) . ' ' .
	($self->orderby ? 'ORDER BY ' . $self->orderby . ' ' . $self->orderby_dir . ' ' : ' ') .
	'LIMIT ?,?';
	push @table_query_values, $self->offset, $self->limit;
	$self->log->trace('table query: ' . $table_query 
		. ', placeholders: ' . join(',', @table_query_values));
	
	$self->data_db->{dbh}->query($table_query, @table_query_values, sub { 
		my ($dbh, $rows, $rv) = @_;
		if (not $rv or not ref($rows) or ref($rows) ne 'ARRAY'){
			my $errstr = 'got error ' . $rows;
			$self->log->error($errstr);
			$self->add_warning(502, $errstr, { mysql => $self->peer_label });
			$cb->();
			return;
		}
		elsif (not scalar @$rows){
			$self->log->error('Did not get rows though we had Sphinx results! tables: ' . Dumper(\%tables));
			$cb->();
			return; 
		}
		$self->log->trace('got db rows: ' . (scalar @$rows));
		
		my $results = {};
		foreach my $row (@$rows){
			$row->{node} = $self->peer_label ? $self->peer_label : '127.0.0.1';
			$row->{node_id} = unpack('N*', inet_aton($row->{node}));
			if ($self->orderby){
				$row->{_orderby} = $row->{ $self->orderby };
			}
			else {
				$row->{_orderby} = $row->{timestamp};
			}
			# Copy import info into the row
			foreach my $range (@$ranges){
				if ($range->{values}->[0] <= $row->{id} and $range->{values}->[1] <= $row->{id}){
					foreach my $import_col (@{ $Fields::Import_fields }){
						if ($range->{import_info}->{$import_col}){
							$row->{$import_col} = $range->{import_info}->{$import_col};
						}
					}
					last;
				}
			}
			$self->results->add_result($row);
		}
		$self->stats->{mysql_query} += (time() - $start);
		$cb->();
	});	
}	

1;