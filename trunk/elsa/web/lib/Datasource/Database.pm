package Datasource::Database;
use Moose;
use Moose::Meta::Class;
use Data::Dumper;
use CHI;
use DBI;
use JSON;
use URL::Encode qw(url_encode);
use Time::HiRes;
use Search::QueryParser::SQL;
use Date::Manip;
extends 'Datasource';
with 'Fields';

our $Name = 'Database';
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);
has 'cache' => (is => 'rw', isa => 'Object', required => 1);
has 'dsn' => (is => 'rw', isa => 'Str', required => 1);
has 'username' => (is => 'rw', isa => 'Str', required => 1);
has 'password' => (is => 'rw', isa => 'Str', required => 1);
has 'query_template' => (is => 'rw', isa => 'Str', required => 1);
has 'fields' => (is => 'rw', isa => 'ArrayRef', required => 1);
has 'parser' => (is => 'rw', isa => 'Object');
has 'db' => (is => 'rw', isa => 'Object');
has 'timestamp_column' => (is => 'rw', isa => 'Str');

sub BUILD {
	my $self = shift;
	
	$self->db(DBI->connect($self->dsn, $self->username, $self->password, { RaiseError => 1 }));
	my ($query, $sth);

	if ($self->dsn =~ /mysql/){
		$self->query_template =~ /FROM\s+([\w\_]+)/;
#		my $table = $1;
#		$self->dsn =~ /database=([\w\_]+)/;
#		my $database = $1;
#		$query = 'SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema=? AND table_name=?';
#		$sth = $self->db->prepare($query);
#		$sth->execute($database, $table);
		my %cols;
		
#		while (my $row = $sth->fetchrow_hashref){
#			next if $Fields::Reserved_fields->{ lc($row->{column_name}) };
#			next unless grep @{ $self->fields }, $row->{column_name};
#			if ($row->{data_type} =~ /char/){
#				$cols{ $row->{column_name} } = { name => $row->{column_name}, type => $row->{data_type}, fuzzy_op => 'LIKE', fuzzy_not_op => 'NOT LIKE' };
#			}
#			else {
#				$cols{ $row->{column_name} } = { name => $row->{column_name}, type => $row->{data_type} };
#			}
#		}
		
		foreach my $row (@{ $self->fields }){
			if (not $row->{type}){
				$row->{fuzzy_op} = 'LIKE';
				$row->{fuzzy_not_op} = 'NOT LIKE';
			}
			else {
				$row->{fuzzy_not_op} = '<=';
			}
			my $col_name = $row->{name};
			if ($row->{alias}){
				if ($row->{alias} eq 'timestamp'){
					$self->timestamp_column($row->{name});
				}
				#$row->{name} .= ' AS ' . $row->{alias};
				$col_name = $row->{alias};
			}
			$cols{$col_name} = $row;
		}
				
		foreach my $field (keys %$Fields::Reserved_fields){
		 	$cols{$field} = { name => $field, callback => sub { '1=1' } };
		}
		
		$self->log->debug('cols ' . Dumper(\%cols));
		$self->parser(Search::QueryParser::SQL->new(columns => \%cols, fuzzify2 => 1));
	} 
	else {
		$self->parser(Search::QueryParser::SQL->new(columns => $self->fields, fuzzify2 => 1, like => 'LIKE'));
	}
	
	return $self;
}

sub _query {
	my $self = shift;
	my $q = shift;
	
	my ($query, $sth);
	
	my $query_string = $q->query_string;
	$query_string =~ s/\|.*//;
	
	my ($where, $placeholders) = @{ $self->parser->parse($query_string)->dbi };
	$where =~ s/(?:(?:AND|OR|NOT)\s*)?1=1\s*(?:AND|OR|NOT)?//g; # clean up dummy values
	$self->log->debug('where: ' . Dumper($where));
	
	my @select;
	my $groupby = '';
	my $time_select_conversions = {
		year => 'CAST(UNIX_TIMESTAMP(' . $self->timestamp_column . ')/86400*365 AS unsigned)',
		month => 'CAST(UNIX_TIMESTAMP(' . $self->timestamp_column . ')/86400*30 AS unsigned)',
		week => 'CAST(UNIX_TIMESTAMP(' . $self->timestamp_column . ')/86400*7 AS unsigned)',
		day => 'CAST(UNIX_TIMESTAMP(' . $self->timestamp_column . ')/86400 AS unsigned)',
		hour => 'CAST(UNIX_TIMESTAMP(' . $self->timestamp_column . ')/3600 AS unsigned)',
		minute => 'CAST(UNIX_TIMESTAMP(' . $self->timestamp_column . ')/60 AS unsigned)',
		seconds => 'UNIX_TIMESTAMP(' . $self->timestamp_column . ')',
	};
	
	if ($q->has_groupby){
		# Check to see if there is a numeric count field
		my $count_field;
		foreach my $field (@{ $self->fields }){
			if ($field->{alias} and $field->{alias} eq 'count'){
				$count_field = $field->{name};
			}
		}
		
		if ($time_select_conversions->{ $q->groupby->[0] }){
			if ($count_field){
				push @select, 'SUM(' . $count_field . ') AS `@count`', $time_select_conversions->{ $q->groupby->[0] } . ' AS `@groupby`';
			}
			else {
				push @select, 'COUNT(*) AS `@count`', $time_select_conversions->{ $q->groupby->[0] } . ' AS `@groupby`';
			}
			$groupby = 'GROUP BY `@groupby`';
		}
		elsif ($q->groupby->[0] eq 'node'){
			#TODO Need to break this query into subqueries if grouped by node
			die('not supported');
		}
		else {
			foreach my $field (@{ $self->fields }){
				if ($field->{alias} eq $q->groupby->[0] or $field->{name} eq $q->groupby->[0]){
					if ($count_field){
						push @select, 'SUM(' . $count_field . ') AS `@count`', $field->{name} . ' AS `@groupby`';
					}
					else {
						push @select, 'COUNT(*) AS `@count`', $field->{name} . ' AS `@groupby`';
					}
					$groupby = 'GROUP BY ' . join(',', @{ $q->groupby });
					last;
				}
			}
		}	
		unless ($groupby){
			die('Invalid groupby ' . $groupby);
		}
	}
	
	my $orderby;
	foreach my $row (@{ $self->fields }){
		if ($row->{alias}){
			push @select, $row->{name} . ' AS ' . $row->{alias};
			if ($row->{alias} eq 'timestamp'){
				if ($where and $where ne ' '){
					$where = '(' . $where . ') AND ' . $row->{name} . '>=? AND ' . $row->{name} . '<=? ';
				}
				else {
					$where = $row->{name} . '>=? AND ' . $row->{name} . '<=? ';
				}
				push @$placeholders, epoch2iso($q->start), epoch2iso($q->end);				
				$orderby = $row->{alias} ? $row->{alias} : $row->{name};
			}
		}
		else {
			push @select, $row->{name};
		}
	}
	
	$query = sprintf($self->query_template, join(', ', @select), $where, $groupby, $orderby, $q->offset, $q->limit);
	$self->log->debug('query: ' . $query);
	$self->log->debug('placeholders: ' . Dumper($placeholders));
	$sth = $self->db->prepare($query);
	$sth->execute(@$placeholders);
	
	my $overall_start = time();
	my @rows;
	while (my $row = $sth->fetchrow_hashref){
		$self->log->debug('row: ' . Dumper($row));
		push @rows, $row;
	}
	if ($q->has_groupby){
		my %results;
		my $total_records = 0;
		my $records_returned = 0;
		my @tmp;
		foreach my $groupby ($q->all_groupbys){
			if (exists $Fields::Time_values->{ $groupby }){
				# Sort these in ascending label order
				my $increment = $Fields::Time_values->{ $groupby };
				my %agg; 
				foreach my $row (@rows){
					#my $unixtime = UnixDate($row->{'@groupby'}, '%s');
					my $unixtime = $row->{'@groupby'};
					#my $value = $unixtime - ($unixtime % $increment);
					my $value = $unixtime * $increment;
										
					$self->log->trace('$value: ' . epoch2iso($value) . ', increment: ' . $increment . 
						', unixtime: ' . $unixtime . ', localtime: ' . (scalar localtime($value)));
					$row->{intval} = $value;
					$agg{ $row->{intval} } += $row->{'@count'};
				}
				
				foreach my $key (sort { $a <=> $b } keys %agg){
					push @tmp, { 
						intval => $key, 
						'@groupby' => epoch2iso($key), 
						'@count' => $agg{$key}
					};
				}	
				
				# Fill in zeroes for missing data so the graph looks right
				my @zero_filled;
				
				$self->log->trace('using increment ' . $increment . ' for time value ' . $groupby);
				OUTER: for (my $i = 0; $i < @tmp; $i++){
					push @zero_filled, $tmp[$i];
					if (exists $tmp[$i+1]){
						for (my $j = $tmp[$i]->{intval} + $increment; $j < $tmp[$i+1]->{intval}; $j += $increment){
							#$self->log->trace('i: ' . $tmp[$i]->{intval} . ', j: ' . ($tmp[$i]->{intval} + $increment) . ', next: ' . $tmp[$i+1]->{intval});
							push @zero_filled, { 
								'@groupby' => epoch2iso($j),
								intval => $j,
								'@count' => 0
							};
							last OUTER if scalar @zero_filled > $q->limit;
						}
					}
				}
				$results{$groupby} = [ @zero_filled ];
			}
			elsif (UnixDate($rows[0]->{'@groupby'}, '%s')){
				# Sort these in ascending label order
				my $increment = 86400 * 30;
				my %agg; 
				foreach my $row (@rows){
					my $unixtime = UnixDate($row->{'@groupby'}, '%s');
					my $value = $unixtime - ($unixtime % $increment);
										
					$self->log->trace('key: ' . epoch2iso($value) . ', tv: ' . $increment . 
						', unixtime: ' . $unixtime . ', localtime: ' . (scalar localtime($value)));
					$row->{intval} = $value;
					$agg{ $row->{intval} } += $row->{'@count'};
				}
				
				foreach my $key (sort { $a <=> $b } keys %agg){
					push @tmp, { 
						intval => $key, 
						'@groupby' => epoch2iso($key), #$self->resolve_value(0, $key, $groupby), 
						'@count' => $agg{$key}
					};
				}	
				
				# Fill in zeroes for missing data so the graph looks right
				my @zero_filled;
				
				$self->log->trace('using increment ' . $increment . ' for time value ' . $groupby);
				OUTER: for (my $i = 0; $i < @tmp; $i++){
					push @zero_filled, $tmp[$i];
					if (exists $tmp[$i+1]){
						$self->log->debug('$tmp[$i]->{intval} ' . $tmp[$i]->{intval});
						$self->log->debug('$tmp[$i+1]->{intval} ' . $tmp[$i+1]->{intval});
						for (my $j = $tmp[$i]->{intval} + $increment; $j < $tmp[$i+1]->{intval}; $j += $increment){
							$self->log->trace('i: ' . $tmp[$i]->{intval} . ', j: ' . ($tmp[$i]->{intval} + $increment) . ', next: ' . $tmp[$i+1]->{intval});
							push @zero_filled, { 
								'@groupby' => epoch2iso($j),
								intval => $j,
								'@count' => 0
							};
							last OUTER if scalar @zero_filled > $q->limit;
						}
					}
				}
				$results{$groupby} = [ @zero_filled ];
			}
			else { 
				# Sort these in descending value order
				foreach my $row (sort { $b->{'@count'} <=> $a->{'@count'} } @rows){
					$total_records += $row->{'@count'};
					$row->{intval} = $row->{'@count'};
					push @tmp, $row;
					last if scalar @tmp > $q->limit;
				}
				$results{$groupby} = [ @tmp ];
			}
			$records_returned += scalar @tmp;
		}
		if (ref($q->results) eq 'Results::Groupby'){
			$q->results->add_results(\%results);
		}
		else {
			$q->results(Results::Groupby->new(conf => $self->conf, results => \%results, total_records => $total_records));
		}
	}
	else {
		foreach my $row (@rows){
			my $ret = { timestamp => $row->{timestamp}, class => 'NONE', host => '0.0.0.0', 'program' => 'NA' };
			$ret->{_fields} = [
				{ field => 'host', value => '0.0.0.0', class => 'any' },
				{ field => 'program', value => 'NA', class => 'any' },
				{ field => 'class', value => 'NONE', class => 'any' },
			];
			my @msg;
			foreach my $key (sort keys %$row){
				push @msg, $key . '=' . $row->{$key};
				push @{ $ret->{_fields} }, { field => $key, value => $row->{$key}, class => 'NONE' };
			}
			$ret->{msg} = join(' ', @msg);
			$q->results->add_result($ret);
			last if scalar $q->results->total_records >= $q->limit;
		}
	}
	
	
#	
#	my $ret = {};
#	my %queries; # per-node hash
#	foreach my $node (keys %{ $q->node_info->{nodes} }){
#		$ret->{$node} = { rows => [] };
#		$queries{$node} = [];
#		my $node_info = $q->node_info->{nodes}->{$node};
#		# Prune tables
#		my @table_arr;
#		foreach my $table (@{ $node_info->{tables}->{tables} }){
#			if ($q->start and $q->end){
#				if ($table->{table_type} eq 'archive' and
#					(($q->start >= $table->{start_int} and $q->start <= $table->{end_int})
#					or ($q->end >= $table->{start_int} and $q->end <= $table->{end_int})
#					or ($q->start <= $table->{start_int} and $q->end >= $table->{end_int})
#					or ($table->{start_int} <= $q->start and $table->{end_int} >= $q->end))
#				){
#					push @table_arr, $table->{table_name};
#				}
#			}
#			else {
#				push @table_arr, $table->{table_name};
#			}
#		}	
#		unless (@table_arr){
#			$self->log->debug('no tables for node ' . $node);
#			next;
#		}
#		
#		my $time_select_conversions = {
#			day => 'CAST(timestamp/86400 AS unsigned) AS day',
#			hour => 'CAST(timestamp/3600 AS unsigned) AS hour',
#			minute => 'CAST(timestamp/60 AS unsigned) AS minute',
#		};
#		
#		my $queries = $self->_build_query($q);
#		foreach my $table (@table_arr){
#			my $start = time();
#			foreach my $query (@$queries){
#				# strip sphinx-specific attr_ prefix
#				$query->{where} =~ s/attr\_((?:i|s)\d)([<>=]{1,2})\?/$1$2\?/g; 
#				my $search_query;
#				if ($query->{groupby}){
#					$query->{groupby} =~ s/attr\_((?:i|s)\d)/$1/g;
#					if ($time_select_conversions->{ $query->{groupby_field} }){
#						my $groupby = $time_select_conversions->{ $query->{groupby_field} };
#						$search_query = "SELECT COUNT(*) AS count, class_id, $groupby\n";
#					}
#					else {
#						$search_query = "SELECT COUNT(*) AS count, class_id, $query->{groupby} AS \"$query->{groupby_field}\"\n";
#					}
#					$search_query .= "FROM $table main\n" .
#						'WHERE ' . $query->{where} . "\nGROUP BY $query->{groupby}\n" . 'ORDER BY 1 DESC LIMIT ?,?';
#				}
#				else {
#					$search_query = "SELECT main.id,\n" .
#						"\"" . $node . "\" AS node,\n" .
#						"DATE_FORMAT(FROM_UNIXTIME(timestamp), \"%Y/%m/%d %H:%i:%s\") AS timestamp,\n" .
#						"INET_NTOA(host_id) AS host, program, class_id, class, msg,\n" .
#						"i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5\n" .
#						"FROM $table main\n" .
#						"LEFT JOIN " . $node_info->{db} . ".programs ON main.program_id=programs.id\n" .
#						"LEFT JOIN " . $node_info->{db} . ".classes ON main.class_id=classes.id\n" .
#						'WHERE ' . $query->{where} . "\n" . 'LIMIT ?,?';
#				}
#				#$self->log->debug('archive_query: ' . $search_query . ', values: ' . 
#				#	Dumper($query->{values}, $args->{offset}, $args->{limit}));
#				push @{ $queries{$node} }, 
#					{ query => $search_query, values => [ @{ $query->{values} }, $q->offset, $limit ] };
#			}
#		}
#	}
#	my $total_found = 0;
#	my ($query, $sth);
#	my $queries_todo_count = 0;
#	foreach my $node (keys %queries){
#		$queries_todo_count += scalar @{ $queries{$node} };
#	}
#	
#	QUERY_LOOP: while ($queries_todo_count){
#		last unless scalar keys %queries;
#		my $cv = AnyEvent->condvar;
#		$cv->begin(sub {
#			$cv->send;
#		});
#		
#		foreach my $node (keys %queries){
#			my $query_hash = shift @{ $queries{$node} };
#			next unless $query_hash;
#			
#			# Check if the query was cancelled
#			return if $q->check_cancelled;
#			
#			eval {
#				my $start = time();
#				foreach my $key (keys %$query_hash){
#					$self->log->debug('node: ' . $node . ', key: ' . $key . ', val: ' . Dumper($query_hash->{$key}));
#				}
#				$self->log->debug('running query ' . $query_hash->{query});
#				$self->log->debug(' with values ' . join(',', @{ $query_hash->{values} }));
#				$cv->begin;
#				$q->node_info->{nodes}->{$node}->{dbh}->query($query_hash->{query}, sub { 
#						$self->log->debug('Archive query for node ' . $node . ' finished in ' . (time() - $start));
#						my ($dbh, $rows, $rv) = @_;
#						$self->log->trace('node ' . $node . ' got archive result: ' . Dumper($rows));
#						if (not $rv){
#							my $e = 'node ' . $node . ' got error ' . $rows;
#							$self->log->error($e);
#							$self->add_warning($e);
#							$cv->end;
#							next;
#						}
#						push @{ $ret->{$node}->{rows} }, @$rows;
#						$cv->end; #end archive query
#					},
#					@{ $query_hash->{values} });
#			};
#			if ($@){
#				$ret->{$node}->{error} = 'sphinx query error: ' . $@;
#				$self->log->error('sphinx query error: ' . $@);
#				$cv->end;
#			}
#
#		}
#		$cv->end; # bookend initial begin
#		$cv->recv; # block until all of the above completes
#		
#		# See how many we have left to do in case we're done
#		$queries_todo_count = 0;
#		foreach my $node (keys %queries){
#			$queries_todo_count += scalar @{ $queries{$node} };
#		}
#	}
#	
#	#my $total_records = 0;
#	if ($q->has_groupby){
#		foreach my $groupby ($q->all_groupbys){
#			my %agg;
#			foreach my $node (sort keys %$ret){
#				# One-off for grouping by node
#				if ($groupby eq 'node'){
#					$agg{$node} = scalar @{ $ret->{$node}->{rows} };
#					next;
#				}
#				
#				foreach my $row (@{ $ret->{$node}->{rows} }){
#					my $field_infos = $q->resolve($groupby, $row->{$groupby}, '=');
#					my $attr = (keys %{ $field_infos->{attrs}->{ $row->{class_id} } })[0];
#					my $key;
#					if ($attr){
#						$attr =~ s/attr\_//;
#						
#						if (exists $Fields::Time_values->{ $groupby }){
#							# We will resolve later
#							$key = (values %{ $field_infos->{attrs}->{0} })[0];
#						}
#						elsif (exists $Fields::Field_to_order->{ $attr }){
#							# Resolve normally
#							$key = $self->resolve_value($row->{class_id}, 
#								$row->{$groupby}, $attr);
#						}
#					}
#					else {
#						my $field_order = $self->get_field($groupby)->{ $row->{class_id} }->{field_order};
#						$key = $self->resolve_value($row->{class_id}, $row->{$groupby}, $Fields::Field_order_to_field->{$field_order});
#					}
#										
#					$agg{ $key } += $row->{count};	
#				}
#			}
#			$self->log->trace('got agg ' . Dumper(\%agg) . ' for groupby ' . $groupby);
#			if (exists $Fields::Time_values->{ $groupby }){
#				# Sort these in ascending label order
#				my @tmp;
#				foreach my $key (sort { $a <=> $b } keys %agg){
#					my $unixtime = ($key * $Fields::Time_values->{ $groupby });
#					push @tmp, { 
#						intval => $unixtime, 
#						'@groupby' => $self->resolve_value(0, $key, $groupby), 
#						'@count' => $agg{$key}
#					};
#				}
#				
#				# Fill in zeroes for missing data so the graph looks right
#				my @zero_filled;
#				my $increment = $Fields::Time_values->{ $groupby };
#				$self->log->trace('using increment ' . $increment . ' for time value ' . $groupby);
#				OUTER: for (my $i = 0; $i < @tmp; $i++){
#					push @zero_filled, $tmp[$i];
#					if (exists $tmp[$i+1]){
#						for (my $j = $tmp[$i]->{intval} + $increment; $j < $tmp[$i+1]->{intval}; $j += $increment){
#							$self->log->trace('i: ' . $tmp[$i]->{intval} . ', j: ' . ($tmp[$i]->{intval} + $increment) . ', next: ' . $tmp[$i+1]->{intval});
#							push @zero_filled, { 
#								'@groupby' => epoch2iso($j), 
#								intval => $j,
#								'@count' => 0
#							};
#							last OUTER if scalar @zero_filled > $limit;
#						}
#					}
#				}
#				foreach (@zero_filled){
#					$q->results->add_result($groupby, $_);
#				}
#			}
#			else { 
#				# Sort these in descending value order
#				my @tmp;
#				foreach my $key (sort { $agg{$b} <=> $agg{$a} } keys %agg){
#					push @tmp, { intval => $agg{$key}, '@groupby' => $key, '@count' => $agg{$key} };
#					last if scalar @tmp >= $limit;
#				}
#				foreach (@tmp){
#					$q->results->add_result($groupby, $_);
#				}
#				#$self->log->debug('archive groupby results: ' . Dumper($q->results));
#			}
#		}
#	}
#	else {
#		my @tmp; # we need to sort chronologically
#		NODE_LOOP: foreach my $node (keys %$ret){
#			#$total_records += scalar @{ $ret->{$node}->{rows} };
#			foreach my $row (@{ $ret->{$node}->{rows} }){
#				$row->{_fields} = [
#						{ field => 'host', value => $row->{host}, class => 'any' },
#						{ field => 'program', value => $row->{program}, class => 'any' },
#						{ field => 'class', value => $row->{class}, class => 'any' },
#					];
#				# Resolve column names for fields
#				foreach my $col qw(i0 i1 i2 i3 i4 i5 s0 s1 s2 s3 s4 s5){
#					my $value = delete $row->{$col};
#					# Swap the generic name with the specific field name for this class
#					my $field = $self->node_info->{fields_by_order}->{ $row->{class_id} }->{ $Fields::Field_to_order->{$col} }->{value};
#					if (defined $value and $field){
#						# See if we need to apply a conversion
#						$value = $self->resolve_value($row->{class_id}, $value, $col);
#						push @{ $row->{_fields} }, { 'field' => $field, 'value' => $value, 'class' => $self->node_info->{classes_by_id}->{ $row->{class_id} } };
#					}
#				}
#				push @tmp, $row;
#			}
#		}
#		foreach my $row (sort { $a->{timestamp} cmp $b->{timestamp} } @tmp){
#			$q->results->add_result($row);
#			last if scalar $q->results->total_records >= $limit;
#		}
#	}
	
	$q->time_taken(time() - $overall_start);
	
	$self->log->debug('completed query in ' . $q->time_taken . ' with ' . $q->results->total_records . ' rows');
	$self->log->debug('results: ' . Dumper($q->results));
	
	return 1;
}

 
1;
