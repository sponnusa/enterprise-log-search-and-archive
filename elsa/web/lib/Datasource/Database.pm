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
				push @select, 'SUM(' . $count_field . ') AS `_count`', $time_select_conversions->{ $q->groupby->[0] } . ' AS `_groupby`';
			}
			else {
				push @select, 'COUNT(*) AS `_count`', $time_select_conversions->{ $q->groupby->[0] } . ' AS `_groupby`';
			}
			$groupby = 'GROUP BY _groupby';
		}
		elsif ($q->groupby->[0] eq 'node'){
			#TODO Need to break this query into subqueries if grouped by node
			die('not supported');
		}
		else {
			foreach my $field (@{ $self->fields }){
				if ($field->{alias} eq $q->groupby->[0] or $field->{name} eq $q->groupby->[0]){
					if ($count_field){
						push @select, 'SUM(' . $count_field . ') AS _count', $field->{name} . ' AS _groupby';
					}
					else {
						push @select, 'COUNT(*) AS _count', $field->{name} . ' AS _groupby';
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
					my $unixtime = $row->{_groupby};
					my $value = $unixtime * $increment;
										
					$self->log->trace('$value: ' . epoch2iso($value) . ', increment: ' . $increment . 
						', unixtime: ' . $unixtime . ', localtime: ' . (scalar localtime($value)));
					$row->{intval} = $value;
					$agg{ $row->{intval} } += $row->{_count};
				}
				
				foreach my $key (sort { $a <=> $b } keys %agg){
					push @tmp, { 
						intval => $key, 
						_groupby => epoch2iso($key), 
						_count => $agg{$key}
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
								_groupby => epoch2iso($j),
								intval => $j,
								_count => 0
							};
							last OUTER if scalar @zero_filled > $q->limit;
						}
					}
				}
				$results{$groupby} = [ @zero_filled ];
			}
			elsif (UnixDate($rows[0]->{_groupby}, '%s')){
				# Sort these in ascending label order
				my $increment = 86400 * 30;
				my %agg; 
				foreach my $row (@rows){
					my $unixtime = UnixDate($row->{_groupby}, '%s');
					my $value = $unixtime - ($unixtime % $increment);
										
					$self->log->trace('key: ' . epoch2iso($value) . ', tv: ' . $increment . 
						', unixtime: ' . $unixtime . ', localtime: ' . (scalar localtime($value)));
					$row->{intval} = $value;
					$agg{ $row->{intval} } += $row->{_count};
				}
				
				foreach my $key (sort { $a <=> $b } keys %agg){
					push @tmp, { 
						intval => $key, 
						_groupby => epoch2iso($key), #$self->resolve_value(0, $key, $groupby), 
						_count => $agg{$key}
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
								_groupby => epoch2iso($j),
								intval => $j,
								_count => 0
							};
							last OUTER if scalar @zero_filled > $q->limit;
						}
					}
				}
				$results{$groupby} = [ @zero_filled ];
			}
			else { 
				# Sort these in descending value order
				foreach my $row (sort { $b->{_count} <=> $a->{_count} } @rows){
					$total_records += $row->{_count};
					$row->{intval} = $row->{_count};
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
			
	$q->time_taken(time() - $overall_start);
	
	$self->log->debug('completed query in ' . $q->time_taken . ' with ' . $q->results->total_records . ' rows');
	$self->log->debug('results: ' . Dumper($q->results));
	
	return 1;
}

 
1;
