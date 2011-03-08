package ELSA::Search;
use strict;
use warnings;
use Data::Dumper;
use Time::HiRes qw(sleep time);
use Date::Manip;
use Digest::MD5 qw(md5_hex);
use Search::QueryParser;
use Search::QueryParser::SQL;
use Storable qw(dclone);
use Socket;
use Sphinx::Search;
use Sphinx::Config;
use String::CRC32;
use JSON;
use POE;
use POE::Event::Message;
use IO::Socket;
use Data::Serializer;
use Sort::Key qw(rukeysort);
use Net::DNS;
use Sys::Hostname::FQDN;


BEGIN {
	$POE::Event::Message::Filter = new POE::Filter::Reference( 
		Data::Serializer->new(
			serializer => 'YAML::Syck',
			portable => 1,
		)
	);
}

use base qw( ELSA );
use ELSA::Exceptions;

our $Default_limit = 100;
our $Max_limit = 1000;
our $Search_method = 'serial';
our $Implicit_plus = 0;
our $Max_batch_queries = 64;

sub new {
	my $class = shift;
	my $config_file_name = shift;
					
	my $self = $class->SUPER::new($config_file_name);
	bless ($self, $class);
	$self->init_db();
	
	$self->{_DISTINCT_FIELD_CLASS_IDS} = {};
	$self->{_GIVEN_CLASSES} = {};
	$self->{_EXCLUDED_CLASSES} = {};
	$self->{_KNOWN_CLASSES} = $self->get_classes_by_name();
#	$self->log->debug("known classes: " . Dumper($self->{_KNOWN_CLASSES}));
	
	$self->{_ATTR_CONVERSIONS} = $self->get_attr_conversions();
#	$self->log->debug("ATTR_CONVERSIONS: " . Dumper($self->{_ATTR_CONVERSIONS}));
	$self->{_FIELD_CONVERSIONS} = $self->get_field_conversions();
	$self->{_RESULTS} = [];
	$self->{_WARNINGS} = [];
	
	# Find out what the max queries we can run in a single batch is
	my $sphinx_config = new Sphinx::Config();
	$sphinx_config->parse($self->conf->get('sphinx/config_file'));
	if ($sphinx_config->get('searchd')->{max_batch_queries} and $sphinx_config->get('searchd')->{max_batch_queries} < $Max_batch_queries){
 		$Max_batch_queries = $sphinx_config->get('searchd')->{max_batch_queries};
	}
	else {
		# Sphinx default
		$Max_batch_queries = 32;
	}
	
	return $self;
}

sub break {
	my $self = shift;
	my $qid = sprintf('%d', shift);
	my $set = shift;
	throw_e error => 'No query_cancel_dir set!' unless -d $self->conf->get('query_cancel_dir');
	
	if ($set){
		open(FH, '> ' . $self->conf->get('query_cancel_dir') . '/' . $qid);
		print FH 1;
		close(FH);
		$self->log->info('Set break for qid ' . $qid);
		return 1;
	}
	else {
		if (-f $self->conf->get('query_cancel_dir') . '/' . $qid){
			$self->log->info('Found break for qid ' . $qid);
			return 1;
		}
		else {
			return 0;
		}
	}
}

sub unbreak {
	my $self = shift;
	my $qid = sprintf('%d', shift);
	throw_e error => 'No query_cancel_dir set!' unless -d $self->conf->get('query_cancel_dir');
	
	if (-f $self->conf->get('query_cancel_dir') . '/' . $qid){
		$self->log->info('Removing break for qid ' . $qid);
		unlink $self->conf->get('query_cancel_dir') . '/' . $qid;
		return 1;
	}
	else {
		return 0;
	}
	
}

sub _archive_query {
	my $self = shift;
	my $args = shift;
	
	throw_params param => 'args', value => $args 
		unless $args and ref($args) eq 'HASH';
	throw_params param => 'query_meta_params', value => Dumper($args->{query_meta_params})
		unless $args->{query_meta_params} and ref($args->{query_meta_params}) eq 'HASH';
	throw_params param => 'qid', value $args unless $args->{qid};
	
	$self->{_META_PARAMS} = $args->{query_meta_params};
	$self->{_LIMIT} = $Default_limit;
	$self->{_OFFSET} = 0;
	if ($args->{query_meta_params}->{timeout}){
		$self->{_TIMEOUT} = sprintf("%d", ($args->{query_meta_params}->{timeout} * 1000)); #time is in milleseconds
	}
	else {
		$self->{_TIMEOUT} = sprintf("%d", ($self->conf->get('manager/query_timeout') * 1000));
	}
	$self->log->debug("Using timeout of $self->{_TIMEOUT}");
	
	# If we were given a hash (from some other module), parse that into a query string
	if ($args->{query}){
		my @query_terms;
		while( my ($param, $value) = each %{ $args->{query} }){
			#printf("1: param: %s, value: %s\n", Dumper($param), Dumper($value));
			if (ref($value) eq 'HASH'){
				$value = join('&', join('=', each %{$value} ));
				push @query_terms, $value;
			}
			else {
				push @query_terms, $param . '=' . $value;
			}
			#printf("2: param: %s, value: %s\n", Dumper($param), Dumper($value));
		}
		$args->{query_string} = join('&', @query_terms);
		$args->{query_string} =~ s/\s+/\%20/g;
		print "query_string: " . $args->{query_string} . "\n";
	}
	throw_params param => 'query_string', value => undef
		unless $args->{query_string};
	
	my $raw_query = $args->{query_string};
	
	# Attach the query filters for this user from permissions
	my $filtered_raw_query = $raw_query;
	if ($self->{_META_PARAMS}->{permissions}->{filter}){
		$filtered_raw_query .= ' ' . $self->{_META_PARAMS}->{permissions}->{filter};
	}
	
	# Check to see if the class was given in meta params
	if ($self->{_META_PARAMS}->{class}){
		$self->{_GIVEN_CLASSES}->{ sprintf("%d", $self->{_KNOWN_CLASSES}->{ $self->{_META_PARAMS}->{class} }) } = 1;
	}
	
	# Check for meta limit
	if ($self->{_META_PARAMS}->{limit}){
		$self->{_LIMIT} = sprintf("%d", $self->{_META_PARAMS}->{limit});
		$self->log->debug("Set limit " . $self->{_LIMIT});
	}
	
	my $classes = $self->get_classes_by_name();
	
	my %attrs;

	my $callback = sub {
		my ($col, $op, $val) = @_;
		$self->log->debug('callback with col:' . $col . ', op:' . $op . 'val:' . $val);
		my $fields = $self->get_field($col);
		my @terms;
		foreach my $class_id (keys %$fields){
			my $field = $fields->{$class_id};
			$self->log->debug('field: ' . Dumper($field));
			my $normalized_val = $self->normalize_value($class_id, $val, $field->{field_order});
			my $term = '(class_id=' . $self->db->quote($class_id) . ' AND ';
			if ($field->{field_type} eq 'int'){
				$term .= $ELSA::Field_order_to_field->{ $field->{field_order} } . $op . $self->db->quote($normalized_val);
			}
			else {
				if ($op eq '!=' or $op =~ /^\s*NOT/){
					$term .=  $ELSA::Field_order_to_field->{ $field->{field_order} } . ' NOT LIKE ' . $self->db->quote('%' . $normalized_val . '%');
				}
				else {
					$term .=  $ELSA::Field_order_to_field->{ $field->{field_order} } . ' LIKE ' . $self->db->quote('%' . $normalized_val . '%');
				}
			}
			$term .= ')';
			push @terms, $term;
		}
		return '('. join(' OR ', @terms) . ')';
	};
	
	my $msg_callback = sub {
		my ($col, $op, $val) = @_;
		#$val =~ s/([\_\%])/\\$1/g; # escape any special chars
		my $term;
		if ($op eq '!=' or $op =~ /^\s*NOT/){
			$term = 'msg NOT LIKE ' . $self->db->quote('%' . $val . '%');
		}
		else {
			$term .=  'msg LIKE ' . $self->db->quote('%' . $val . '%');
		}
		
		return '('. $term . ')';
	};
	
	my $limit_callback = sub {
		my ($col, $op, $val) = @_;
		$self->{_LIMIT} = sprintf('%d', $val);
	};
	
	# Build list of columns
	my $columns = {
		'timestamp' => {
			name => 'timestamp',
			type => 'timestamp',
		},
		'program_id' => {
			name => 'program_id',
			type => 'integer',
			alias => 'program',
			callback => sub {
		        my ($col, $op, $val) = @_;
		        return "$col $op " . crc32($val);
		    },
		},
		'class_id' => {
			name => 'class_id',
			type => 'integer',
			alias => 'class',
			callback => sub {
		        my ($col, $op, $val) = @_;
		        return "$col $op " . $classes->{$val};
			},
		},
		'msg' => {
			name => 'msg',
			type => 'varchar',
			callback => $msg_callback,
			#fuzzy_op => 'LIKE',
			#fuzzy_not_op => 'NOT LIKE',
		},
		'limit' => {
			name => 'limit',
			callback => $limit_callback,
		}
	};
	
		
	foreach my $field (@{ $self->get_fields() }){
		$columns->{ $field->{fqdn_field} } = {
			name => $field->{fqdn_field},
			callback => $callback,
		};
		$columns->{ $field->{value} } = {
			name => $field->{value},
			callback => $callback,
		};
	}
		
	#$self->log->debug('columns: ' . Dumper($columns));
	
	my $where_clause;
	
	if ($raw_query =~ /\S/){
		
		my $qp = new Search::QueryParser::SQL(
			columns => $columns,
			default_column => 'msg',
			#fuzzify2 => 1,
		);
		#$self->log->debug('qp: ' . Dumper($qp));
		my $sql_parser = $qp->parse($filtered_raw_query, $Implicit_plus) or throw_e error => $qp->err;
		#$self->log->debug('sql_parser: ' . Dumper($sql_parser));
		$where_clause = $sql_parser->stringify();
		$self->log->debug('where_clause: ' . $where_clause);
		#$self->log->debug('placeholders: '. Dumper(\@placeholders));
	}
	else {
		throw_e error => 'No query terms given';
	}
	$where_clause .= ' AND TIMESTAMP BETWEEN ? AND ?';
	
	# Adjust times if necessary
	unless ($self->{_META_PARAMS}->{start}){
		$self->{_META_PARAMS}->{start} = 0;
	}
	unless ($self->{_META_PARAMS}->{end}){
		$self->{_META_PARAMS}->{end} = CORE::time();
	}
	
	# Find tables we'll need
	my ($query, $sth);
	$query = sprintf('SELECT table_name FROM %s.v_directory WHERE table_type="archive"' . "\n" .
		'AND (? BETWEEN table_start_int AND table_end_int ' . "\n" .
			'OR (? < table_start_int AND table_end_int < ?)' . "\n" .
			'OR ? BETWEEN table_start_int AND table_end_int)' . "\n" .
		'ORDER BY table_start_int ASC',
		$ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($self->{_META_PARAMS}->{start}, 
		$self->{_META_PARAMS}->{start}, $self->{_META_PARAMS}->{end},
		$self->{_META_PARAMS}->{end});
	my @tables;
	while (my $row = $sth->fetchrow_hashref){
		push @tables, $row->{table_name};
	}
	
	unless (scalar @tables){
		$self->log->warn('No tables found');
		return 0;
	}
	
	# Get the fields we'll resolve column names to
	my %resolved_fields;
	$self->{_CLASSES} = $self->get_classes();
	foreach my $class_id (keys %{ $self->{_CLASSES} }){
		$resolved_fields{$class_id} = $self->get_fields_arr_by_order($class_id);
	}
	#$self->log->debug("resolved_fields: " . Dumper(\%resolved_fields));
	
	my $found = 0;
	$self->{_RAW_RESULTS}->{warnings} = [];
	
	my $groupby = '';
	my $groupby_field = '';
	# Add a groupby query if necessary
	if ($self->{_META_PARAMS}->{groupby}){
		# There must be a given class for this to make sense
		my $groupby_class_id = 0;
		if ($self->{_GIVEN_CLASSES}){
			# use the first key found (there should only be one)
			foreach my $id (keys %{ $self->{_GIVEN_CLASSES} }){
				$groupby_class_id = $id;
				last;
			}
		}
		foreach my $field (@{ $self->{_META_PARAMS}->{groupby} }){
			my $field_infos = $self->get_field($field);
			$self->log->debug("Attempting to groupby field $field from class_id $groupby_class_id and field_infos " . Dumper($field_infos));
			my $resolved_field = $field_infos->{$groupby_class_id} ?
				$ELSA::Field_order_to_field->{ $field_infos->{$groupby_class_id}->{field_order} } : '';
			unless ($resolved_field){
				# check to see if this was an "any" class field like host or program
				if ($field_infos->{0}){
					$resolved_field = $ELSA::Field_order_to_field->{ $field_infos->{0}->{field_order} };
					# Tack on the '_id' suffix
					#$resolved_field .= '_id';
					$groupby_class_id = 0;
				}
				else {
					$self->log->debug('class_id ' . $groupby_class_id . ' does not have field ' . $field);
					next;
				}
			}
			$self->log->debug('resolved field: ' . $resolved_field);
			next unless $resolved_field;
			$groupby_field = $resolved_field;
			$groupby = 'GROUP BY ' . $resolved_field . "\n";
			$where_clause .= ' AND class_id=' . $groupby_class_id;
			last; # really doesn't make sense to have more than one groupby, we'll just use the first
		}
		
		$self->{_GROUPS} = {};
		my $given_groupby_field = $self->{_META_PARAMS}->{groupby}->[0]; # only doing one
		$self->{_GROUPS}->{ $given_groupby_field } = [];
		$query = 'SELECT SUM(count) AS count, groupby_sub AS groupby FROM (';
		my @subqueries;
		my @params;
		my $counter = 0;
		foreach my $table_name (@tables){
			push @subqueries, 'SELECT COUNT(*) AS count, ' . $groupby_field . ' AS groupby_sub ' . "\n" .
			'FROM '	. $table_name  . ' t' . $counter . "\n" .
			'WHERE ' . $where_clause . "\n" .
			$groupby;
			push @params, $self->{_META_PARAMS}->{start}, $self->{_META_PARAMS}->{end};
			$counter++;
		}
		$query .= join(' UNION ', @subqueries) . ') derived_table ' . "\n" .
			'GROUP BY groupby' . "\n" .
			'ORDER BY count DESC' . "\n" .
			"LIMIT ?,?";
		eval {
			$sth = $self->db->prepare($query);
			$self->log->debug("query: $query");
			$sth->execute(@params, 0, $self->{_LIMIT});
			while (my $row = $sth->fetchrow_hashref){
				$found++;
				last if $found > $self->{_LIMIT};
				
				# See if we need to apply a conversion
				$row->{groupby} = $self->resolve_value($groupby_class_id, $row->{groupby}, $ELSA::Field_to_order->{$groupby_field});
				
				push @{ $self->{_GROUPS}->{ $given_groupby_field } }, $row;
			}
			$self->{_RAW_RESULTS}->{total_found} = scalar @{ $self->{_GROUPS}->{ $given_groupby_field } };
			$self->{_RAW_RESULTS}->{total_returned} = scalar @{ $self->{_GROUPS}->{ $given_groupby_field } };
		};
		if ($@){
			$self->log->error('Query ' . $query . "\n" . ' got error ' . $@);
		}
	}
	else {
		TABLE_LOOP: foreach my $table_name (@tables){
			if ($self->break($args->{qid})){ # check to make sure this query hasn't been cancelled
				# remove the lock file
				$self->unbreak($args->{qid});
				last;
			}
			
			$query = sprintf("SELECT main.id,\n" .
				"\"" . $self->conf->get('manager/server_name') . "\" AS node,\n" .
				"DATE_FORMAT(FROM_UNIXTIME(timestamp), \"%%Y/%%m/%%d %%H:%%i:%%s\") AS timestamp,\n" .
				"INET_NTOA(host_id) AS host, program, class_id, class, rule_id, msg,\n" .
				"i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5\n" .
				"FROM %s main\n" .
				"JOIN syslog.programs ON main.program_id=programs.id\n" .
				"JOIN syslog.classes ON main.class_id=classes.id\n" .
				"WHERE %s\n" .
				$groupby .
				"LIMIT ?,?",
				$table_name, $where_clause, 0, $self->{_META_PARAMS}->{limit});
		
			eval {
				$sth = $self->db->prepare($query);
				$self->log->debug("query: $query");
				$sth->execute($self->{_META_PARAMS}->{start}, $self->{_META_PARAMS}->{end}, 0, $self->{_LIMIT});
				while (my $row = $sth->fetchrow_hashref){
					$found++;
					last TABLE_LOOP if $found > $self->{_LIMIT};
					$row->{_fields} = [
						{ field => 'host', value => $row->{host}, class => 'any' },
						{ field => 'program', value => $row->{program}, class => 'any' },
						{ field => 'class', value => $row->{class}, class => 'any' },
					];
					# Resolve column names for fields
					foreach my $col qw(i0 i1 i2 i3 i4 i5 s0 s1 s2 s3 s4 s5){
						my $value = delete $row->{$col};
						if ($value and $resolved_fields{ $row->{class_id} }->{ $ELSA::Field_to_order->{$col} }){
							my $field = $resolved_fields{ $row->{class_id} }->{ $ELSA::Field_to_order->{$col} }->{value};
							# Swap the generic name with the specific field name for this class
							#$self->log->debug("swapping $col with $field and value $value f_t_order: " . $ELSA::Field_to_order->{$col});
							
							# See if we need to apply a conversion
							$value = $self->resolve_value($row->{class_id}, $value, $ELSA::Field_to_order->{$col});
							#$self->log->debug("resolved $field value to $value");
							push @{ $row->{_fields} }, { 'field' => $field, 'value' => $value, 'class' => $self->{_CLASSES}->{ $row->{class_id} } };
						}
					}
					if ($self->{_META_PARAMS}->{groupby}){
						$self->{_RAW_RESULTS}->{groups}->{ $self->{_META_PARAMS}->{groupby} } ||= [];
						push @{ $self->{_RAW_RESULTS}->{groups}->{ $self->{_META_PARAMS}->{groupby} } }, $row;
					}
					else {
						push @{ $self->{_RESULTS} }, $row;
					}
				}
			};
			if ($@){
				$self->log->error('Query ' . $query . "\n" . ' got error ' . $@);
			}
			$self->{_RAW_RESULTS}->{total_found} = scalar @{ $self->{_RESULTS} };
			$self->{_RAW_RESULTS}->{total_returned} = scalar @{ $self->{_RESULTS} };
		}
	}
		
	return 1;
}

sub query {
	my $self = shift;
	my $args = shift;
	
	throw_params param => 'args', value => $args 
		unless $args and ref($args) eq 'HASH';
	throw_params param => 'query_meta_params', value => Dumper($args->{query_meta_params})
		unless $args->{query_meta_params} and ref($args->{query_meta_params}) eq 'HASH';
	
	$self->stats->mark('query');
	$self->{_META_PARAMS} = $args->{query_meta_params};
	$self->{_LIMIT} = $Default_limit;
	$self->{_OFFSET} = 0;
	if ($args->{query_meta_params}->{timeout}){
		$self->{_TIMEOUT} = sprintf("%d", ($args->{query_meta_params}->{timeout} * 1000)); #time is in milleseconds
	}
	else {
		$self->{_TIMEOUT} = sprintf("%d", ($self->conf->get('manager/query_timeout') * 1000));
	}
	$self->log->debug("Using timeout of $self->{_TIMEOUT}");
	
	# If we were given a hash (from some other module), parse that into a query string
	if ($args->{query}){
		my @query_terms;
		while( my ($param, $value) = each %{ $args->{query} }){
			printf("1: param: %s, value: %s\n", Dumper($param), Dumper($value));
			if (ref($value) eq 'HASH'){
				$value = join('&', join('=', each %{$value} ));
				push @query_terms, $value;
			}
			else {
				push @query_terms, $param . '=' . $value;
			}
			printf("2: param: %s, value: %s\n", Dumper($param), Dumper($value));
		}
		$args->{query_string} = join('&', @query_terms);
		$args->{query_string} =~ s/\s+/\%20/g;
		print "query_string: " . $args->{query_string} . "\n";
	}
	throw_params param => 'query_string', value => undef
		unless $args->{query_string};
		
	if ($self->{_META_PARAMS}->{archive_query}){
		return $self->_archive_query($args);
	}
	
	# Parse our query
	$self->stats->mark('query_parse');
	$self->_parse_query_string($args->{query_string}, $args->{stopwords});
	
	$self->stats->mark('query_parse', 1);
	
	# Find available data
	if ($self->{_META_PARAMS}->{indexes}){
		$self->{_INDEXES} = $self->{_META_PARAMS}->{indexes};
	}
	else {
		throw_e error => 'No indexes given to query';
	}
	#$self->log->debug('number of indexes before pruning: ' . (scalar keys %{ $self->{_INDEXES} }));
	
	# Find the latest index time
	my ($min_start, $min_end, $max_start, $max_end) = (0, 0, 0, 0);
	foreach my $index_name (keys %{ $self->{_INDEXES} }){
		if ($self->{_INDEXES}->{$index_name}->{start} > $max_start){
			$max_start = $self->{_INDEXES}->{$index_name}->{start};
		}
		if ($self->{_INDEXES}->{$index_name}->{end} > $max_end){
			$max_end = $self->{_INDEXES}->{$index_name}->{end};
		}
	}
	
	# Check to see if the query is after the latest end, but not in the future (this happens if the indexing process is backed up)
	if ($self->{_START_INT} <= time() and $self->{_START_INT} > $max_end){
		$self->{_START_INT} = $max_start;
	}
	if ($self->{_END_INT} <= time() and $self->{_END_INT} > $max_end){
		$self->{_END_INT} = $max_end;
	}
	
	# Prune indexes that aren't in our time range
	foreach my $index_name (keys %{ $self->{_INDEXES} }){
		unless (($self->{_INDEXES}->{$index_name}->{start} >= $self->{_START_INT}
				and $self->{_INDEXES}->{$index_name}->{start} <= $self->{_END_INT})
			or ($self->{_INDEXES}->{$index_name}->{start} >= $self->{_START_INT}
				and $self->{_INDEXES}->{$index_name}->{end} <= $self->{_END_INT})
			or ($self->{_INDEXES}->{$index_name}->{start} <= $self->{_START_INT}
				and $self->{_INDEXES}->{$index_name}->{end} >= $self->{_END_INT})
			or ($self->{_INDEXES}->{$index_name}->{end} >= $self->{_START_INT}
				and $self->{_INDEXES}->{$index_name}->{end} <= $self->{_END_INT})
		){
			my $deleted = delete $self->{_INDEXES}->{$index_name};
			#$self->log->trace('Pruning index ' . $index_name . Dumper($deleted) . ' because it is too early');
			#$self->log->debug('number of indexes after pruning: ' . (scalar keys %{ $self->{_INDEXES} }));
		}
	}

	#$self->log->debug('indexes: ' . Dumper($self->{_INDEXES}));
	
	# The queries will be per index per class
	my $num_distinct_classes = scalar keys %{ $self->{_DISTINCT_CLASSES} };
	unless ($num_distinct_classes){
		# this query will use every class
		$num_distinct_classes = scalar keys %{ $self->{_CLASSES} };
	}
	if ((scalar keys %{ $self->{_INDEXES} }) * $num_distinct_classes > $Max_batch_queries){
		$self->log->warn('Too many indexes, returning meta');
		$self->{_INDEXES} = { 'distributed_meta' => 1 };
	}
	
	unless (scalar keys %{ $self->{_INDEXES} }){
		throw_e error => "No indexes found.";
	}
		
	# Execute search
	my $start = time();
	$self->_execute();
	$self->log->debug(sprintf("Executed in %.5f seconds", (time() - $start)));
	
	if ($self->{_META_PARAMS}->{groupby}){
		# Resolve groups content
		$self->_get_groups_content();
		$self->log->debug(sprintf("Got groups data in %.5f seconds", (time() - $start)));
	}
	$self->stats->mark('query', 1);	
	$self->log->info(sprintf("Query returned %d rows", scalar @{ $self->{_RAW_RESULTS}->{rows} }));
	return 1;
}

sub results {
	my $self = shift;
	return $self->{_RESULTS};
}

sub limit {
	my $self = shift;
	return $self->{_LIMIT};
}

sub warnings {
	my $self = shift;
	return $self->{_WARNINGS};
}

sub get_directory {
	my $self = shift;
	my ($query, $sth);
	
	$query = "SELECT table_name, min_id AS first_id, max_id AS last_id\n" .
		"FROM tables WHERE table_type_id=(SELECT id FROM table_types WHERE table_type=\"index\") ORDER BY min_id DESC";
	$sth = $self->db->prepare($query);
	$sth->execute();
	return $sth->fetchall_hashref('table_name');
}

sub _get_node_numberspaces {
	my $self = shift;
	
	my %numberspaces;
	
	$self->log->debug("PEERS: " . Dumper($self->{_PEERS}));
	foreach my $node (keys %{ $self->{_PEERS} }){
		# Calculate the id_offset which dictates which id numberspace this node uses
		my $id_offset = $self->conf->get('peer_id_multiplier') * $self->{_PEERS}->{$node};
		$numberspaces{$node} = { 
			base_id => $id_offset, 
			ceiling_id => $id_offset + $self->conf->get('peer_id_multiplier'),
		};	 
	}

	return \%numberspaces;
}

sub sort_by_node {
	my $self = shift;
	my $args = shift;
	
	throw_params param => 'peers', value => Dumper($args->{peers})
			unless $args->{peers} and ref($args->{peers}) eq 'HASH';
	$self->{_PEERS} = $args->{peers};
	# Tack on the local node's info
	$self->{_PEERS}->{ $self->conf->get('manager/server_name') } = $self->conf->get('manager/server_id');
	
	my $numberspaces = $self->_get_node_numberspaces();
	$self->log->debug("numberspaces: " . Dumper($numberspaces));
	
	# First pass sorts which nodes we're querying
	my %rows_by_node;
	my $total = 0;
	while (my $row = shift(@{ $self->{_RAW_RESULTS}->{rows} }) and $total < $self->limit() ){
		$total++;
		# Find which node this id belongs to based on numberspace
		foreach my $node (keys %{ $numberspaces }){
			if ($row->{doc} >= $numberspaces->{$node}->{base_id}
				and $row->{doc} < $numberspaces->{$node}->{ceiling_id} ){
				push @{ $rows_by_node{$node} }, $row;
				last;
			}
		}
	}
	
#	$self->log->debug("Using rows_by_node: " . Dumper(\%rows_by_node));
	
	return \%rows_by_node;
}

sub get_row_content {
	my $self = shift;
	my $args = shift;
	
	throw_params param => 'rows', value => Dumper($args->{rows})
		unless $args->{rows} and ref($args->{rows}) eq 'ARRAY';
	
	$self->stats->mark('get_row_content');
	
	my ($query, $sth);
	
	# First pass to finalize distinct classes by seeing what we actually got
	my %distinct_classes;
	foreach my $row (@{ $args->{rows} }){
		$distinct_classes{ $row->{class_id} } = 1;
	}
	
	unless ($self->{_CLASSES} and scalar keys %{ $self->{_CLASSES} }){
		$self->{_CLASSES} = $self->get_classes();
	}
	
	# Get the fields we'll resolve column names to
	my %resolved_fields;
	foreach my $class_id (keys %distinct_classes){
		$resolved_fields{$class_id} = $self->get_fields_arr_by_order($class_id);
	}
	$self->log->debug("resolved_fields: " . Dumper(\%resolved_fields));
	
	# Get our directory
	$self->{_DIRECTORY} = $self->get_directory();
	#$self->log->debug("directory: " . Dumper($self->{_DIRECTORY}));

	# Process our result rows
	my %statement_handles;
	foreach my $row (@{ $args->{rows} }){
		my $id = $row->{doc};
				
		# Find sth from statement_handles cache, else create it
		my $table_sth;
		STH_LOOP: foreach my $table_name (sort { $b cmp $a } keys %{ $self->{_DIRECTORY} }){
			if ($id >= $self->{_DIRECTORY}->{$table_name}->{first_id} 
				and $id <= $self->{_DIRECTORY}->{$table_name}->{last_id}){
				unless ($statement_handles{$table_name}){
					# Create statement handle
					$query = sprintf("SELECT main.id,\n" .
						"\"" . $self->conf->get('manager/server_name') . "\" AS node,\n" .
						"DATE_FORMAT(FROM_UNIXTIME(timestamp), \"%%Y/%%m/%%d %%H:%%i:%%s\") AS timestamp,\n" .
						"INET_NTOA(host_id) AS host, program, class_id, class, rule_id, msg,\n" .
						"i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5\n" .
						"FROM %s main\n" .
						"LEFT JOIN syslog.programs ON main.program_id=programs.id\n" .
						"LEFT JOIN syslog.classes ON main.class_id=classes.id\n" .
						"WHERE main.id=?",
						$table_name);
					$statement_handles{$table_name} = $self->db->prepare($query);
					$self->log->debug("query: $query");
				}
				$table_sth = $statement_handles{$table_name};
				last STH_LOOP;	
			}
		}
		#throw_e error => "Undefined table for id $id" unless $table_sth;
		unless ($table_sth){
			$self->log->error("Undefined table for id $id");
			next;
		}
						
		$table_sth->execute($id);
		my $row = $table_sth->fetchrow_hashref;
#		$self->log->debug("got row " . Dumper($row));
		# Do the meta data fields
		$row->{_fields} = [
			{ field => 'host', value => $row->{host}, class => 'any' },
			{ field => 'program', value => $row->{program}, class => 'any' },
			{ field => 'class', value => $row->{class}, class => 'any' },
		];
		# Resolve column names for fields
		foreach my $col qw(i0 i1 i2 i3 i4 i5 s0 s1 s2 s3 s4 s5){
			my $value = delete $row->{$col};
			if ($value and $resolved_fields{ $row->{class_id} }->{ $ELSA::Field_to_order->{$col} }){
				my $field = $resolved_fields{ $row->{class_id} }->{ $ELSA::Field_to_order->{$col} }->{value};
				# Swap the generic name with the specific field name for this class
				#$self->log->debug("swapping $col with $field and value $value f_t_order: " . $ELSA::Field_to_order->{$col});
				
				# See if we need to apply a conversion
				$value = $self->resolve_value($row->{class_id}, $value, $ELSA::Field_to_order->{$col});
				#$self->log->debug("resolved $field value to $value");
				push @{ $row->{_fields} }, { 'field' => $field, 'value' => $value, 'class' => $self->{_CLASSES}->{ $row->{class_id} } };
			}
		}
		push @{ $self->{_RESULTS} }, $row;
		
		$self->stats->mark('get_row_content', 1);
	}
	
	return 1;
}

sub _get_groups_content {
	my $self = shift;
	
	my ($query, $sth);
	$self->stats->mark('get_groups_content');
	
	# Process our result groups ( no need to go to the remote nodes for this)
	my $raw_field_to_table = {
		'program_id' => 'programs',
		'class_id' => 'classes',
		'class'=> 'classes',
		'program'=>'programs',
	};
	foreach my $field (keys %{ $self->{_RAW_RESULTS}->{groups} }){
		# Resolve the field id in @groupby
		$self->log->debug('Group field:'.$field);
		if ($field =~ /(\w+)\_id$/){
			my $db_field = $1;
			$self->log->debug('db_field:'.$db_field);
			$query = sprintf("SELECT %s FROM %s.%s WHERE id=?",
				$db_field, $ELSA::Meta_db_name, $raw_field_to_table->{$field});
			$sth = $self->db->prepare($query);
			foreach my $row (@{ $self->{_RAW_RESULTS}->{groups}->{$field} }){
				$self->log->debug("db_field: $db_field, row: " . Dumper($row));
				$sth->execute($row->{'@groupby'});
				my $resolve_row = $sth->fetchrow_hashref;
				$row->{'@groupby'} = $resolve_row->{$db_field};	
				$self->log->debug("resolve_row db_field: $resolve_row->{$db_field}");
			}
		}
		elsif(exists($raw_field_to_table->{$field})){
			$query = sprintf("SELECT %s FROM %s.%s WHERE id=?",
				$field, $ELSA::Meta_db_name, $raw_field_to_table->{$field});
			$sth = $self->db->prepare($query);
			foreach my $row (@{ $self->{_RAW_RESULTS}->{groups}->{$field} }){
				$self->log->debug("db_field: $field, row: " . Dumper($row));
				$sth->execute($row->{'@groupby'});
				my $resolve_row = $sth->fetchrow_hashref;
				$row->{'@groupby'} = $resolve_row->{$field};	
				$self->log->debug("resolve_row db_field: $resolve_row->{$field}");				
			}
		}
		else {
			#TODO deal with groupby's that have different value resolve algorithms
			foreach my $row (@{ $self->{_RAW_RESULTS}->{groups}->{$field} }){
				my $field_order = -1;
				my $field_infos = $self->get_field($field);
				if ($field_infos and $field_infos->{ $row->{class_id} } and $field_infos->{ $row->{class_id} }->{field_order}){
					$field_order = $field_infos->{ $row->{class_id} }->{field_order};
				}
				else {
					$self->log->debug('class_id ' . $row->{class_id} . ' does not have field ' . $field);
					next;
					#throw_e error => 'Unable to find field_order from field ' . $field . ' and row ' . Dumper($row) . ' and field_infos: ' . Dumper($field_infos);
				}
				$row->{'@groupby'} = $self->resolve_value($row->{class_id}, $row->{'@groupby'}, $field_order);
			}
		}
		# Sort these in descending value order
		$self->{_GROUPS}->{$field} = [ rukeysort { $_->{'@count'} } @{ $self->{_RAW_RESULTS}->{groups}->{$field} } ];
		$self->log->debug('_GROUPS: ' . Dumper($self->{_GROUPS}));
	}
	
	$self->log->debug('_GROUPS: ' . Dumper($self->{_GROUPS}));
	$self->log->debug('_RAW_RESULTS groups: ' . Dumper($self->{_RAW_RESULTS}->{groups}));
	
	$self->stats->mark('get_groups_content', 1);
	return 1;
}

sub groups {
	my $self = shift;
	return $self->{_GROUPS};
}

sub _set_sphinx_global_filters {
	my $self = shift;
	my $sphinx = shift;
	
	# Apply start/end filters
	if (defined $self->{_START_INT} and defined $self->{_END_INT}){
		$sphinx->SetFilterRange('timestamp', $self->{_START_INT}, $self->{_END_INT});
	}
	elsif (defined $self->{_START_INT}){
		$sphinx->SetFilterRange('timestamp', $self->{_START_INT}, (2**32)-1);
	}
	elsif (defined $self->{_END_INT}){
		$sphinx->SetFilterRange('timestamp', 0, $self->{_END_INT});
	}
	
	# Apply all-class filters (attributes with class_id 0)
	# ANDs
	if ($self->{_ATTR_TERMS}->{and}->{0}){
		foreach my $attr (keys %{ $self->{_ATTR_TERMS}->{and}->{0} }){
			if (ref($self->{_ATTR_TERMS}->{and}->{0}->{$attr}) eq 'ARRAY' 
				and scalar @{ $self->{_ATTR_TERMS}->{and}->{0}->{$attr} }){
				$self->log->trace('Setting global filter AND ' . $attr . ' ' . join(' ', @{ $self->{_ATTR_TERMS}->{and}->{0}->{$attr} }));
				$sphinx->SetFilter($attr, [ @{ $self->{_ATTR_TERMS}->{and}->{0}->{$attr} } ] );
			}
			elsif (ref($self->{_ATTR_TERMS}->{and}->{0}->{$attr}) eq 'HASH' 
				and scalar keys %{ $self->{_ATTR_TERMS}->{and}->{0}->{$attr} }){
				$self->log->trace('Setting global filter AND ' . $attr . ' ' . join(' ', keys %{ $self->{_ATTR_TERMS}->{and}->{0}->{$attr} }));
				$sphinx->SetFilter($attr, [ keys %{ $self->{_ATTR_TERMS}->{and}->{0}->{$attr} } ] );
			}
		}
	}
	# NOTs
	if ($self->{_ATTR_TERMS}->{not}->{0}){
		foreach my $attr (keys %{ $self->{_ATTR_TERMS}->{not}->{0} }){
			if (ref($self->{_ATTR_TERMS}->{not}->{0}->{$attr}) eq 'ARRAY'
				and scalar @{ $self->{_ATTR_TERMS}->{not}->{0}->{$attr} }){
				$self->log->trace('Setting global filter NOT ' . $attr . ' ' . join(' ', @{ $self->{_ATTR_TERMS}->{not}->{0}->{$attr} }));
				$sphinx->SetFilter($attr, [ @{ $self->{_ATTR_TERMS}->{not}->{0}->{$attr} } ], 1);
			}
			elsif (ref($self->{_ATTR_TERMS}->{not}->{0}->{$attr}) eq 'HASH'
				and scalar keys %{ $self->{_ATTR_TERMS}->{not}->{0}->{$attr} }){
				$self->log->trace('Setting global filter NOT ' . $attr . ' ' . join(' ', keys %{ $self->{_ATTR_TERMS}->{not}->{0}->{$attr} }));
				$sphinx->SetFilter($attr, [ keys %{ $self->{_ATTR_TERMS}->{not}->{0}->{$attr} } ], 1);
			}
		}
	}
	
	# Loop through and see if we have "between" statements where min and max are supplied by two separate range ops
	
	foreach my $boolean qw(range_and range_not){
		foreach my $attr (keys %{ $self->{_ATTR_TERMS}->{$boolean}->{0} }){
			foreach my $filter_hash (@{ $self->{_ATTR_TERMS}->{$boolean}->{0}->{$attr} }){
				$self->log->trace('Setting global filter range: ' . join(', ', $boolean, $filter_hash->{attr}, $filter_hash->{min}, $filter_hash->{max}, $filter_hash->{exclude}));
				$sphinx->SetFilterRange($filter_hash->{attr}, $filter_hash->{min}, $filter_hash->{max}, $filter_hash->{exclude});
			}
		}
	}

	
	return 1;
}

sub _execute {
	my $self = shift;
	
	$self->stats->mark('query_execute');
	my $cutoff = 0;
	# Still not seeing this impact query times, so going without cutoff
#	my $cutoff = $Max_limit;
#	if ($self->{_META_PARAMS}->{groupby}){
#		# No cutoff limit for group bys since sphinx will need to analyze a lot of records
#		$cutoff = 0;
#	}
	
	my $sphinx = new Sphinx::Search({debug => 1, log => $self->log, cutoff => $cutoff}); 
	$sphinx->SetServer($self->conf->get('sphinx/host'), $self->conf->get('sphinx/port'))
		or throw_e error => $sphinx->GetLastError();
	
	$sphinx->SetLimits($self->{_OFFSET}, $self->{_LIMIT}, $Max_limit, $cutoff);
	$sphinx->SetMaxQueryTime($self->{_TIMEOUT});
	$sphinx->SetMatchMode(SPH_MATCH_EXTENDED2);
	$sphinx->SetRankingMode(SPH_RANK_NONE);
	$sphinx->SetSortMode(SPH_SORT_ATTR_ASC, 'timestamp');
	
	$self->{_QUERIES} = []; # place to store our query with our result in a multi-query
	
	$self->_set_sphinx_global_filters($sphinx);
	
	# We will run a query per distinct class, but they will be packaged together for optimization
	foreach my $class_id (sort keys %{ $self->{_DISTINCT_CLASSES} }){
		next unless $class_id; # class 0 is handled in _set_sphinx_global_filters
		$self->log->trace('Setting filter for class_id=' . $class_id);
		$sphinx->SetFilter('class_id', [$class_id]) if $class_id;
		
		# Apply our fields
		my $filter_query = '';
		my @tmp_arr;
		
		#TODO ANY should really be any, not msg, but that's for another time...
		foreach my $term (@{ $self->{_ANY_FIELD_TERMS}->{and} }){
			#push @tmp_arr, '(@msg ' . $term . ')';
			push @tmp_arr, '(' . $term . ')';
		}
		if (scalar @tmp_arr){
			$filter_query .= ' (' . join(' ', @tmp_arr) . ')';
		}
		
		@tmp_arr = ();
		foreach my $term (@{ $self->{_ANY_FIELD_TERMS}->{or} }){
			#push @tmp_arr, '(@msg ' . $term . ')';
			push @tmp_arr, '(' . $term . ')';
		}
		if (scalar @tmp_arr){
			$filter_query .= ' (' . join('|', @tmp_arr) . ')';
		}
		
		@tmp_arr = ();
		foreach my $term (@{ $self->{_ANY_FIELD_TERMS}->{not} }){
			#push @tmp_arr, '(@msg ' . $term . ')';
			push @tmp_arr, '(' . $term . ')';
		}
		if (scalar @tmp_arr){
			$filter_query .= ' !(' . join('|', @tmp_arr) . ')';
		}
		
		$self->log->debug("no-field filter_query: $filter_query");
		
		
		# Then the individual class field terms
		# First, the ANDs
		@tmp_arr = ();
		foreach my $field (sort keys %{ $self->{_FIELD_TERMS}->{and}->{$class_id} }){
			foreach my $term (@{ $self->{_FIELD_TERMS}->{and}->{$class_id}->{$field} }){
				#push @tmp_arr, '@' . $field . ' ' . $term;
				push @tmp_arr, '(@' . $field . ' ' . $term . ')';
			}
		}
		if (scalar @tmp_arr){
			$filter_query .= ' (' . join(' ', @tmp_arr) . ')';
		}
				
		# Then, the NOTs
		@tmp_arr = ();
		foreach my $field (sort keys %{ $self->{_FIELD_TERMS}->{not}->{$class_id} }){
			foreach my $term (@{ $self->{_FIELD_TERMS}->{not}->{$class_id}->{$field} }){
				#push @tmp_arr, '@' . $field . ' ' . $term;
				push @tmp_arr, '(@' . $field . ' ' . $term . ')';
			}
		}
		if (scalar @tmp_arr){
			$filter_query .= ' !(' . join('|', @tmp_arr) . ')';
		}
		
		# Then, the ORs
		@tmp_arr = ();
		foreach my $field (sort keys %{ $self->{_FIELD_TERMS}->{or}->{$class_id} }){
			foreach my $term (@{ $self->{_FIELD_TERMS}->{or}->{$class_id}->{$field} }){
				#push @tmp_arr, '@' . $field . ' ' . $term;
				push @tmp_arr, '(@' . $field . ' ' . $term . ')';
			}
		}
		if (scalar @tmp_arr){
			$filter_query .= ' (' . join('|', @tmp_arr) . ')';
		}
		
		# Apply our attributes as filters
		# ANDs
		foreach my $attr (sort keys %{ $self->{_ATTR_TERMS}->{and}->{$class_id} }){
			$self->log->trace('Setting filter AND ' . $attr . ' ' . join(' ', @{ $self->{_ATTR_TERMS}->{and}->{$class_id}->{$attr} }));
			$sphinx->SetFilter( $attr, $self->{_ATTR_TERMS}->{and}->{$class_id}->{$attr} );
		}
		
		# NOTs
		foreach my $attr (sort keys %{ $self->{_ATTR_TERMS}->{not}->{$class_id} }){
			$self->log->trace('Setting filter NOT ' . $attr . ' ' . join(' ', @{ $self->{_ATTR_TERMS}->{not}->{$class_id}->{$attr} }));
			$sphinx->SetFilter($attr, $self->{_ATTR_TERMS}->{not}->{$class_id}->{$attr}, 1);
		}
		
		# For ranged filters, we'll have to do some trickiness here to calculate the right min/max values since
		#  sphinx doesn't do a true >/< operator
		
		my @attr_min_max;
		my $min_val = 0;
		my $max_val = 2**32 - 1; #uint
		# Loop through and see if we have "between" statements where min and max are supplied by two separate range ops
		foreach my $attr (sort keys %{ $self->{_ATTR_TERMS}->{range_and}->{$class_id} }){
			foreach my $filter_hash (@{ $self->{_ATTR_TERMS}->{range_and}->{$class_id}->{$attr} }){
				push @attr_min_max, { 
					attr => $attr, 
					min => $filter_hash->{min} ? $filter_hash->{min} : $min_val,
					max => $filter_hash->{max} ? $filter_hash->{max} : $max_val,
					exclude => 0,
				}
			}
		}
		foreach my $attr (sort keys %{ $self->{_ATTR_TERMS}->{range_not}->{$class_id} }){
			foreach my $filter_hash (@{ $self->{_ATTR_TERMS}->{range_not}->{$class_id}->{$attr} }){
				push @attr_min_max, { 
					attr => $attr, 
					min => $filter_hash->{min} ? $filter_hash->{min} : $min_val,
					max => $filter_hash->{max} ? $filter_hash->{max} : $max_val,
					exclude => 1,
				}			
			}
		}
		$self->log->debug("class_id $class_id attr_min_max: " . Dumper(\@attr_min_max));
	
		foreach my $filter_hash (@attr_min_max){
			$sphinx->SetFilterRange($filter_hash->{attr}, $filter_hash->{min}, $filter_hash->{max}, $filter_hash->{exclude});
			#$self->log->debug('adding filter_hash: ' . Dumper($filter_hash));
		}
		
		unless ($self->{_META_PARAMS}->{groups_only}){
			foreach my $index (keys %{ $self->{_INDEXES} }){
				push @{ $self->{_QUERIES} }, { filter => $filter_query, type => 'normal', class_id => $class_id };
				#$self->log->debug('filter_query: ' . $filter_query . ' index: ' . $index);
				$sphinx->AddQuery($filter_query, $index);
			}
		}
		
		# Add a groupby query if necessary
		if ($self->{_META_PARAMS}->{groupby}){
			foreach my $field (@{ $self->{_META_PARAMS}->{groupby} }){
				my $field_infos = $self->get_field($field);
				$self->log->debug("Attempting to chart field $field from class_id $class_id and field_infos " . Dumper($field_infos));
				my $resolved_field = $field_infos->{$class_id} ?
					$ELSA::Field_order_to_attr->{ $field_infos->{$class_id}->{field_order} } : '';
				my $groupby_class_id = $class_id;
				unless ($resolved_field){
					# check to see if this was an "any" class field like host or program
					if ($field_infos->{0}){
						$resolved_field = $ELSA::Field_order_to_attr->{ $field_infos->{0}->{field_order} };
						$groupby_class_id = 0;
					}
					else {
						$self->log->debug('class_id ' . $class_id . ' does not have field ' . $field);
						next;
					}
				}
				$self->log->debug('resolved field: ' . $resolved_field);
				next unless $resolved_field;
				$sphinx->SetGroupBy($resolved_field, SPH_GROUPBY_ATTR, '@count DESC');
				foreach my $index (keys %{ $self->{_INDEXES} }){
					push @{ $self->{_QUERIES} }, { filter => $filter_query, type => 'groupby', field => $field, class_id => $groupby_class_id };
					$sphinx->AddQuery($filter_query, $index);
				}
				$sphinx->ResetGroupBy(); 
			}
		}
		
		#$self->log->debug("sphinx: " . Dumper($sphinx));
		
		$self->log->debug('sphinx filters before reset: ' . Dumper($sphinx->{_filters}));
		$sphinx->ResetFilters(); # prevent the filters from falling through
		$self->log->debug('sphinx filters after reset: ' . Dumper($sphinx->{_filters}));
		$self->_set_sphinx_global_filters($sphinx); #re-apply global filters for next pass
		$self->log->debug('sphinx filters after reapplication: ' . Dumper($sphinx->{_filters}));
	} # end class_id loop
	
	# In case there were no specific classes to be had
	unless (scalar keys %{ $self->{_DISTINCT_CLASSES} }){
		
		# Filter out excluded classes
		if (scalar keys %{ $self->{_EXCLUDED_CLASSES} }){
			$self->log->debug("Excluding classes " . join(', ', keys %{ $self->{_EXCLUDED_CLASSES} }));
			$sphinx->SetFilter('class_id', [ keys %{ $self->{_EXCLUDED_CLASSES} } ], 1 );	
		}
		
		my $filter_query = '';
		# Now add the ANY field terms
		#AND
		foreach my $term (@{ $self->{_ANY_FIELD_TERMS}->{and} }){
			$filter_query .= ' ' . $term;
		}
		
		#OR
		if (scalar @{ $self->{_ANY_FIELD_TERMS}->{or} }){
			$filter_query .= ' (' . join('|', @{ $self->{_ANY_FIELD_TERMS}->{or} }) . ')';
		}
				
		#NOT
		foreach my $term (@{ $self->{_ANY_FIELD_TERMS}->{not} }){
			$filter_query .= ' !' . $term;
		}
		$self->log->debug("no-field filter_query: $filter_query");
		#$self->log->debug("no-field sphinx: " . Dumper($sphinx));
		
		unless ($self->{_META_PARAMS}->{groups_only}){
			foreach my $index (keys %{ $self->{_INDEXES} }){
				push @{ $self->{_QUERIES} }, { filter => $filter_query, type => 'normal' };
				$sphinx->AddQuery($filter_query, $index);
			}
		}
		
		# Add a groupby query if necessary
		if ($self->{_META_PARAMS}->{groupby}){
			my %fields_done;
			foreach my $field (@{ $self->{_META_PARAMS}->{groupby} }){
				unless ($fields_done{$field}){
					my $field_infos = $self->get_field($field);
					$self->log->debug('field_infos:'.Dumper($field_infos));
					foreach my $groupby_class_id (keys %{$field_infos}){
						#$self->log->debug($groupby_class_id);
						next unless $self->{_PERMITTED_CLASSES}->{$groupby_class_id};
						my $resolved_field = $ELSA::Field_order_to_attr->{ $field_infos->{$groupby_class_id}->{field_order} };
						$self->log->debug(Dumper($resolved_field));
						$sphinx->SetGroupBy($resolved_field, SPH_GROUPBY_ATTR, '@count DESC');
						foreach my $index (keys %{ $self->{_INDEXES} }){
							push @{ $self->{_QUERIES} }, { filter => $filter_query, type => 'groupby', field => $field };
							$sphinx->AddQuery($filter_query, $index);
						}
						$fields_done{$field} = 1;
					}
				}	
			}
			$sphinx->ResetGroupBy(); 	
		}
	}
	
	$self->log->debug("sphinx before RunQueries SetGroupBy: " . Dumper($sphinx));
	$self->stats->mark('sphinx_run_queries');
	my $sphinx_results = $sphinx->RunQueries();
	$self->stats->mark('sphinx_run_queries', 1);
	$self->log->debug("sphinx_results: " . Dumper($sphinx_results));
	
	# Aggregate the multi-query results
	$self->{_RAW_RESULTS} = { rows => [], groups => {}, total_found => 0 };
	my $raw_total = 0;
	my (@errors, @warnings);
	my $total_time = 0;
	my $counter = 0;
	my $total_found = 0;
	my %groupby_arrs;
	my $groupby_class_id = 0;
	$self->log->trace('QUERIES: ' . Dumper($self->{_QUERIES}));
	foreach my $result_set (@{ $sphinx_results }){
		#$self->log->trace('counter: ' . $counter . ', type: ' . $self->{_QUERIES}->[$counter]->{type});
		$total_found += $result_set->{total_found};
		$raw_total += scalar @{ $result_set->{matches} };
		$total_time += $result_set->{'time'};
		if ($result_set->{error}){
			push @errors, $result_set->{error};
		}
		elsif (scalar @{ $result_set->{matches} } and $self->{_QUERIES}->[$counter]->{type} eq 'groupby'){
			my $groupby_field = $self->{_QUERIES}->[$counter]->{field};
			# Summarize results (keys can be spread across disparate classes, and we need to aggregate).
			$groupby_arrs{ $groupby_field } ||= {};
			foreach my $row (@{ $result_set->{matches} }){
				$groupby_arrs{ $groupby_field }->{ $row->{'@groupby'} } += $row->{'@count'};
				# This will work as long as the disparate classes have the same resolve_value algorithm
				$groupby_class_id = $self->{_QUERIES}->[$counter]->{class_id};
				#$self->log->debug('adding groupby ' . $row->{'@groupby'} . ' count ' . $row->{'@count'} . ' which yields ' . Dumper(\%groupby_arrs));
			}
		}
		else {
			push @{ $self->{_RAW_RESULTS}->{rows} }, @{ $result_set->{matches} };	
		}
		
		if ($result_set->{warning}){
			push @warnings, $result_set->{warning};
		}
		$counter++;
	}
	$self->log->debug('%groupby_arrs: ' . Dumper(\%groupby_arrs));
	if (scalar keys %groupby_arrs){
		# Make into an array
		my @arr;
		foreach my $field (keys %groupby_arrs){
			foreach my $groupby (keys %{ $groupby_arrs{$field} }){
				push @arr, { 
					'class_id' => $groupby_class_id, # we need this to pass to field_infos later to get the field_order
					'@groupby' => $groupby, 
					'@count' => $groupby_arrs{$field}->{$groupby},
				};
			}
			$self->{_RAW_RESULTS}->{groups}->{ $field } = [ @arr ];
			# TODO this won't work for multiple groupby fields as the last one will overwrite--the total_found should be an arr
			$total_found = scalar @arr;
		}
	}
	
	# Sort the rows by timestamp (they are initially sorted by class_id, then timestamp)
	$self->{_RAW_RESULTS}->{rows} = [ sort { $a->{timestamp} <=> $b->{timestamp} } @{ $self->{_RAW_RESULTS}->{rows} } ];
	
	$self->{_RAW_RESULTS}->{warnings} = \@warnings;
	$self->{_RAW_RESULTS}->{total_found} = $total_found;
	$self->{_RAW_RESULTS}->{total_returned} = scalar @{ $self->{_RAW_RESULTS}->{rows} };
	#$self->log->trace("RAW_RESULTS: " . Dumper($self->{_RAW_RESULTS}));
	$self->log->debug(sprintf("raw_total was %d", $raw_total));
	if (scalar @errors){
		foreach my $error (@errors){
			$self->log->error("SPHINX ERROR: $error");
		}
	}
	if (scalar @warnings){
		foreach my $warning (@warnings){
			$self->log->error("SPHINX warning: $warning");
		}
	}
	unless (defined $sphinx_results){
		throw_e error => 'SPHINX ERROR: ' . $sphinx->GetLastError() . "\n" . $sphinx->GetLastWarning();
	}
	$self->stats->mark('query_execute', 1);
}

sub total_found {
	my $self = shift;
	return $self->{_RAW_RESULTS}->{total_found};
}

sub total_returned {
	my $self = shift;
	return $self->{_RAW_RESULTS}->{total_returned};
}

sub get_indexes {
	my $self = shift;
	my $args = shift;
	
	$self->stats->mark('local_get_indexes');
	my $start = $self->{_START_INT};
	my $end = $self->{_END_INT};
	
	if ($args){
		throw_params param => 'args', value => $args 
			unless ref($args) eq 'HASH' and $args->{start} and $args->{end};
		$start = $args->{start};
		$end = $args->{end};
	}

	my ($query, $sth);
	$query = sprintf("SELECT CONCAT(\"distributed_\", IF(type=\"temporary\", \"temp_\", \"perm_\"), id) AS index_name,\n" .
		"start, end\n" .
		"FROM %s.indexes\n" .
		"WHERE (start <= ? AND end > ?)\n" .
		"OR (start BETWEEN ? AND ?)\n" .
		"OR (end BETWEEN ? AND ?)", $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($start, $end, 
		$start, $end, 
		$start, $end);
	$self->stats->mark('local_get_indexes', 1);
	return $sth->fetchall_hashref('index_name');
}

sub resolve {
	my $self = shift;
	my $raw_field = shift;
	my $raw_value = shift;
	my $operator = shift;
	
	# Return all possible class_id, real_field, real_value combos
	$self->log->debug("resolving: raw_field: $raw_field, raw_value: $raw_value, operator: $operator");
	
	my %values = ( fields => {}, attrs => {} );
	# Find all possible real fields/classes for this raw field
	my $field_infos = $self->get_field($raw_field);
	foreach my $class_id (keys %{$field_infos}){
		if (scalar keys %{ $self->{_GIVEN_CLASSES} } and not $self->{_GIVEN_CLASSES}->{0}){
			unless ($self->{_GIVEN_CLASSES}->{$class_id} or $class_id == 0){
				$self->log->debug("Skipping class $class_id because it was not given");
				next;
			}
		}
		# we don't want to count class_id 0 as "distinct"
		if ($class_id){
			$self->{_DISTINCT_CLASSES}->{$class_id} = 1;
		}
		
		my $field_order = $field_infos->{$class_id}->{field_order};
		if ($ELSA::Field_order_to_field->{ $field_order }
			and ($operator eq '=' or $operator eq '-' or $operator eq '')){
			$values{fields}->{$class_id}->{ $ELSA::Field_order_to_field->{ $field_order } } =
					[ $self->normalize_value($class_id, $raw_value, $field_order) ];
		}
		elsif ($ELSA::Field_order_to_attr->{ $field_order }){
			$values{attrs}->{$class_id}->{ $ELSA::Field_order_to_attr->{ $field_order } } =
				[ $self->normalize_value($class_id, $raw_value, $field_order) ];			
		}
		else {
			$self->log->warn("Unknown field: $raw_field");
		}
	}
	return \%values;
}

sub normalize_value {
	my $self = shift;
	my $class_id = shift;
	my $value = shift;
	my $field_order = shift;
	
	unless (defined $class_id and defined $value and defined $field_order){
		$self->log->error('Missing an arg: ' . $class_id . ', ' . $value . ', ' . $field_order);
		return $value;
	}
	$self->log->debug("normalizing for class_id $class_id with the following: " . Dumper($self->{_FIELD_CONVERSIONS}->{ $class_id }));
	return $value unless $self->{_FIELD_CONVERSIONS}->{ $class_id };
	
	if ($field_order == ELSA::FIELD_HOST){ #host is handled specially
		my @ret;
		if ($value =~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/) {
			$self->log->debug('converting host ' . $value. ' to inet_aton');
			@ret = ( unpack('N*', inet_aton($value)) ); 
		}
		elsif ($value =~ /^[a-zA-Z0-9\-\.]+$/){
			my $host_to_resolve = $value;
			unless ($value =~ /\./){
				my $fqdn_hostname = Sys::Hostname::FQDN::fqdn();
				$fqdn_hostname =~ /^[^\.]+\.(.+)/;
				my $domain = $1;
				$self->log->debug('non-fqdn given, assuming to be domain: ' . $domain);
				$host_to_resolve .= '.' . $domain;
			}
			$self->log->debug('resolving and converting host ' . $host_to_resolve. ' to inet_aton');
			my $res   = Net::DNS::Resolver->new;
			my $query = $res->search($host_to_resolve);
			if ($query){
				my @ips;
				foreach my $rr ($query->answer){
					next unless $rr->type eq "A";
					$self->log->debug('resolved host ' . $host_to_resolve . ' to ' . $rr->address);
					push @ips, $rr->address;
				}
				if (scalar @ips){
					foreach my $ip (@ips){
						my $ip_int = unpack('N*', inet_aton($ip));
						push @ret, $ip_int;
					}
				}
				else {
					throw_e error => 'Unable to resolve host ' . $host_to_resolve . ': ' . $res->errorstring;
				}
			}
			else {
				throw_e error => 'Unable to resolve host ' . $host_to_resolve . ': ' . $res->errorstring;
			}
		}
		else {
			throw_e error => 'Invalid host given: ' . $value;
		}
		if (wantarray){
			return @ret;
		}
		else {
			return $ret[0];
		}
	}
	elsif ($self->{_FIELD_CONVERSIONS}->{ $class_id }->{'IPv4'}
		and $self->{_FIELD_CONVERSIONS}->{ $class_id }->{'IPv4'}->{$field_order}){
		return unpack('N', inet_aton($value));
	}
	elsif ($self->{_FIELD_CONVERSIONS}->{ $class_id }->{PROTO} 
		and $self->{_FIELD_CONVERSIONS}->{ $class_id }->{PROTO}->{$field_order}){
		$self->log->debug("Converting $value to proto");
		return $ELSA::Proto_map->{ $value };
	}
	elsif ($self->{_FIELD_CONVERSIONS}->{ $class_id }->{COUNTRY_CODE} 
		and $self->{_FIELD_CONVERSIONS}->{ $class_id }->{COUNTRY_CODE}->{$field_order}){
		$self->log->debug("Converting $value to country_code");
		return join('', unpack('c*', pack('A*', $value)));
	}
	elsif ($ELSA::Field_order_to_attr->{$field_order}
		and $self->{_ATTR_CONVERSIONS}->{ $ELSA::Field_order_to_attr->{$field_order} } 
		and $self->{_ATTR_CONVERSIONS}->{ $ELSA::Field_order_to_attr->{$field_order} }->{$value}){
		$self->log->debug("Converting $value to attr");
		return $self->{_ATTR_CONVERSIONS}->{ $ELSA::Field_order_to_attr->{$field_order} }->{$value};
	}
	else {
		#apparently we don't know about any conversions
		$self->log->debug("No conversion for $value and class_id $class_id, field_order $field_order.");
		return $value; 
	}
}

# Opposite of normalize
sub resolve_value {
	my $self = shift;
	my $class_id = shift;
	my $value = shift;
	my $field_order = shift;
			
	if ($self->{_FIELD_CONVERSIONS}->{ $class_id }->{'IPv4'}->{$field_order}){
		#$self->log->debug("Converting $value from IPv4");
		return inet_ntoa(pack('N', $value));
	}
	elsif ($self->{_FIELD_CONVERSIONS}->{ $class_id }->{PROTO}->{$field_order}){
		#$self->log->debug("Converting $value from proto");
		return $ELSA::Inverse_proto_map->{ $value };
	}
	elsif ($self->{_FIELD_CONVERSIONS}->{ $class_id }->{COUNTRY_CODE} 
		and $self->{_FIELD_CONVERSIONS}->{ $class_id }->{COUNTRY_CODE}->{$field_order}){
		my @arr = $value =~ /(\d{2})(\d{2})/;
		return unpack('A*', pack('c*', @arr));
	}
	else {
		#apparently we don't know about any conversions
		#$self->log->debug("No conversion for $value and class_id $class_id");
		return $value; 
	}
}

sub _parse_query_string {
	my $self = shift;
	my $raw_query = shift;
	my $stopwords = shift;
	
	# Attach the query filters for this user from permissions
	my $filtered_raw_query = $raw_query;
	if ($self->{_META_PARAMS}->{permissions}->{filter}){
		$filtered_raw_query .= ' ' . $self->{_META_PARAMS}->{permissions}->{filter};
	}
	
	# Check to see if the class was given in meta params
	if ($self->{_META_PARAMS}->{class}){
		$self->{_GIVEN_CLASSES}->{ sprintf("%d", $self->{_KNOWN_CLASSES}->{ $self->{_META_PARAMS}->{class} }) } = 1;
	}
	
	# If no class was given anywhere, see if we can divine it from a groupby or local_groupby
	if (not scalar keys %{ $self->{_GIVEN_CLASSES} }){
		if ($self->{_META_PARAMS}->{groupby}){
			foreach my $field (@{ $self->{_META_PARAMS}->{groupby} }){
				my $field_infos = $self->get_field($field);
				foreach my $class_id (keys %{$field_infos}){
					$self->{_GIVEN_CLASSES}->{$class_id} = 1;
				}
			}
		}
		elsif ($self->{_META_PARAMS}->{local_groupby}){
			foreach my $field (@{ $self->{_META_PARAMS}->{local_groupby} }){
				my $field_infos = $self->get_field($field);
				foreach my $class_id (keys %{$field_infos}){
					$self->{_GIVEN_CLASSES}->{$class_id} = 1;
				}
			}
		}
	}
	
	
	# Check for meta limit
	if ($self->{_META_PARAMS}->{limit}){
		$self->{_LIMIT} = sprintf("%d", $self->{_META_PARAMS}->{limit});
		$self->log->debug("Set limit " . $self->{_LIMIT});
	}
	
	$self->{_FIELD_TERMS} = {
		'or' => {},
		'and' => {},
		'not' => {},
	};
	
	$self->{_ANY_FIELD_TERMS} = {
		'or' => [],
		'and' => [],
		'not' => [],
	};
	
	$self->{_ATTR_TERMS} = {
		'and' => {},
		'not' => {},
		'range_and' => {},
		'range_not' => {},
	};
	
	$self->{_CLASSES} = $self->get_classes();
	$self->{_DISTINCT_CLASSES} = {};
	
	if ($raw_query =~ /\S/){ # could be meta_attr-only
		my $qp = new Search::QueryParser(rxTerm => qr/[^\s()]+/, rxField => qr/[\w,\.]+/);
		my $orig_parsed_query = $qp->parse($filtered_raw_query, $Implicit_plus) or throw_e error => $qp->err;
		$self->log->debug("orig_parsed_query: " . Dumper($orig_parsed_query));
		
		my $parsed_query = dclone($orig_parsed_query); #dclone so recursion doesn't mess up original
		
		# Recursively parse the query terms
		$self->_parse_query_term($parsed_query);
	}
	else {
		throw_e error => 'No query terms given';
	}
	
	# Determine if there are any other search fields.  If there are, then use host as a filter.
	$self->log->debug('_FIELD_TERMS: ' . Dumper($self->{_FIELD_TERMS}));
	my $host_is_filter = 0;
	foreach my $boolean qw(and or){
		foreach my $class_id (keys %{ $self->{_FIELD_TERMS}->{$boolean} }){
			next unless $class_id;
			$host_is_filter++;
		}
		foreach my $term (@{ $self->{_ANY_FIELD_TERMS}->{$boolean} }){
			$host_is_filter++;
		}
	}
	if ($host_is_filter){
		$self->log->debug('Using host as a filter because there were ' . $host_is_filter . ' query terms.');
		$self->log->debug('$self->{_FIELD_TERMS} before adjustment: ' . Dumper($self->{_FIELD_TERMS}));
		foreach my $boolean qw(or and not){
			next unless $self->{_FIELD_TERMS}->{$boolean} 
				and $self->{_FIELD_TERMS}->{$boolean}->{0} 
				and $self->{_FIELD_TERMS}->{$boolean}->{0}->{host};
			# OR doesn't make sense as an attr filter, change to and
			my $attr_boolean = $boolean;
			if ($attr_boolean eq 'or'){
				$attr_boolean = 'and';
			}
			$self->{_ATTR_TERMS}->{$attr_boolean} ||= {};
			$self->{_ATTR_TERMS}->{$attr_boolean}->{0} ||= {};
			$self->{_ATTR_TERMS}->{$attr_boolean}->{0}->{host_id} = { map { $_ => 1 } @{ delete $self->{_FIELD_TERMS}->{$boolean}->{0}->{host} } };
			$self->log->debug('swapping host_id field terms to be attr terms for boolean ' . $boolean);
		}
	}

	$self->log->debug('attr before conversion: ' . Dumper($self->{_ATTR_TERMS}));
	# convert the ranges found in the query string from hash to array.  there can be only one range per attr in the query terms.
	foreach my $boolean qw(range_and range_not){
		foreach my $class_id (keys %{ $self->{_ATTR_TERMS}->{$boolean} }){
			foreach my $attr (keys %{ $self->{_ATTR_TERMS}->{$boolean}->{$class_id} }){
				$self->{_ATTR_TERMS}->{$boolean}->{$class_id}->{$attr} =  [
					{ 
						attr => $attr, 
						min => $self->{_ATTR_TERMS}->{$boolean}->{$class_id}->{$attr}->{min},
						max => $self->{_ATTR_TERMS}->{$boolean}->{$class_id}->{$attr}->{max},
						exclude => $boolean eq 'range_and' ? 0 : 1,
					}
				];
			}
		}
	}
	
	# Check for blanket allow on classes
	if ($self->{_META_PARAMS}->{permissions}->{class_id}->{0}){
		$self->log->debug('User has access to all classes');
		$self->{_PERMITTED_CLASSES} = $self->{_CLASSES};
	}
	else {
		$self->{_PERMITTED_CLASSES} = { %{ $self->{_META_PARAMS}->{permissions}->{class_id} } };
		# Drop any query terms that wanted to use an unpermitted class
		foreach my $item qw(_FIELD_TERMS _ATTR_TERMS){
			foreach my $boolean qw(and or not range_and range_not){
				foreach my $class_id (keys %{ $self->{$item}->{$boolean} }){
					next if $class_id eq 0; # this is handled specially below
					unless ($self->{_PERMITTED_CLASSES}->{$class_id}){
						my $forbidden = delete $self->{$item}->{$boolean}->{$class_id};
						$self->log->warn('Forbidding ' . $item . ' from class_id ' . $class_id . ' with ' . Dumper($forbidden));
					}
				}
			}
		}
	}
	
	# Adjust classes if necessary
	$self->log->debug('_GIVEN_CLASSES before adjustments: ' . Dumper($self->{_GIVEN_CLASSES}));
	if (scalar keys %{ $self->{_GIVEN_CLASSES} } == 1 and $self->{_GIVEN_CLASSES}->{0}){
		$self->{_DISTINCT_CLASSES} = $self->{_PERMITTED_CLASSES};
	}
	elsif (scalar keys %{ $self->{_GIVEN_CLASSES} }){ #if 0 (meaning any) is given, go with permitted classes
		$self->{_DISTINCT_CLASSES} = {};
		foreach my $key (keys %{ $self->{_GIVEN_CLASSES} }){
			if ($self->{_PERMITTED_CLASSES}->{$key}){
				$self->{_DISTINCT_CLASSES}->{$key} = 1;
			}
		}
	}
	elsif (scalar keys %{ $self->{_DISTINCT_CLASSES} }) {
		foreach my $key (keys %{ $self->{_DISTINCT_CLASSES} }){
			unless ($self->{_PERMITTED_CLASSES}->{$key}){
				delete $self->{_DISTINCT_CLASSES}->{$key};
			}
		}
	}
	else {
		$self->{_DISTINCT_CLASSES} = $self->{_PERMITTED_CLASSES};
	}
	$self->log->debug('_DISTINCT_CLASSES after adjustments: ' . Dumper($self->{_DISTINCT_CLASSES}));
	
	# Find all field names in the AND
	my %required_fields;
	$self->log->debug('_FIELD_TERMS and: ' . Dumper($self->{_FIELD_TERMS}->{and}));
	foreach my $class_id (keys %{ $self->{_FIELD_TERMS}->{and} }){
		my $fields_by_order = $self->get_fields_arr_by_order($class_id);
		$self->log->debug('class_id: ' . $class_id);
		foreach my $raw_field (keys %{ $self->{_FIELD_TERMS}->{and}->{$class_id} }){
			my $field_order = $ELSA::Field_to_order->{ $raw_field };
			$self->log->debug('field_order: ' . $field_order);
			my $ordered_field_hash = $fields_by_order->{ $field_order };
			$self->log->debug('ordered_field_hash: ' . Dumper($ordered_field_hash));
			my $field = $ordered_field_hash->{value};
			$self->log->debug('_FIELD_TERMS boolean:and, class_id: ' . $class_id . ' raw_field:' . $raw_field . ', field: ' . $field);
			next unless $field;
			$required_fields{ $field } = 1;
		}
	}
	foreach my $boolean qw(and range_and){
		$self->log->debug('_ATTR_TERMS ' . $boolean . ': ' . Dumper($self->{_ATTR_TERMS}->{$boolean}));
		foreach my $class_id (keys %{ $self->{_ATTR_TERMS}->{$boolean} }){
			$self->log->debug('class_id: ' . $class_id);
			my $fields_by_order = $self->get_fields_arr_by_order($class_id);
			$self->log->debug('fields_by_order: ' . Dumper($fields_by_order));
			foreach my $raw_field (keys %{ $self->{_ATTR_TERMS}->{$boolean}->{$class_id} }){
				$self->log->debug('raw_field: ' . $raw_field);
				$raw_field =~ s/^attr\_//g; #strip off the attr_ to get the actual field name
				my $field_order = $ELSA::Field_to_order->{ $raw_field };
				$self->log->debug('field_order: ' . $field_order);
				my $ordered_field_hash = $fields_by_order->{ $field_order };
				$self->log->debug('ordered_field_hash: ' . Dumper($ordered_field_hash));
				my $field = $ordered_field_hash->{value};
				$self->log->debug('_ATTR_TERMS boolean:' . $boolean . ', class_id: ' . $class_id . ' raw_field:' . $raw_field . ', field: ' . $field);
				next unless $field;
				$required_fields{ $field } = 1;
			}
		}
	}
	$self->log->debug('required_fields: ' . Dumper(\%required_fields));
	# Remove any classes that won't provide the field needed from the query
	my $fields = $self->get_fields_by_name();
	
	foreach my $candidate_class_id (keys %{ $self->{_DISTINCT_CLASSES} }){
		foreach my $required_field (keys %required_fields){
			$self->log->debug('checking for required field: ' . $required_field);
			my $found = 0;
			foreach my $row (@{ $fields->{$required_field} }){
				if ($row->{class_id} eq $candidate_class_id){
					$self->log->debug('found required_field ' . $required_field . ' in class_id ' . $candidate_class_id . ' at row: ' . Dumper($row));
					$found = 1;
					last;
				}
				elsif ($row->{class_id} == 0){
					$self->log->debug('required_field ' . $required_field . ' is a meta attr and exists in all classes');
					$found = 1;
					last;
				}
			}
			unless ($found){
				$self->log->debug('removing class_id ' . $candidate_class_id);
				#delete $self->{_ATTR_TERMS}->{$boolean}->{$class_id};
				delete $self->{_DISTINCT_CLASSES}->{$candidate_class_id};
			}
		}
	}		
	
	if (scalar keys %{ $self->{_EXCLUDED_CLASSES} }){
		foreach my $class_id (keys %{ $self->{_EXCLUDED_CLASSES} }){
			$self->log->debug("Excluding class_id $class_id");
			delete $self->{_DISTINCT_CLASSES}->{$class_id};
		}
	}
	
	$self->log->debug('ATTR_TERMS: ' . Dumper($self->{_ATTR_TERMS}));
	
	my $num_removed_terms = 0;
	
	# Adjust hosts/programs based on permissions
	foreach my $attr qw(host_id program_id){
		# Do we have a blanket allow permission?
		if ($self->{_META_PARAMS}->{permissions}->{$attr}->{0}){
			$self->log->debug('Permissions grant access to any ' . $attr);
			next;
		}
		else {
			# Need to only allow access to the whitelist in permissions
			
			# Add filters for the whitelisted items
			# If there are no exceptions to the whitelist, no query will succeed
			if (not scalar keys %{ $self->{_META_PARAMS}->{permissions}->{$attr} }){
				throw_e error => 'Insufficient privileges for querying any ' . $attr; 
			}
			
			# Remove items not explicitly whitelisted
			foreach my $boolean qw(and or range_and){
				next unless $self->{_ATTR_TERMS}->{$boolean} 
					and $self->{_ATTR_TERMS}->{$boolean}->{0} 
					and $self->{_ATTR_TERMS}->{$boolean}->{0}->{$attr};
				$self->log->debug('boolean: ' . Dumper($self->{_ATTR_TERMS}->{$boolean}));
				for (my $i = 0; $i < scalar @{ $self->{_ATTR_TERMS}->{$boolean}->{0}->{$attr} }; $i++){
					my $id = $self->{_ATTR_TERMS}->{$boolean}->{0}->{$attr}->[$i];
					unless ($self->{_META_PARAMS}->{permissions}->{$attr}->{$id}){
						$self->log->warn("Excluding id $id from $attr based on permissions");
						my $deleted = delete $self->{_ATTR_TERMS}->{$boolean}->{0}->{$attr}->[$i];
						$self->log->debug('deleted: ' . $deleted);
						#$self->{_ATTR_TERMS}->{not}->{0}->{$attr}->[$i] = $deleted;
						$num_removed_terms++;
					}
				}
			}
			# Add allowed items to filter
			foreach my $id (keys %{ $self->{_META_PARAMS}->{permissions}->{$attr} }){
				$self->log->debug("Adding id $id to $attr based on permissions");
				# Deal with ranges
				if ($self->{_META_PARAMS}->{permissions}->{$attr}->{$id}){
					if ($self->{_META_PARAMS}->{permissions}->{$attr}->{$id} =~ /(\d+)\-(\d+)/){
						$self->{_ATTR_TERMS}->{range_and}->{0}->{$attr} ||= [];
						push @{ $self->{_ATTR_TERMS}->{range_and}->{0}->{$attr} }, { attr => $attr, min => $1, max => $2, exclude => 0 };
					}
				}
				push @{ $self->{_ATTR_TERMS}->{and}->{0}->{$attr} }, $id;
			}
		}
	}
	
	# One-off for dealing with hosts as fields
	foreach my $boolean qw(and or not){
		if ($self->{_FIELD_TERMS}->{$boolean}->{0} and $self->{_FIELD_TERMS}->{$boolean}->{0}->{host}){
			foreach my $host_int (@{ $self->{_FIELD_TERMS}->{$boolean}->{0}->{host} }){
				$self->log->debug('adding host_int ' . $host_int);
				push @{ $self->{_ANY_FIELD_TERMS}->{$boolean} }, '(@host ' . $host_int . ')';
			}
		}
	}
	
	# Check all field terms to see if they are a stopword and warn if necessary
	if ($stopwords and ref($stopwords) and ref($stopwords) eq 'HASH'){
		$self->log->debug('checking terms against ' . (scalar keys %$stopwords) . ' stopwords');
		foreach my $boolean qw(and or not){
			foreach my $class_id (keys %{ $self->{_FIELD_TERMS}->{$boolean} }){
				my $fields_by_order = $self->get_fields_arr_by_order($class_id);
				foreach my $raw_field (keys %{ $self->{_FIELD_TERMS}->{$boolean}->{$class_id} }){
					next unless $self->{_FIELD_TERMS}->{$boolean}->{$class_id}->{$raw_field};
					for (my $i = 0; $i < (scalar @{ $self->{_FIELD_TERMS}->{$boolean}->{$class_id}->{$raw_field} }); $i++){
						my $term = $self->{_FIELD_TERMS}->{$boolean}->{$class_id}->{$raw_field}->[$i];
						if ($stopwords->{$term}){
							my $err = 'Removed term ' . $term . ' which is too common';
							push @{ $self->{_WARNINGS} }, $err;
							$self->log->warn($err);
							$num_removed_terms++;
							# Drop the term
							if (scalar @{ $self->{_FIELD_TERMS}->{$boolean}->{$class_id}->{$raw_field} } == 1){
								delete $self->{_FIELD_TERMS}->{$boolean}->{$class_id}->{$raw_field};
								last;
							}
							else {
								splice(@{ $self->{_FIELD_TERMS}->{$boolean}->{$class_id}->{$raw_field} }, $i, 1);
							}
						}
					}
				}
			}
		}
	}
	
	$self->log->debug("_FIELD_TERMS: " . Dumper($self->{_FIELD_TERMS}));
	$self->log->debug("_ANY_FIELD_TERMS: " . Dumper($self->{_ANY_FIELD_TERMS}));
	$self->log->debug("_ATTR_TERMS: " . Dumper($self->{_ATTR_TERMS}));
	$self->log->debug("PERMITTED_CLASSES: " . Dumper($self->{_PERMITTED_CLASSES}));
	$self->log->debug("GIVEN_CLASSES: " . Dumper($self->{_GIVEN_CLASSES}));
	$self->log->debug("DISTINCT_CLASSES: " . Dumper($self->{_DISTINCT_CLASSES}));
	
	# Verify that we're still going to actually have query terms after the filtering has taken place	
	my $query_term_count = 0;
	if (scalar keys %{ $self->{_DISTINCT_CLASSES} }){
		foreach my $item qw(_FIELD_TERMS _ATTR_TERMS){
			foreach my $boolean qw(and or not range_and range_not){
				next unless $self->{$item}->{$boolean};
				foreach my $class_id (keys %{ $self->{$item}->{$boolean} }){
					$query_term_count += scalar keys %{ $self->{$item}->{$boolean}->{$class_id} };
				}
			}
		}
	}
	foreach my $boolean qw(or and){
		$query_term_count += scalar @{ $self->{_ANY_FIELD_TERMS}->{$boolean} }; 
	}
	
	# we might have a class-only query
	foreach my $class (keys %{ $self->{_DISTINCT_CLASSES} }){
		unless ($num_removed_terms){ # this query used to have terms, so it wasn't really class-only
			$query_term_count++;
		}
	}
	
	unless ($query_term_count){
		my $e = 'All query terms were stripped based on permissions or they were too common';
		$self->log->error($e);
		throw_e error => $e;
	}
	
	if ($self->{_META_PARAMS}->{start}){
		$self->{_START_INT} = sprintf('%d', $self->{_META_PARAMS}->{start});
	}
	if ($self->{_META_PARAMS}->{end}){
		$self->{_END_INT} =sprintf('%d', $self->{_META_PARAMS}->{end});
	}
	$self->log->debug('META_PARAMS: ' . Dumper($self->{_META_PARAMS}));
	
	# Adjust query time params as necessary
	if ($self->{_META_PARAMS}->{adjust_query_times}){
		my $min_max_times = $self->get_min_max_indexes();
		if ($self->{_START_INT} < $min_max_times->{start_int}){
			$self->{_START_INT} = $min_max_times->{start_int};
			$self->log->warn("Given start time too early, adjusting to " 
				. ELSA::epoch2iso($self->{_START_INT}));
		}
		elsif ($self->{_START_INT} > $min_max_times->{end_int}){
			$self->{_START_INT} = $min_max_times->{end_int} - $self->conf->get('sphinx/index_interval');
			$self->log->warn("Given start time too late, adjusting to " 
				. ELSA::epoch2iso($self->{_START_INT}));
		}
		# If no end given, default to max
		if (not $self->{_END_INT}){
			$self->{_END_INT} = $min_max_times->{end_int};
		}
		elsif ($self->{_END_INT} > $min_max_times->{end_int}){
			$self->{_END_INT} = $min_max_times->{end_int};
			$self->log->warn("Given end time too late, adjusting to " 
				. ELSA::epoch2iso($self->{_END_INT}));
		}
		elsif ($self->{_END_INT} < $min_max_times->{start_int}){
			$self->{_END_INT} = $min_max_times->{start_int} + $self->conf->get('sphinx/index_interval');
			$self->log->warn("Given end time too early, adjusting to " 
				. ELSA::epoch2iso($self->{_END_INT}));
		}
	}
	
	# Failsafe for times
	if ($self->{_META_PARAMS}->{start} or $self->{_META_PARAMS}->{end}){
		unless ($self->{_START_INT}){
			$self->{_START_INT} = 0;
		}
		unless ($self->{_END_INT}){
			$self->{_END_INT} = time();
		}
	}
	
	#TODO when we are using string attributes
#	# Find all unique fields in the classes in the query
#	my %uniq_fields;
#	foreach my $class_id (keys %{ $self->{_DISTINCT_CLASSES} }){
#		foreach my $field (keys %$fields){
#			foreach my $field_hash (@{ $fields->{$field} }){
#				if ($field_hash->{class_id} eq $class_id){
#					$uniq_fields{$field} = 1;
#				}
#			}
#		}
#	}
#
#	$self->{_META_PARAMS}->{groupby} = [ keys %uniq_fields ];
#	$self->log->debug('Grouping on these fields: ' . Dumper($self->{_META_PARAMS}->{groupby}));
	
	return 1;
}

sub _parse_query_term {
	my $self = shift;
	my $terms = shift;
	
	$self->log->debug('terms: ' . Dumper($terms));
	
	my $min_val = 0;
	my $max_val = 2**32 - 1; #uint
			
	foreach my $operator (keys %{$terms}){
		my $arr = $terms->{$operator};
		foreach my $term_hash (@{$arr}){
			next unless defined $term_hash->{value};
			
			# Recursively handle parenthetical directives
			if (ref($term_hash->{value}) eq 'HASH'){
				$self->_parse_query_term($term_hash->{value});
				next;
			}
			
			# Escape any digit-dash-word combos (except for host or program)
			$term_hash->{value} =~ s/(\d+)\-/$1\\\-/g unless ($term_hash->{field} eq 'program' or $term_hash->{field} eq 'host');
			
			# Get rid of any non-indexed chars
			$term_hash->{value} =~ s/[^a-zA-Z0-9\.\@\-\_\\]/\ /g;
			
			# Escape any '@' or sphinx will error out thinking it's a field prefix
			$term_hash->{value} =~ s/\@/\\\@/g;
			
			# Sphinx can only handle numbers up to 15 places
			if ($term_hash->{value} =~ /^[0-9]+$/ and  length($term_hash->{value}) > 15){
				throw_e error => 'Integer search terms must be 15 or fewer digits, received ' 
					. $term_hash->{value} . ' which is ' .  length($term_hash->{value}) . ' digits.';
			}
			
			
			if ($term_hash->{field} eq 'start'){
				# special case for start/end
				$self->{_START_INT} = UnixDate($term_hash->{value}, "%s");
				$self->log->debug("START: " . $self->{_START_INT});
				next;
			}
			elsif ($term_hash->{field} eq 'end'){
				# special case for start/end
				$self->{_END_INT} = UnixDate($term_hash->{value}, "%s");
				$self->log->debug("END: " . $self->{_END_INT});
				next;
			}
			elsif ($term_hash->{field} eq 'limit'){
				# special case for limit
				$self->{_LIMIT} = sprintf("%d", $term_hash->{value});
				$self->log->debug("Set limit " . $self->{_LIMIT});
				next;
			}
			elsif ($term_hash->{field} eq 'offset'){
				# special case for offset
				$self->{_OFFSET} = sprintf("%d", $term_hash->{value});
				$self->log->debug("Set offset " . $self->{_OFFSET});
				next;
			}
			elsif ($term_hash->{field} eq 'class'){
				# special case for class
				my $class;
				if ($self->{_KNOWN_CLASSES}->{ $term_hash->{value} }){
					$class = $self->{_KNOWN_CLASSES}->{ $term_hash->{value} };
				}
				elsif ($self->{_KNOWN_CLASSES}->{ uc($term_hash->{value}) }){
					$class = $self->{_KNOWN_CLASSES}->{ uc($term_hash->{value}) };
				}
				elsif ($self->{_KNOWN_CLASSES}->{ lc($term_hash->{value}) }){
					$class = $self->{_KNOWN_CLASSES}->{ lc($term_hash->{value}) };
				}
				else {
					throw_e error => "Unknown class $term_hash->{value}";
					#$self->log->error("Unknown class $term_hash->{value}");
				}
				
				if ($operator eq '-'){
					# We're explicitly removing this class
					$self->{_EXCLUDED_CLASSES}->{ $class } = 1;
				}
				else {
					$self->{_GIVEN_CLASSES}->{ $class } = 1;
				}
				$self->log->debug("Set operator $operator for given class " . $term_hash->{value});	
			
				
				next;
			}
			
			# Process a field/value or attr/value
			if ($term_hash->{field} and $term_hash->{value}){
				
				my $operators = {
					'>' => 1,
					'>=' => 1,
					'<' => 1,
					'<=' => 1,
					'!=' => 1, 
				};
				# Default unknown operators to AND
				unless ($operators->{ $term_hash->{op} }){
					$term_hash->{op} = '=';
				}
				
				my $values = $self->resolve(
					$term_hash->{field}, 
					$term_hash->{value}, 
					$term_hash->{op}
				);
								
				if ($operator eq '-'){
					if ($term_hash->{op} eq '='){
						foreach my $class_id (keys %{ $values->{fields} }){
							foreach my $real_field (keys %{ $values->{fields}->{$class_id} }){
								$self->{_FIELD_TERMS}->{not}->{$class_id}->{$real_field} ||= [];
								push @{ $self->{_FIELD_TERMS}->{not}->{$class_id}->{$real_field} }, 
									@{ $values->{fields}->{$class_id}->{$real_field} };
							}	
						}
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								$self->{_ATTR_TERMS}->{not}->{$class_id}->{$real_field} ||= [];
								push @{ $self->{_ATTR_TERMS}->{not}->{$class_id}->{$real_field} },
									@{ $values->{attrs}->{$class_id}->{$real_field} };
							}
						}
					}
					elsif ($term_hash->{op} eq '<' or $term_hash->{op} eq '<='){
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								unless ($self->{_ATTR_TERMS}->{range_not}->{$class_id}->{$real_field}){
									$self->{_ATTR_TERMS}->{range_not}->{$class_id}->{$real_field} = { min => $min_val, max => $max_val };
								}
								$self->{_ATTR_TERMS}->{range_not}->{$class_id}->{$real_field}->{max} = $values->{attrs}->{$class_id}->{$real_field}->[0];
							}
						};
						
					}
					elsif ($term_hash->{op} eq '>' or $term_hash->{op} eq '>='){
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								unless ($self->{_ATTR_TERMS}->{range_not}->{$class_id}->{$real_field}){
									$self->{_ATTR_TERMS}->{range_not}->{$class_id}->{$real_field} = { min => $min_val, max => $max_val };
								}
								$self->{_ATTR_TERMS}->{range_not}->{$class_id}->{$real_field}->{min} = $values->{attrs}->{$class_id}->{$real_field}->[0];
							}
						};
					}
					else {
						# Only thing left is '!=' which in this context is a double-negative
						foreach my $class_id (keys %{ $values->{fields} }){
							foreach my $real_field (keys %{ $values->{fields}->{$class_id} }){
								if ($self->{_FIELD_TERMS}->{and}->{$class_id}->{$real_field}){
									 push @{ $self->{_FIELD_TERMS}->{and}->{$class_id}->{$real_field} }, @{ $values->{fields}->{$class_id}->{$real_field} };
								}
								else {
									$self->{_FIELD_TERMS}->{and}->{$class_id}->{$real_field} = [ @{ $values->{fields}->{$class_id}->{$real_field} } ];
								}
							}	
						}
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								if ($self->{_ATTR_TERMS}->{and}->{$class_id}->{$real_field}){
									 push @{ $self->{_ATTR_TERMS}->{and}->{$class_id}->{$real_field} }, @{ $values->{attrs}->{$class_id}->{$real_field} };
								}
								else {
									$self->{_ATTR_TERMS}->{and}->{$class_id}->{$real_field} = [ @{ $values->{attrs}->{$class_id}->{$real_field} } ];
								}
							}
						}
					}
				}
				elsif ($operator eq '+') {
					if ($term_hash->{op} eq '='){
						foreach my $class_id (keys %{ $values->{fields} }){
							foreach my $real_field (keys %{ $values->{fields}->{$class_id} }){
								if ($self->{_FIELD_TERMS}->{and}->{$class_id}->{$real_field}){
									 push @{ $self->{_FIELD_TERMS}->{and}->{$class_id}->{$real_field} }, @{ $values->{fields}->{$class_id}->{$real_field} };
								}
								else {
									$self->{_FIELD_TERMS}->{and}->{$class_id}->{$real_field} = [ @{ $values->{fields}->{$class_id}->{$real_field} } ];
								}
							}	
						}
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								if ($self->{_ATTR_TERMS}->{and}->{$class_id}->{$real_field}){
									 push @{ $self->{_ATTR_TERMS}->{and}->{$class_id}->{$real_field} }, @{ $values->{attrs}->{$class_id}->{$real_field} };
								}
								else {
									$self->{_ATTR_TERMS}->{and}->{$class_id}->{$real_field} = [ @{ $values->{attrs}->{$class_id}->{$real_field} } ];
								}
							}
						}
					}
					elsif ($term_hash->{op} eq '<' or $term_hash->{op} eq '<='){
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								unless ($self->{_ATTR_TERMS}->{range_and}->{$class_id}->{$real_field}){
									$self->{_ATTR_TERMS}->{range_and}->{$class_id}->{$real_field} = { min => $min_val, max => $max_val };
								}
								$self->{_ATTR_TERMS}->{range_and}->{$class_id}->{$real_field}->{max} = $values->{attrs}->{$class_id}->{$real_field}->[0];
							}
						}
					}
					elsif ($term_hash->{op} eq '>' or $term_hash->{op} eq '>='){
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								unless ($self->{_ATTR_TERMS}->{range_and}->{$class_id}->{$real_field}){
									$self->{_ATTR_TERMS}->{range_and}->{$class_id}->{$real_field} = { min => $min_val, max => $max_val };
								}
								$self->{_ATTR_TERMS}->{range_and}->{$class_id}->{$real_field}->{min} = $values->{attrs}->{$class_id}->{$real_field}->[0];
							}
						}
					}
					else {
						# Only thing left is '!='
						foreach my $class_id (keys %{ $values->{fields} }){
							foreach my $real_field (keys %{ $values->{fields}->{$class_id} }){
								if ($self->{_FIELD_TERMS}->{not}->{$class_id}->{$real_field}){
									 push @{ $self->{_FIELD_TERMS}->{not}->{$class_id}->{$real_field} }, @{ $values->{fields}->{$class_id}->{$real_field} };
								}
								else {
									$self->{_FIELD_TERMS}->{not}->{$class_id}->{$real_field} = [ @{ $values->{fields}->{$class_id}->{$real_field} } ];
								}
							}	
						}
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								if ($self->{_ATTR_TERMS}->{not}->{$class_id}->{$real_field}){
									 push @{ $self->{_ATTR_TERMS}->{not}->{$class_id}->{$real_field} }, @{ $values->{attrs}->{$class_id}->{$real_field} };
								}
								else {
									$self->{_ATTR_TERMS}->{not}->{$class_id}->{$real_field} = [ @{ $values->{attrs}->{$class_id}->{$real_field} } ];
								}
							}
						}
					}
				}
				else { #OR
					if ($term_hash->{op} eq '='){
						foreach my $class_id (keys %{ $values->{fields} }){
							foreach my $real_field (keys %{ $values->{fields}->{$class_id} }){
								if ($self->{_FIELD_TERMS}->{or}->{$class_id}->{$real_field}){
									 push @{ $self->{_FIELD_TERMS}->{or}->{$class_id}->{$real_field} }, @{ $values->{fields}->{$class_id}->{$real_field} };
								}
								else {
									$self->{_FIELD_TERMS}->{or}->{$class_id}->{$real_field} = [ @{ $values->{fields}->{$class_id}->{$real_field} } ];
								}
							}	
						}
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								$self->log->warn("OR on attr $real_field is impossible, converting to AND");
								if ($self->{_ATTR_TERMS}->{and}->{$class_id}->{$real_field}){
									 push @{ $self->{_ATTR_TERMS}->{and}->{$class_id}->{$real_field} }, @{ $values->{attrs}->{$class_id}->{$real_field} };
								}
								else {
									$self->{_ATTR_TERMS}->{and}->{$class_id}->{$real_field} = [ @{ $values->{attrs}->{$class_id}->{$real_field} } ];
								}
							}
						}
					}
					elsif ($term_hash->{op} eq '<' or $term_hash->{op} eq '<='){
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								unless ($self->{_ATTR_TERMS}->{range_and}->{$class_id}->{$real_field}){
									$self->{_ATTR_TERMS}->{range_and}->{$class_id}->{$real_field} = { min => $min_val, max => $max_val };
								}
								$self->{_ATTR_TERMS}->{range_and}->{$class_id}->{$real_field}->{max} = $values->{attrs}->{$class_id}->{$real_field}->[0];
							}
						};
						
					}
					elsif ($term_hash->{op} eq '>' or $term_hash->{op} eq '>='){
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								unless ($self->{_ATTR_TERMS}->{range_and}->{$class_id}->{$real_field}){
									$self->{_ATTR_TERMS}->{range_and}->{$class_id}->{$real_field} = { min => $min_val, max => $max_val };
								}
								$self->{_ATTR_TERMS}->{range_and}->{$class_id}->{$real_field}->{min} = $values->{attrs}->{$class_id}->{$real_field}->[0];
							}
						};
					}
					else {
						# Only thing left is '!='
						foreach my $class_id (keys %{ $values->{fields} }){
							foreach my $real_field (keys %{ $values->{fields}->{$class_id} }){
								if ($self->{_FIELD_TERMS}->{not}->{$class_id}->{$real_field}){
									 push @{ $self->{_FIELD_TERMS}->{not}->{$class_id}->{$real_field} }, @{ $values->{fields}->{$class_id}->{$real_field} };
								}
								else {
									$self->{_FIELD_TERMS}->{not}->{$class_id}->{$real_field} = [ @{ $values->{fields}->{$class_id}->{$real_field} } ];
								}
							}	
						}
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								if ($self->{_ATTR_TERMS}->{not}->{$class_id}->{$real_field}){
									 push @{ $self->{_ATTR_TERMS}->{not}->{$class_id}->{$real_field} }, @{ $values->{attrs}->{$class_id}->{$real_field} };
								}
								else {
									$self->{_ATTR_TERMS}->{not}->{$class_id}->{$real_field} = [ @{ $values->{attrs}->{$class_id}->{$real_field} } ];
								}
							}
						}
					}
				}
			}
			# Otherwise there was no field given, search all fields
			elsif (defined $term_hash->{value}){
				if($term_hash->{quote}){
					$term_hash->{value} = $self->_normalize_quoted_value($term_hash->{value});
				}
				
				if ($operator eq '-'){
					push @{ $self->{_ANY_FIELD_TERMS}->{not} }, $term_hash->{value};
				}
				elsif ($operator eq '+'){
					push @{ $self->{_ANY_FIELD_TERMS}->{and} }, $term_hash->{value};
				}
				else {
					push @{ $self->{_ANY_FIELD_TERMS}->{or} }, $term_hash->{value};
				}
			}
			else {
				throw_e error => "no field or value given: " . Dumper($term_hash);
			}
		}
	}
	
	return 1;
}

sub _normalize_quoted_value {
	my $self = shift;
	my $value = shift;
	
	# Strip punctuation
	$value =~ s/[^a-zA-Z0-9\.\@\s\-]/\ /g;
	return '"' . $value . '"';
}

1;

__END__
