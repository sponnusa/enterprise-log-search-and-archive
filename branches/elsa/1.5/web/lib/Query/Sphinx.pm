package Query::Sphinx;
use Moose;
use Data::Dumper;
use SyncMysql;
use Time::HiRes qw(time);
use AnyEvent;
use Ouch qw(:trytiny);
use Socket;
use String::CRC32;
use Sys::Hostname::FQDN;
use Net::DNS;
extends 'Query';

has 'sphinx_db' => (is => 'rw', isa => 'HashRef');
has 'post_filters' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { { and => {}, not => {} } },
	handles => { has_postfilters => 'keys' });

sub BUILD {
	my $self = shift;
	
	# Map directives to their properties
	foreach my $prop (keys %{ $self->parser->directives }){
		$self->$prop($self->parser->directives->{$prop});
	}
	
	return $self;
}

sub estimate_query_time {
	my $self = shift;
	
	my $query_time = 0;
	
	# Do we have any stopwords?
	$query_time += ((scalar keys %{ $self->parser->stopword_terms }) * 10);
	
	# How many terms?
	$query_time += ($self->parser->index_term_count * 3);
	
#	# Do a query with a cutoff=1 to find the total number of docs to be filtered through and apply an estimate
#	my ($save_cutoff, $save_limit) = ($self->directives->{cutoff}, $self->directives->{limit});
#	$q->cutoff(1);
#	$q->limit(1);
#	$self->_sphinx_query($q);
#	
#	my $sphinx_filter_rows_per_second = 500_000; # guestimate of how many found hits/sec/node sphinx will filter
#	if ($self->conf->get('sphinx_filter_rows_per_second')){
#		$sphinx_filter_rows_per_second = $self->conf->get('sphinx_filter_rows_per_second');
#	}
#	
#	$self->log->trace('total_docs: ' . $q->results->total_docs);
#	$query_time = ($q->results->total_docs / $sphinx_filter_rows_per_second / (scalar keys %{ $q->node_info->{nodes} }));
#	
#	# Reset to original vals
#	$q->cutoff($save_cutoff);
#	$q->limit($save_limit);
	
	
	return $query_time;
}

sub _normalize_quoted_value {
	my $self = shift;
	my $value = shift;
	
	# Quoted integers don't work for some reason
	if ($value =~ /^[a-zA-Z0-9]+$/){
		return $value;
	}
	else {
		return '"' . $value . '"';
	}
}

sub _normalize_terms {
	my $self = shift;
	
	# Normalize query terms
	foreach my $boolean (keys %{ $self->terms }){
		foreach my $key (keys %{ $self->terms->{$boolean} }){
			my $term_hash = $self->terms->{$boolean}->{$key};
			# Get rid of any non-indexed chars
			$term_hash->{value} =~ s/[^a-zA-Z0-9\.\@\-\_\\]/\ /g;
			# Escape backslashes followed by a letter
			$term_hash->{value} =~ s/\\([a-zA-Z])/\ $1/g;
			#$term_hash->{value} =~ s/\\\\/\ /g; # sphinx doesn't do this for some reason
			# Escape any '@' or sphinx will error out thinking it's a field prefix
			if ($term_hash->{value} =~ /\@/ and not $term_hash->{quote}){
				# need to quote
				$term_hash->{value} = '"' . $term_hash->{value} . '"';
			}
			# Escape any hyphens
			$term_hash->{value} =~ s/\-/\\\\\-/g;
			
			if($term_hash->{quote}){
				$term_hash->{value} = $self->_normalize_quoted_value($term_hash->{value});
			}
			if ($term_hash->{value} =~ /^"?\s+"?$/){
				my $err = 'Term ' . $term_hash->{value} . ' was comprised of only non-indexed chars and removed';
				$self->add_warning(400, $err, { term => $term_hash->{value} });
				$self->log->warn($err);
				next;
			}
		}
	}
	
	return 1;
}

sub _build_queries {
	my $self = shift;
	my $index = shift;
	
	my @queries;
	# All OR's get factored out and become separate queries
	if (scalar keys %{ $self->terms->{or} }){
		foreach my $key (keys %{ $self->terms->{or} }){
			push @queries, @{ $self->_build_query($index, $key) };
		}
	}
	elsif (scalar keys %{ $self->terms->{and} }){
		push @queries, @{ $self->_build_query($index) };
	}
	else {
		throw(400, 'No positive value in query', { query => 0 });
	}
	
	return \@queries;
}

sub _build_query {
	my $self = shift;
	my $index = shift;
	my $or_key = shift;
	
	my $classes = $self->_get_class_ids($or_key);
	$self->log->debug('searching classes: ' . join(',', sort keys %$classes));
	my @queries;
	foreach my $class_id (sort keys %$classes){
		my $terms_and_filters = $self->_get_search_terms($class_id, $index, $or_key);
		$self->log->debug('terms_and_filters: ' . Dumper($terms_and_filters));
		my $match_str = $self->_get_match_str($class_id, $terms_and_filters->{searches});
		my $attr_str = $self->_get_attr_tests($class_id, $terms_and_filters->{filters});
		my $query = {
			select => $self->_get_select_clause($class_id, $attr_str),
			where => $self->_get_where_clause($class_id, $match_str),
			groupby => $self->_get_groupby_clause($class_id),
			orderby => $self->_get_orderby_clause($class_id),
		};
		$self->log->debug('query: ' . Dumper($query));
		push @queries, $query;
	}
	
	return \@queries;
}

sub _get_where_clause {
	my $self = shift;
	my $class_id = shift;
	my $match_str = shift;
	
	if ($class_id){
		return { 
			clause => 'MATCH(\'' . $match_str . '\') AND attr_tests=1 AND class_id=?',
			values => [ $class_id ]
		}
	}
	else {
		return { 
			clause => 'MATCH(\'' . $match_str . '\') AND attr_tests=1',
			values => []
		}
	}
}

sub _get_groupby_clause {
	my $self = shift;
	my $class_id = shift;
	
	return '' unless $self->groupby;
	return $self->_sphinx_attr($self->groupby, $class_id);
}

sub _get_orderby_clause {
	my $self = shift;
	my $class_id = shift;
	
	return '' unless $self->orderby;
	return $self->_sphinx_attr($self->orderby, $class_id);
}

# Divine what classes this query will encompass given the terms
sub _get_class_ids {
	my $self = shift;
	my $or_key = shift;
	
	my %classes;
	
	# If there is a groupby, verify that the classes in the groupby match the terms
	if ($self->groupby){
		my $field_classes = $self->_classes_for_field($self->groupby);
		foreach my $class_id (keys %$field_classes){
			if ($self->user->is_permitted('class_id', $class_id)){
				$classes{$class_id} = $field_classes->{$class_id};
			}
		}
		unless (scalar keys %classes){
			throw(401, 'No authorized classes for groupby field ' . $self->groupby, { term -> $self->groupby });
		}
	}
	# If there is a orderby, verify that the classes in the orderby match the terms
	elsif ($self->orderby){
		my $field_classes = $self->_classes_for_field($self->orderby);
		foreach my $class_id (keys %$field_classes){
			if ($self->user->is_permitted('class_id', $class_id)){
				$classes{$class_id} = $field_classes->{$class_id};
			}
		}
		unless (scalar keys %classes){
			throw(401, 'No authorized classes for orderby field ' . $self->orderby, { term -> $self->orderby });
		}
	}
	
	# Find the unique fields requested
	my %fields;
	if ($or_key){
		$self->terms->{or}->{$or_key}->{field} and $fields{ $self->terms->{or}->{$or_key}->{field} } = 1;
	}
	foreach my $boolean (qw(and not)){
		foreach my $key (keys %{ $self->terms->{$boolean} }){
#			# If we find any term that is an OR with an unspecified field, we have to search all classes
#			if (not $self->terms->{$boolean}->{$key}->{field} and $boolean eq 'or'){
#				return $self->permitted_classes;
#			}
			$self->terms->{$boolean}->{$key}->{field} and $fields{ $self->terms->{$boolean}->{$key}->{field} } = 1;
		}
	}
	
	# Foreach field, find classes
	foreach my $field (keys %fields){
		my $field_classes = $self->_classes_for_field($field);
		foreach my $class_id (keys %$field_classes){
			if ($self->groupby and not exists $classes{$class_id}){
				throw(400, 'Term ' . $field . ' is incompatible with groupby field . ' . $self->groupby, { term => $field });
			}
			elsif ($self->orderby and not exists $classes{$class_id}){
				throw(400, 'Term ' . $field . ' is incompatible with orderby field . ' . $self->orderby, { term => $field });
			}
			
			if ($self->user->is_permitted('class_id', $class_id)){
				$classes{$class_id} = $field_classes->{$class_id};
			}
		}
	}
	
	# Verify field permissions
	foreach my $boolean (qw(and or not)){
		foreach my $key (keys %{ $self->terms->{$boolean} }){
			foreach my $class_id (keys %classes){
				unless ($self->user->is_permitted($self->terms->{$boolean}->{$key}->{field}, $self->terms->{$boolean}->{$key}->{value}, $class_id)){
					delete $classes{$class_id};
				}
			}
		}
	}
	
	# If no classes are specified via terms/groupby/orderby, go with all
	unless (scalar keys %fields or $self->groupby or $self->orderby){
		if (not $self->user->permissions->{class_id}->{0}){
			return $self->permitted_classes;
		}
		else {
			return { 0 => 1 };
		}
	}
	
	return \%classes;
}
		
sub _classes_for_field {
	my $self = shift;
	my $field_name = shift;
	my $field_hashes = $self->info->{fields_by_name}->{$field_name};
	my %classes;
	foreach my $field_hash (@$field_hashes){
		$classes{ $field_hash->{class_id} } = $field_hash->{class};
	}
	return \%classes;
}

sub _sphinx_col_name {
	my $self = shift;
	my $field_name = shift;
	my $class_id = shift;
	
	my $field_hashes = $self->info->{fields_by_name}->{$field_name};
	foreach my $field_hash (@$field_hashes){
		if ($field_hash->{class_id} eq $class_id){
			return $Fields::Field_order_to_field->{ $field_hash->{field_order} };
		}
	}
		
	#throw(500, 'Unable to find column for field ' . $field_name, { term => $field_name });
	return;
}

sub _is_int_field {
	my $self = shift;
	my $field_name = shift;
	my $class_id = shift;
	
	my $field_hashes = $self->info->{fields_by_name}->{$field_name};
	foreach my $field_hash (@$field_hashes){
		if ($field_hash->{class_id} eq $class_id){
			return $field_hash->{field_type};
		}
	}
		
	#throw(500, 'Unable to find type for field ' . $field_name, { term => $field_name });
	return;
}

sub _get_match_str {
	my $self = shift;
	my $class_id = shift;
	my $searches = shift;
	
	my %clauses;
	
	foreach my $hash (@{ $searches }){
		$clauses{ $hash->{boolean} } ||= {};
		
		if ($hash->{field}){
			my $value = $self->_sphinx_value($hash->{field}, $hash->{value}, $class_id);
			my $field = $self->_sphinx_col_name($hash->{field}, $class_id);
			$clauses{ $hash->{boolean} }->{'(@' . $field . ' ' . $value . ')'} = 1;
		}
		else {
			$clauses{ $hash->{boolean} }->{'(' . $hash->{value} . ')'} = 1;
		}
	}
	
	my @boolean_clauses;
	
	if (scalar keys %{ $clauses{and} }){
		push @boolean_clauses, '(' . join(' ', sort keys %{ $clauses{and} }) . ')';
	}
	if (scalar keys %{ $clauses{or} }){
		push @boolean_clauses, '(' . join('|', sort keys %{ $clauses{or} }) . ')';
	}
	if (scalar keys %{ $clauses{not} }){
		push @boolean_clauses, '(' . join('|', sort keys %{ $clauses{not} }) . ')';
	}
	
	return join(' ', @boolean_clauses);
}

sub _sphinx_value {
	my $self = shift;
	my $field_name = shift;
	my $value = shift;
	my $class_id = shift;
	
	my $field_order;
	if ($field_name){
		my $field_hash = $self->_get_field($field_name, $class_id);
		$field_order = $field_hash->{field_order};
	}
	else {
		$field_order = -1;
	}
	
	my $orig_value = $value;
	$value =~ s/^\"//;
	$value =~ s/\"$//;
	
	#$self->log->trace('args: ' . Dumper($args) . ' value: ' . $value . ' field_order: ' . $field_order);
	
	unless (defined $class_id and defined $value and defined $field_order){
		$self->log->error('Missing an arg: ' . $class_id . ', ' . $value . ', ' . $field_order);
		return $value;
	}
	
	return $value unless $self->info->{field_conversions}->{ $class_id };
	
	if ($field_order == $Fields::Field_to_order->{host}){ #host is handled specially
		my @ret;
		if ($value =~ /^"?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"?$/) {
			@ret = ( unpack('N*', inet_aton($1)) ); 
		}
		elsif ($value =~ /^"?([a-zA-Z0-9\-\.]+)"?$/){
			my $host_to_resolve = $1;
			unless ($host_to_resolve =~ /\./){
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
					throw(500, 'Unable to resolve host ' . $host_to_resolve . ': ' . $res->errorstring, { external_dns => $host_to_resolve });
				}
			}
			else {
				throw(500, 'Unable to resolve host ' . $host_to_resolve . ': ' . $res->errorstring, { external_dns => $host_to_resolve });
			}
		}
		else {
			throw(400, 'Invalid host given: ' . Dumper($value), { host => $value });
		}
		if (wantarray){
			return @ret;
		}
		else {
			return $ret[0];
		}
	}
	elsif ($field_order == $Fields::Field_to_order->{class}){
		return $self->info->{classes}->{ uc($value) };
	}
	elsif ($self->info->{field_conversions}->{ $class_id }->{'IPv4'}
		and $self->info->{field_conversions}->{ $class_id }->{'IPv4'}->{$field_order}
		and $value =~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/){
		$self->log->debug('converting ' . $value . ' to IPv4');
		return unpack('N', inet_aton($value));
	}
	elsif ($self->info->{field_conversions}->{ $class_id }->{PROTO} 
		and $self->info->{field_conversions}->{ $class_id }->{PROTO}->{$field_order}){
		$self->log->trace("Converting $value to proto");
		return exists $Fields::Proto_map->{ uc($value) } ? $Fields::Proto_map->{ uc($value) } : int($value);
	}
	elsif ($self->info->{field_conversions}->{ $class_id }->{COUNTRY_CODE} 
		and $self->info->{field_conversions}->{ $class_id }->{COUNTRY_CODE}->{$field_order}){
		if ($Fields::Field_order_to_attr->{$field_order} =~ /attr_s/){
			$self->log->trace("Converting $value to CRC of country_code");
			return crc32(join('', unpack('c*', pack('A*', uc($value)))));
		}
		else {
			$self->log->trace("Converting $value to country_code");
			return join('', unpack('c*', pack('A*', uc($value))));
		}
	}
	elsif ($Fields::Field_order_to_attr->{$field_order} eq 'program_id'){
		$self->log->trace("Converting $value to attr");
		return crc32($value);
	}
	elsif ($Fields::Field_order_to_attr->{$field_order} =~ /^attr_s\d+$/){
		# String attributes need to be crc'd
		return crc32($value);
	}
	else {
		# Integer value
		if ($orig_value == 0 or int($orig_value)){
			return $orig_value;
		}
		else {
			# Try to find an int and use that
			$orig_value =~ s/\\?\s//g;
			if (int($orig_value)){
				return $orig_value;
			}
			else {
				throw(400, 'Invalid query term, not an integer: ' . $orig_value, { term => $orig_value });
			}
		}
	}
		
	throw(500, 'Unable to find value for field ' . $field_name, { term => $field_name });
}

sub _get_field {
	my $self = shift;
	my $field_name = shift;
	my $class_id = shift;
	
	my $field_hashes = $self->info->{fields_by_name}->{$field_name};
	foreach my $field_hash (@$field_hashes){
		if ($field_hash->{class_id} eq $class_id){
			return $field_hash
		}
	}
		
	throw(500, 'Unable to find column for field ' . $field_name, { term => $field_name });
}

sub _sphinx_attr {
	my $self = shift;
	my $field_name = shift;
	my $class_id = shift;
	
	my $field_hashes = $self->info->{fields_by_name}->{$field_name};
	foreach my $field_hash (@$field_hashes){
		if ($field_hash->{class_id} eq $class_id){
			return $Fields::Field_order_to_attr->{ $field_hash->{field_order} };
		}
	}
		
	throw(500, 'Unable to find column for field ' . $field_name, { term => $field_name });
}

sub _get_attr_tests {
	my $self = shift;
	my $class_id = shift;
	my $filters = shift;
	
	my %terms;
	foreach my $hash (@$filters){
		my $attr = $self->_sphinx_attr($hash->{field}, $class_id);
		push @{ $terms{ $hash->{boolean} } }, sprintf('%s=%d', $attr, $self->_sphinx_value($hash->{field}, $hash->{value}, $class_id));
	}
	
	my @attr_clauses;
	if ($terms{and} and scalar @{ $terms{and} }){
		push @attr_clauses, '(' . join(' AND ', @{ $terms{and} }) . ')';
	}
	if ($terms{or} and scalar @{ $terms{or} }){
		push @attr_clauses, '(' . join(' OR ', @{ $terms{or} }) . ')';
	}
	if ($terms{not} and scalar @{ $terms{not} }){
		push @attr_clauses, 'NOT (' . join(' OR ', @{ $terms{or} }) . ')';
	}
	
	return scalar @attr_clauses ? join(' AND ', @attr_clauses) : 1;
}

sub _get_select_clause {
	my $self = shift;
	my $class_id = shift;
	my $attr_string = shift;
	
	if ($self->groupby){
		return {
			clause => 'SELECT COUNT(*) AS _count, ' . $self->_sphinx_col_name($self->groupby, $class_id) . ' AS _groupby, '
			. $attr_string . ' AS attr_tests',
			values => [],
		}
	}
	return {
		clause => 'SELECT *, ' . $attr_string . ' AS attr_tests',
		values => [],
	}
}

sub _get_search_terms {
	my $self = shift;
	my $class_id = shift;
	my $index = shift;
	my $or_key = shift;

	my $ret = { searches => [], filters => [] };
	
	# For some reason, the copy of the hash ref was optimizing to just a ref
	my $local_terms = { and => {}, not => {} };
	foreach my $boolean (qw(and not)){
		foreach my $key (keys %{ $self->terms->{$boolean} }){
			$local_terms->{$boolean}->{$key} = { %{ $self->terms->{$boolean}->{$key} } };
		}
	}
	if ($or_key){
		$local_terms->{and}->{$or_key} = $self->terms->{or}->{$or_key};
	}
	
	$self->log->debug('or_key: ' . $or_key);
	
	foreach my $boolean (qw(and not)){
		foreach my $key (keys %{ $local_terms->{$boolean} }){
			my %hash = %{ $local_terms->{$boolean}->{$key} };
			$hash{boolean} = $boolean;
			my $index_schema = $self->_get_index_schema($index);
			
			my $added_to_search = 0;
#			# An OR has to be a search term
#			if ($boolean eq 'or'){
#				if ($hash{field} and $self->_sphinx_col_name($hash{field}, $class_id) 
#					and $index_schema->{fields}->{ $self->_sphinx_col_name($hash{field}, $class_id) }){
#					push @{ $ret->{searches} }, { %hash };
#				}
#				elsif ($hash{field}){
#					$self->add_warning(304, 'Removing field stipulation ' . $hash{field} . ' for value ' . $hash{value}, { term => $hash{value} });
#					$hash{field} = '';
#					push @{ $ret->{searches} }, { %hash };
#					$added_to_search = 1;
#				}
#			}
			
			# Is this a non-search op?
			unless ($hash{op} eq ':' or $hash{op} eq '=' or $hash{op} eq '~'){
				push @{ $ret->{filters} }, { %hash } if $hash{field};
				next;
			}
			
			# Is it an int field?
			if($hash{field} and $self->_is_int_field($hash{field}, $class_id)){
				push @{ $ret->{filters} }, { %hash } if $hash{field};
				next;
			}
			
			# Is this a stopword?
			elsif ($self->parser->stopword_terms->{ $hash{value} }){
				# Will need to tack it on the post filter list
				$self->post_filters->{ $hash{boolean} }->{ $hash{value} } = $hash{field};
				next;
			}
			
			# Is it quoted and the op isn't ~ ?
			elsif ($hash{op} ne '~' and $hash{quoted}){
				# Make it a filter
				push @{ $ret->{filters} }, { %hash } if $hash{field};
				next;
			}
			# Default to search term
			elsif (not $added_to_search){ # Verify that we didn't already add this above
				if (($hash{field} and $self->_sphinx_col_name($hash{field}, $class_id) 
					and $index_schema->{fields}->{ $self->_sphinx_col_name($hash{field}, $class_id) }) or not $hash{field}){
					push @{ $ret->{searches} }, { %hash };
				}
				else {
					$self->log->warn('Making term ' . $hash{value} . ' into a filter because the field does not exist in this schema');
					push @{ $ret->{filters} }, { %hash };
				}
			}
		}
	}
	
#	# Any lone OR filters?
#	my @ors_to_move;
#	foreach my $hash (@{ $ret->{filters} }){
#		if ($hash->{boolean} eq 'or'){
#			push @ors_to_move, $hash;
#		}
#	}
#	if (scalar @ors_to_move == 1){
#		# Is there a term there already?
#		my $hash = pop(@ors_to_move);
#		foreach my $existing (@{ $ret->{searches} }){
#			if ($existing->{op} eq $hash->{op} and $existing->{value} eq $hash->{value} and $existing->{boolean} eq $hash->{boolean}){
#				$self->log->trace('No need to move single OR value, value exists as a search term already');
#			}
#			else {
#				$self->log->trace('Cannot have a single OR filter, moving to search terms: ' . Dumper($ret));
#				push @{ $ret->{searches} }, $hash;
#			}
#		}
#	}
	
	# Do we have any search terms now?
	if (scalar @{ $ret->{searches} }){
		return $ret;
	}
	
	# else, we need to try to find a search term from the filters
	my %candidates;
	my %int_candidates;
#	my %or_candidates;
	foreach my $hash (@{ $ret->{filters} }){
#		if ($hash->{boolean} eq 'or'){
#			$or_candidates{ $hash->{value} } = $hash;
#		}
		next unless $hash->{boolean} eq 'and';
		next if $self->parser->stopword_terms->{ $hash->{value} };
		
		# Verify this field exists in this index
		if ($hash->{field}){
			my $index_schema = $self->_get_index_schema($index);
			my $field = $self->_sphinx_col_name($hash->{field}, $class_id);
			unless ($index_schema->{fields}->{$field}){
				$int_candidates{ $hash->{value} } = $hash;
				next;
			}
		}
		
		$candidates{ $hash->{value} } = $hash;
	}
	
	if (scalar keys %candidates){
		# Pick the longest
		my $longest = (sort { length($b) <=> length($a) } keys %candidates)[0];
		push @{ $ret->{searches} }, $candidates{$longest};
	}
	elsif (scalar keys %int_candidates){
		# Use an attribute as an anyfield query, pick the highest number
		my $biggest = (sort { int($b) <=> int($a) } keys %int_candidates)[0];
		push @{ $ret->{searches} }, { field => '', value => $biggest, boolean => 'and' };
	}
#	elsif (scalar keys %or_candidates){
#		# Use all OR's
#		foreach my $term (keys %or_candidates){
#			push @{ $ret->{searches} }, { field => '', value => $term, boolean => 'or' };
#		}
#	}
	
	return $ret;
}

sub _get_permissions_clause {
	my $self = shift;
}

sub _get_index_list {
	my $self = shift;
	
	my @indexes;
	foreach my $index (@{ $self->info->{indexes}->{indexes} }){
		# Check that the time is right
		if (($index->{start_int} <= $self->start and $index->{end_int} >= $self->start)
			or ($index->{start_int} <= $self->end and $index->{end_int} >= $self->end)
			or ($index->{start_int} >= $self->start and $index->{end_int} <= $self->end)){
			
			push @indexes, $index;
		}
	}
	
	return \@indexes;
}

sub _get_index_schema {
	my $self = shift;
	my $index_name = shift;
	foreach my $index (@{ $self->info->{indexes}->{indexes} }){
		if ($index->{name} eq $index_name){
			return $index->{schema};
		}
	}
	throw(500, 'Unable to find index ' . $index_name);
}
	

sub execute {
	my $self = shift;
	my $cb = shift;
	
	$self->_get_sphinx_db(sub {
		my $ok = shift;
		if (not $ok){
			$cb->($self->errors);
			return;
		}
		my $indexes = $self->_get_index_list();
		$self->log->trace('Querying indexes: ' . scalar @$indexes);
		
		unless (scalar @$indexes){
			$self->add_warning('516', 'No data for time period queried', { term => 'start' });
			$cb->($self->results);
			return;
		}
		
		my $start = time();
		my $counter = 0;
		my $total = 0;
		my $cv = AnyEvent->condvar;
		$cv->begin(sub {
			if ($self->has_errors){
				$cb->($self->errors);
			}
			else {
				$self->results->percentage_complete(100 * $counter / $total);
				$cb->($self->results);
			}
		});
		foreach my $index (@$indexes){
			last if $self->limit and $self->results->records_returned >= $self->limit;
			if ($self->timeout and (time() - $start) >= $self->timeout){
				$self->results->is_approximate(1);
				last;
			}
			my $queries = $self->_build_queries($index->{name});
			$total += scalar @$queries;
			$self->log->debug('index: ' . Dumper($index));
			foreach my $query (@$queries){
				$cv->begin;
				$self->_sphinx_query($index->{name}, $query, sub {
					my $per_index_results = shift;
					$counter++;
					if (not $per_index_results or $per_index_results->{error}){
						$self->log->error('Query error: ' . Dumper($per_index_results));
					}
					$cv->end;
				});
			}
		}
		
		$cv->end;
	});
}

sub _sphinx_query {
	my $self = shift;
	my $index = shift;
	my $query = shift;
	my $cb = shift;
	
	my $cv = AnyEvent->condvar;
	my $ret = { stats => {} };
	$cv->begin( sub{
		$self->log->debug('ret: ' . Dumper($ret));
		$cb->($ret);
	});
	my @values = (@{ $query->{select}->{values} }, @{ $query->{where}->{values} });
	my $query_string = $query->{select}->{clause} . ' FROM ' . $index . ' WHERE ' . $query->{where}->{clause};
	if ($self->groupby){
		$query_string .= ' GROUP BY ' . $query->{groupby} . ' ORDER BY _count ';
	}
	elsif ($self->orderby){
		$query_string .= ' ORDER BY ' . $query->{orderby};
	}
	else {
		$query_string .= ' ORDER BY timestamp';
	}
	if ($self->orderby_dir eq 'DESC'){
		$query_string .= ' DESC';
	}
	else {
		$query_string .= ' ASC';
	}
	$query_string .= ' LIMIT ?,?';
	push @values, ($self->offset, $self->limit);
	
	$self->log->trace('Sphinx query: ' . $query_string);
	$self->log->trace('Sphinx query values: ' . join(',', @values));
	
	my $start = time();
	$self->sphinx_db->{sphinx}->sphinx($query_string . ';SHOW META', 0, @values, sub {
		my ($dbh, $result, $rv) = @_;
		
		my $sphinx_query_time = (time() - $start);
		$self->log->debug('Sphinx query finished in ' . $sphinx_query_time);
		$ret->{stats}->{sphinx_query} += $sphinx_query_time;
		$start = time();
		
		if (not $rv){
			my $e = 'sphinx got error ' .  Dumper($result);
			$self->log->error($e);
			$self->add_warning(500, $e, { sphinx => $self->peer_label });
			$ret = { error => $e };
			$cv->end;
		}
		my $rows = $result->{rows};
		$ret->{sphinx_rows} ||= [];
		push @{ $ret->{sphinx_rows} }, @$rows;
		$self->log->trace('got sphinx result: ' . Dumper($result));
		if ($ret->{meta}){
			foreach my $key (keys %{ $result->{meta} }){
				if ($key !~ /^keyword/ and $result->{meta}->{$key} =~ /^\d+(?:\.\d+)?$/){
					$ret->{meta}->{$key} += $result->{meta}->{$key};
				}
				else {
					$ret->{meta}->{$key} = $result->{meta}->{$key};
				}
			}
		}
		else {
			$ret->{meta} = $result->{meta};
		}
		
		# Go get the rows that contain the actual docs
		if (scalar @{ $ret->{sphinx_rows} }){
			$self->_get_rows($ret, sub { 
				$self->_format_records($ret);
				$self->_post_filter_results($ret);
				$cv->end;
			});
		}
		else {
			$self->log->trace('No rows found');
			$cv->end;
		}
	});
}

# Intermediary method to abstract exactly how the real rows get retrieved
sub _get_rows {
	my $self = shift;
	my $ret = shift;
	my $cb = shift;
	
	my ($query, $sth);
	# Find what tables we need to query to resolve rows
	my %tables;
	
	ROW_LOOP: foreach my $row (@{ $ret->{sphinx_rows} }){
		foreach my $table_hash (@{ $self->info->{tables}->{tables} }){
			next unless $table_hash->{table_type} eq 'index' or $table_hash->{table_type} eq 'import';
			if ($table_hash->{min_id} <= $row->{id} and $row->{id} <= $table_hash->{max_id}){
				$tables{ $table_hash->{table_name} } ||= [];
				push @{ $tables{ $table_hash->{table_name} } }, $row->{id};
				next ROW_LOOP;
			}
		}
	}
	
	unless (scalar keys %tables){
		$self->log->error('No tables found for results');
		$cb->($ret);
		return;
	}	
			
	# Go get the actual rows from the dbh
	my @table_queries;
	my @table_query_values;
	my %import_tables;
	foreach my $table (sort keys %tables){
		my $placeholders = join(',', map { '?' } @{ $tables{$table} });
		
		my $table_query = sprintf("SELECT %1\$s.id,\n" .
			"timestamp, host_id, program_id,\n" .
			"INET_NTOA(host_id) AS host, class_id, msg,\n" .
			"i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5\n" .
			"FROM %1\$s\n" .
			'WHERE %1$s.id IN (' . $placeholders . ')',
			$table, $self->sphinx_db->{db});
		
		if ($table =~ /import/){
			$import_tables{$table} = $tables{$table};
		}
		else {
			push @table_queries, $table_query;
			push @table_query_values, @{ $tables{$table} };
		}
	}
	
	if (keys %import_tables){
		$self->_get_import_rows(\%import_tables, $ret, sub {
			$self->_get_mysql_rows(\@table_queries, \@table_query_values, $ret, $cb);
		});
	}
	else {
		$self->_get_mysql_rows(\@table_queries, \@table_query_values, $ret, $cb);
	}
}

sub _get_extra_field_values {
	my $self = shift;
	my $ret = shift;
	my $cb = shift;
	
	my %programs;
	foreach my $row (@{ $ret->{sphinx_rows} }){
		$programs{ $row->{program_id} } = $row->{program_id};
	}
	
	my $query;
	$query = 'SELECT id, program FROM programs WHERE id IN (' . join(',', map { '?' } keys %programs) . ')';
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub {
		$cb->($ret);
	});
	$self->sphinx_db->{dbh}->query($query, (sort keys %programs), sub { 
		my ($dbh, $rows, $rv) = @_;
		if (not $rv or not ref($rows) or ref($rows) ne 'ARRAY'){
			my $errstr = 'got error getting extra field values ' . $rows;
			$self->log->error($errstr);
			$self->add_warning(502, $errstr, { mysql => $self->peer_label });
			$cv->end;
		}
		elsif (not scalar @$rows){
			$self->log->error('Did not get extra field value rows though we had values: ' . Dumper(\%programs)); 
		}
		$self->log->trace('got extra field value db rows: ' . (scalar @$rows));
		foreach my $id (keys %{ $ret->{results} }){
			$ret->{results}->{$id}->{program} = $programs{ $ret->{results}->{$id}->{program_id} };
		}
		$cv->end;
	});
}

sub _format_records {
	my $self = shift;
	my $ret = shift;
	
	my @tmp;
	foreach my $id (sort { $a <=> $b } keys %{ $ret->{results} }){
		my $row = $ret->{results}->{$id};
		$row->{datasource} = 'Sphinx';
		$row->{_fields} = [
				{ field => 'host', value => $row->{host}, class => 'any' },
				{ field => 'program', value => $row->{program}, class => 'any' },
				{ field => 'class', value => $self->info->{classes_by_id}->{ $row->{class_id} }, class => 'any' },
			];
		my $is_import = 0;
		foreach my $import_col (@{ $Fields::Import_fields }){
			if (exists $row->{$import_col}){
				$is_import++;
				push @{ $row->{_fields} }, { field => $import_col, value => $row->{$import_col}, class => 'any' };
			}
		}
		if ($is_import){					
			# Add node
			push @{ $row->{_fields} }, { field => 'node', value => $row->{node}, class => 'any' };
		}
		# Resolve column names for fields
		foreach my $col (qw(i0 i1 i2 i3 i4 i5 s0 s1 s2 s3 s4 s5)){
			my $value = delete $row->{$col};
			# Swap the generic name with the specific field name for this class
			my $field = $self->info->{fields_by_order}->{ $row->{class_id} }->{ $Fields::Field_to_order->{$col} }->{value};
			if (defined $value and $field){
				# See if we need to apply a conversion
				$value = $self->resolve_value($row->{class_id}, $value, $col);
				push @{ $row->{_fields} }, { 'field' => $field, 'value' => $value, 'class' => $self->info->{classes_by_id}->{ $row->{class_id} } };
			}
		}
		push @tmp, $row;
	}
	
	
	# Now that we've got our results, order by our given order by
	if ($self->orderby_dir eq 'DESC'){
		foreach my $row (sort { $b->{_orderby} <=> $a->{_orderby} } @tmp){
			$self->results->add_result($row);
			last if $self->results->records_returned >= $self->limit;
		}
	}
	else {
		foreach my $row (sort { $a->{_orderby} <=> $b->{_orderby} } @tmp){
			$self->log->debug('adding row: ' . Dumper($row));
			$self->results->add_result($row);
			last if $self->results->records_returned >= $self->limit;
		}
	}
	
	$self->results->total_docs($self->results->total_docs + $ret->{meta}->{total_found});
}

sub _post_filter_results {
	my $self = shift;
	my $ret = shift;
	
	if ($self->has_postfilters){
		$self->log->trace('post filtering results with: ' . Dumper($self->post_filters));
		my @keep = $self->results->all_results;
		my $removed = 0;
		for (my $i = 0; $i < @keep; $i++){
			if (not $self->_filter_stopwords($keep[$i])){
				splice(@keep, $i, 1);
				$removed++;
			}
		}
		$self->results->results([ @keep ]);
		$self->results->total_records($self->results->total_records - $removed);
	}
}

sub _filter_stopwords {
	my $self = shift;
	my $record = shift;
	
	# Filter any records which have stopwords
	if (scalar keys %{ $self->post_filters->{and} }){
		my $to_find = scalar keys %{ $self->post_filters->{and} };
		STOPWORD_LOOP: foreach my $stopword (keys %{ $self->post_filters->{and} }){
			my $regex = $self->parser->term_to_regex($stopword);
			my $stopword_field = $self->post_filters->{and}->{$stopword};
			foreach my $field (keys %$record){
				if ((($stopword_field and $field eq $stopword_field) or not $stopword_field) and $record->{$field} =~ qr/$regex/i){
					$self->log->debug('Found stopword: ' . $stopword . ' for term ' . $record->{$field} . ' and field ' . $field);
					$to_find--;
					last STOPWORD_LOOP;
				}
			}
		}
		return 0 if $to_find;
	}
	
	if (scalar keys %{ $self->post_filters->{not} }){
		foreach my $stopword (keys %{ $self->post_filters->{not} }){
			my $regex = $self->parser->term_to_regex($stopword);
			foreach my $field (keys %$record){
				if ($record->{$field} =~ qr/$regex/i){
					$self->log->debug('Found not stopword: ' . $stopword . ' for term ' . $record->{$field} . ' and field ' . $field);
					return 0;
				}
			}
		}
	}
	return 1;
}
	
sub _get_import_rows {
	my $self = shift;
	my $import_tables = shift;
	my $ret = shift;
	my $cb = shift;
	my %import_info;
	my @import_queries;
	
	
	my $import_info_query = 'SELECT id AS import_id, name AS import_name, description AS import_description, ' .
		'datatype AS import_type, imported AS import_date, first_id, last_id FROM ' . $self->sphinx_db->{db} 
		. '.imports WHERE ';
	my @import_info_query_clauses;
	foreach (sort values %$import_tables){
		push @import_info_query_clauses, '? BETWEEN first_id AND last_id';
	}
	$import_info_query .= join(' OR ', @import_info_query_clauses);
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub { $cb->($ret); });
	
	$self->sphinx_db->{dbh}->query($import_info_query, (sort values %$import_tables), sub { 
		my ($dbh, $rows, $rv) = @_;
		if (not $rv or not ref($rows) or ref($rows) ne 'ARRAY'){
			my $errstr = 'got error ' . $rows;
			$self->log->error($errstr);
			$self->add_warning(502, $errstr, { mysql => $self->peer_label });
			$cv->end;
		}
		elsif (not scalar @$rows){
			$self->log->error('Did not get import info rows though we had import values: ' . Dumper($import_tables)); 
		}
		$self->log->trace('got import info db rows: ' . (scalar @$rows));
		
		# Map each id to the right import info
		foreach my $table (sort keys %$import_tables){
			foreach my $id (@{ $import_tables->{$table} }){
				foreach my $row (@$rows){
					if ($row->{first_id} <= $id and $id <= $row->{last_id}){
						$import_info{$id} = $row;
						last;
					}
				}
			}
		}
		$self->log->debug('import_info: ' . Dumper(\%import_info));
		$ret->{import_info} = { %import_info };
		$cv->end;
	});
}		

sub _get_mysql_rows {
	my $self = shift;
	my $table_queries = shift;
	my $table_values = shift;
	my $ret = shift;
	my $cb = shift;
	
	# orderby_map preserves the _orderby field between Sphinx results and MySQL results
	my %orderby_map; 
	foreach my $row (@{ $ret->{sphinx_rows} }){
		if ($self->orderby){
			$orderby_map{ $row->{id} } = $row->{_orderby};
		}
	}
	$self->log->debug('%orderby_map  ' . Dumper(\%orderby_map));
	
	my $table_query = join(';', @$table_queries);
	$self->log->trace('table query: ' . $table_query . ', placeholders: ' . join(',', @$table_values));
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub {
		$self->_get_extra_field_values($ret, $cb);
	});
	
	my $start = time();
	
	$self->sphinx_db->{dbh}->multi_query($table_query, @$table_values, sub { 
		my ($dbh, $rows, $rv) = @_;
		if (not $rv or not ref($rows) or ref($rows) ne 'ARRAY'){
			my $errstr = 'got error getting mysql rows ' . $rows;
			$self->log->error($errstr);
			$self->add_warning(502, $errstr, { sphinx => $self->peer_label });
			$cv->end;
		}
		elsif (not scalar @$rows){
			$self->log->error('Did not get rows though we had Sphinx results!'); 
		}
		$self->log->trace('got db rows: ' . (scalar @$rows));
		
		foreach my $row (@$rows){
			$ret->{results} ||= {};
			$row->{node} = $self->peer_label ? $self->peer_label : '127.0.0.1';
			$row->{node_id} = unpack('N*', inet_aton($row->{node}));
			if ($self->orderby){
				$row->{_orderby} = $orderby_map{ $row->{id} };
			}
			else {
				$row->{_orderby} = $row->{timestamp};
			}
			# Copy import info into the row
			if ($ret->{import_info} and exists $ret->{import_info}->{ $row->{id} }){
				foreach my $import_col (@{ $Fields::Import_fields }){
					if ($ret->{import_info}->{ $row->{id} }->{$import_col}){
						$row->{$import_col} = $ret->{import_info}->{ $row->{id} }->{$import_col};
					}
				}
			}
			$ret->{results}->{ $row->{id} } = $row;
		}
		$ret->{stats}->{mysql_query} += (time() - $start);
		$cv->end;
	});
}

sub _get_sphinx_db {
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
			
	my $sphinx_port = 9306;
	if ($conf->{sphinx_port}){
		$sphinx_port = $conf->{sphinx_port};
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
		$self->log->trace('connecting to sphinx ');
		
		$ret->{sphinx} = SyncMysql->new(log => $self->log, db_args => [
			'dbi:mysql:port=' . $sphinx_port .';host=127.0.0.1', undef, undef,
			{
				mysql_connect_timeout => $self->db_timeout,
				PrintError => 0,
				mysql_multi_statements => 1,
				mysql_bind_type_guessing => 1,
			}
		]);
	};
	if ($@){
		$self->add_warning(502, $@, { mysql => $self->peer_label });
		$cb->(0);
	}		
	
	$self->log->trace('All connected in ' . (time() - $start) . ' seconds');
	$self->sphinx_db($ret);
	
	$cb->(1);
}

1;