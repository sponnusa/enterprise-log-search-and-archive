package Query;
use Moose;
with 'MooseX::Traits';
with 'Utils';
with 'Fields';
with 'MooseX::Clone';
use Results;
use Time::HiRes;
use Data::Dumper;
use Search::QueryParser;
use Storable qw(dclone);
use Socket;

# Object for dealing with user queries

our $Default_limit = 100;
our $Implicit_plus = 0;

# Required
has 'query_string' => (is => 'rw', isa => 'Str', required => 1);
has 'meta_params' => (is => 'rw', isa => 'HashRef', required => 1);
has 'user' => (is => 'rw', isa => 'User', required => 1);
has 'node_info' => (is => 'rw', isa => 'HashRef', required => 1);

# Required with defaults
has 'type' => (is => 'rw', isa => 'Str', required => 1, default => 'index');
has 'results' => (is => 'rw', isa => 'Results', required => 1);
has 'start_time' => (is => 'ro', isa => 'Num', required => 1, default => sub { Time::HiRes::time() });
has 'groupby' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { has_groupby => 'count', all_groupbys => 'elements', add_groupby => 'push' });
has 'timeout' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'cancelled' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'archive' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'livetail' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'datasources' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { { sphinx => 1 } });
has 'analytics' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'system' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'batch' => (is => 'rw', isa => 'Bool', required => 1, default => 0, trigger => \&_set_batch);
has 'limit' => (is => 'rw', isa => 'Int', required => 1, default => $Default_limit);
has 'offset' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'classes' => (is => 'rw', isa => 'HashRef' => required => 1, default => sub { return { map { $_ => {} } qw(given excluded distinct permitted partially_permitted groupby) } });
has 'start' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'end' => (is => 'rw', isa => 'Int', required => 1, default => sub { time() });
has 'cutoff' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'transforms' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { has_transforms => 'count', all_transforms => 'elements', num_transforms => 'count' });
has 'connectors' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { has_connectors => 'count', all_connectors => 'elements', num_connectors => 'count',
		connector_idx => 'get' });
has 'connector_params' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { has_connector_params => 'count', all_connector_params => 'elements', num_connector_params => 'count',
		connector_params_idx => 'get' });
has 'terms' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'nodes' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { { given => {}, excluded => {} } });
has 'hash' => (is => 'rw', isa => 'Str', required => 1, default => '');
has 'highlights' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'warnings' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { 'has_warnings' => 'count', 'add_warning' => 'push', 'clear_warnings' => 'clear' });
has 'stats' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });

# Optional
has 'qid' => (is => 'rw', isa => 'Int');
has 'schedule_id' => (is => 'rw', isa => 'Int');
has 'raw_query' => (is => 'rw', isa => 'Str');
has 'comments' => (is => 'rw', isa => 'Str');
has 'time_taken' => (is => 'rw', isa => 'Num', trigger => \&_set_time_taken);
has 'batch_message' => (is => 'rw', isa => 'Str');

sub BUILDARGS {
	my $class = shift;
	my %params = @_;
	
	if ($params{q}){
		# JSON-encoded query from web
		my $decode = $params{json}->decode($params{q});
		$params{query_string} = $decode->{query_string};
		$params{meta_params} = $decode->{query_meta_params};
		$params{raw_query} = delete $params{q};
	}
	elsif ($params{query_meta_params}){
		$params{meta_params} = delete $params{query_meta_params};
	}
	
	foreach my $property qw(groupby timeout archive analytics datasources nobatch livetail){
		if ($params{meta_params}->{$property}){
			$params{$property} = delete $params{meta_params}->{$property};
		}
	}
	
	$params{results} ||= new Results();
	
	if ($params{conf}->get('query_timeout')){
		$params{timeout} = sprintf("%d", ($params{conf}->get('query_timeout') * 1000));
	}
	
	return \%params;
}
 
sub BUILD {
	my $self = shift;
	
	$self->log->debug('meta_params: ' . Dumper($self->meta_params));
	$self->log->debug('groupby: ' . Dumper($self->groupby));
	
	$self->resolve_field_permissions($self->user);
	
	my ($query, $sth);
	
	my $ret = { query_string => $self->query_string, query_meta_params => $self->meta_params };	
		
	# Is this a system-initiated query?
	if ($self->schedule_id){
		$self->system(1);
	}
	elsif ($self->user->username eq 'system'){
		$self->system(1);
	}
	
	$self->log->trace("Using timeout of " . $self->timeout);
		
	# Parse first to see if limit gets set which could incidate a batch job
	$self->_parse_query_string();
	
	if ($self->has_groupby){
		$self->results( new Results::Groupby() );
	}
	
	unless ($self->qid){
		# Log the query
		$self->db->begin_work;
		$query = 'INSERT INTO query_log (uid, query, system, archive) VALUES (?, ?, ?, ?)';
		$sth   = $self->db->prepare($query);
		$sth->execute( $self->user->uid, $self->raw_query ? $self->raw_query : $self->json->encode({ query_string => $self->query_string, query_meta_params => $self->meta_params }), 
			$self->system, 0 ); # set batch later
		$query = 'SELECT MAX(qid) AS qid FROM query_log';
		$sth   = $self->db->prepare($query);
		$sth->execute();
		my $row = $sth->fetchrow_hashref;
		$self->db->commit;
		
		$self->qid($row->{qid});

		$self->log->debug( "Received query with qid " . $self->qid . " at " . time() );
	}
	
	$self->hash($self->get_hash($self->qid));
	
	# Find highlights to inform the web client
	foreach my $boolean qw(and or){
		foreach my $class_id (keys %{ $self->terms->{field_terms}->{$boolean} }){
			foreach my $field_name (keys %{ $self->terms->{field_terms}->{$boolean}->{$class_id} }){
				foreach my $term (@{ $self->terms->{field_terms}->{$boolean}->{$class_id}->{$field_name} }){
					my $regex = _term_to_regex($term);
					$self->highlights->{$regex} = 1;
				}
			}
		}
		foreach my $term (sort keys %{ $self->terms->{any_field_terms}->{$boolean} }){
			my $regex = _term_to_regex($term);
			$self->highlights->{$regex} = 1;
		}
	}
		
	return $self;	
}

sub _term_to_regex {
	my $term = shift;
	my $regex = $term;
	$regex =~ s/^\s{2,}/\ /;
	$regex =~ s/\s{2,}$/\ /;
	$regex =~ s/\s/\./g;
	$regex =~ s/\\{2,}/\\/g;
	return $regex;
}

sub TO_JSON {
	my $self = shift;
	my $ret = {
		qid => $self->qid,
		totalTime => $self->time_taken,
		results => $self->results->results, 
		totalRecords => $self->results->total_records, 
		recordsReturned => $self->results->records_returned,	
		groupby => $self->groupby,
		query_string => $self->query_string,
		query_meta_params => $self->meta_params,
		hash => $self->hash,
		highlights => $self->highlights,
		stats => $self->stats,
	};
	unless ($ret->{groupby} and ref($ret->{groupby}) and ref($ret->{groupby}) eq 'ARRAY' and scalar @{ $ret->{groupby} }){
		delete $ret->{groupby};
	}
	
	# Check to see if our result is bulky
	unless ($self->meta_params->{nobatch}){
		if ($self->results->is_bulk){
			$ret->{bulk_file} = $self->results->bulk_file;
			$ret->{batch_query} = $self->qid;
			my $link = sprintf('%sQuery/get_bulk_file?qid=%d', 
				$self->conf->get('email/base_url') ? $self->conf->get('email/base_url') : 'http://localhost/',
				$self->qid);
			$ret->{batch_message} = 'Results: <a target="_blank" href="' . $link . '">' . $link . '</a>';
		}
		elsif ($self->batch_message){
			$ret->{batch_message} = $self->batch_message;
		}
		
		if ($self->batch){
			$ret->{batch} = 1;
		}
	}
	
	if ($self->has_warnings){
		$ret->{warnings} = $self->warnings;
	}
	
	return $ret;
}

sub _set_batch {
	my ( $self, $new_val, $old_val ) = @_;
	my ($query, $sth);
	$query = 'UPDATE query_log SET archive=? WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($new_val, $self->qid);
	return $sth->rows;
}

sub _set_time_taken {
	my ( $self, $new_val, $old_val ) = @_;
	my ($query, $sth);
	# Update the db to ack
	$query = 'UPDATE query_log SET num_results=?, milliseconds=? '
	  		. 'WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute( $self->results->records_returned, $new_val, $self->qid );
	return $sth->rows;
}

sub _parse_query_string {
	my $self = shift;
	
	my $raw_query = $self->query_string;
	
	my $stopwords = $self->conf->get('stopwords');
		
	foreach my $class_id (sort keys %{ $self->user->permissions->{fields} }){
		$self->classes->{partially_permitted}->{$class_id} = 1;
	}
	$self->log->trace('partially_permitted_classes: ' . Dumper($self->classes->{partially_permitted}));
	
#	# Attach the query filters for this user from permissions
#	my $filtered_raw_query = $raw_query;
#	if ($self->user->permissions->{filter}){
#		$filtered_raw_query .= ' ' . $self->user->permissions->{filter};
#	}
	
	# Strip off any transforms and apply later
	($raw_query, my @transforms) = split(/\s*\|\s+/, $raw_query);
	$self->log->trace('query: ' . $raw_query . ', transforms: ' . join(' ', @transforms));
	$self->transforms([ @transforms ]);
	
	# See if there are any connectors given
	if ($self->meta_params->{connector}){
		my $connector = $self->meta_params->{connector};
		$self->connectors([ $connector ]);
		$self->connector_params([ $self->meta_params->{connector_params} ]);
	}	
	
	# Check to see if the class was given in meta params
	if ($self->meta_params->{class}){
		$self->classes->{given}->{ sprintf("%d", $self->node_info->{classes}->{ uc($self->meta_params->{class}) }) } = 1;
	}
		
	# Check for meta limit
	if ($self->meta_params->{limit}){
		$self->limit(sprintf("%d", $self->meta_params->{limit}));
		$self->log->debug("Set limit " . $self->limit);
	}
	
	foreach my $type qw(field_terms attr_terms){
		foreach my $boolean qw(and or not){
			$self->terms->{$type}->{$boolean} = {};
		}
	}
	foreach my $boolean qw(and or not){
		$self->terms->{any_field_terms}->{$boolean} = [];
	}
		
	if ($raw_query =~ /\S/){ # could be meta_attr-only
		my $qp = new Search::QueryParser(rxTerm => qr/[^\s()]+/, rxField => qr/[\w,\.]+/);
		my $orig_parsed_query = $qp->parse($raw_query, $Implicit_plus) or die($qp->err);
		$self->log->debug("orig_parsed_query: " . Dumper($orig_parsed_query));
		
		my $parsed_query = dclone($orig_parsed_query); #dclone so recursion doesn't mess up original
		
		# Recursively parse the query terms
		$self->_parse_query_term($parsed_query);
	}
	else {
		die('No query terms given');
	}
	
	# One-off for dealing with hosts as fields
	foreach my $boolean qw(and or not){
		foreach my $op (keys %{ $self->terms->{attr_terms}->{$boolean} }){
			if ($self->terms->{attr_terms}->{$boolean}->{$op}->{host} 
				and $self->terms->{attr_terms}->{$boolean}->{$op}->{host}->{0}
				and $self->terms->{attr_terms}->{$boolean}->{$op}->{host}->{0}->{host_id}){
				foreach my $host_int (@{ $self->terms->{attr_terms}->{$boolean}->{$op}->{host}->{0}->{host_id} }){
					if ($self->user->is_permitted('host_id', $host_int)){
						next if $self->archive; # archive queries don't need this
						$self->log->trace('adding host_int ' . $host_int);
						push @{ $self->terms->{any_field_terms}->{$boolean} }, '(@host ' . $host_int . ')';
						$self->highlights->{ _term_to_regex( inet_ntoa(pack('N*', $host_int)) ) } = 1;
					}
					else {
						die "Insufficient permissions to query host_int $host_int";
					}
				}
			}
		}
	}
	
	# If no class was given anywhere, see if we can divine it from a groupby
	if (not scalar keys %{ $self->classes->{given} }){
		if ($self->has_groupby){
			foreach my $field ($self->all_groupbys){
				# Special case for node
				next if $field eq 'node';
				my $field_infos = $self->get_field($field);
				$self->log->debug('groupby field_infos: ' . Dumper($field_infos));
				foreach my $class_id (keys %{$field_infos}){
					$self->classes->{given}->{$class_id} = 1;
				}
			}
		}
	}

	$self->log->debug('attr before conversion: ' . Dumper($self->terms->{attr_terms}));
	
	# Check for blanket allow on classes
	if ($self->user->permissions->{class_id}->{0} or $self->user->is_admin){
		$self->log->trace('User has access to all classes');
		$self->classes->{permitted} = $self->node_info->{classes_by_id};
	}
	else {
		$self->classes->{permitted} = { %{ $self->user->permissions->{class_id} } };
		
		# Drop any query terms that wanted to use a forbidden class
		foreach my $boolean qw(and or not range_and range_not range_or){
			foreach my $op (keys %{ $self->terms->{attr_terms}->{$boolean} }){
				foreach my $field_name (keys %{ $self->terms->{attr_terms}->{$boolean}->{$op} }){
					foreach my $class_id (keys %{ $self->terms->{attr_terms}->{$boolean}->{$op}->{$field_name} }){
						next if $class_id eq 0 # this is handled specially below
							or $self->classes->{permitted}->{$class_id}
							or exists $self->classes->{partially_permitted}->{$class_id};
						my $forbidden = delete $self->terms->{attr_terms}->{$boolean}->{$op}->{$field_name}->{$class_id};
						$self->log->warn('Forbidding attr_term from class_id ' . $class_id . ' with ' . Dumper($forbidden));
						unless (scalar keys %{ $self->terms->{attr_terms}->{$boolean}->{$op}->{$field_name} }){
							die('All terms for field ' . $field_name . ' were dropped due to insufficient permissions.');
						}
					}
				}
			}
			
			foreach my $class_id (keys %{ $self->terms->{field_terms}->{$boolean} }){
				next if $class_id eq 0 # this is handled specially below
					or $self->classes->{permitted}->{$class_id}
					or exists $self->classes->{partially_permitted}->{$class_id};
				my $forbidden = delete $self->terms->{field_terms}->{$boolean}->{$class_id};
				$self->log->warn('Forbidding field_term from class_id ' . $class_id . ' with ' . Dumper($forbidden));
				foreach my $attr (keys %{ $self->terms->{field_terms}->{$boolean}->{$class_id} } ){
					unless (scalar keys %{ $self->terms->{field_terms}->{$boolean}->{$class_id}->{$attr} }){
						die('All terms for field ' . $attr . ' were dropped due to insufficient permissions.');
					}
				}
			}
		}
	}
	
	# Adjust classes if necessary
	$self->log->trace('given_classes before adjustments: ' . Dumper($self->classes->{given}));
	
	# Verify that all asked for classes are available in the groupby
	if (scalar keys %{ $self->classes->{given} } and scalar keys %{ $self->classes->{groupby} } ){
		foreach my $class_id (keys %{ $self->classes->{given} }){
			unless ($self->classes->{groupby}->{$class_id}){
				$self->log->trace('groupby class ' . $class_id . ' is a requested class_id');
			}
		}
	}
	# Otherwise we're just using the groupby classes
	elsif (scalar keys %{ $self->classes->{groupby} }){
		$self->classes->{given} = $self->classes->{groupby};
	}
		
	if (scalar keys %{ $self->classes->{given} } == 1 and $self->classes->{given}->{0}){
		$self->classes->{distinct} = $self->classes->{permitted};
		foreach my $class_id (keys %{ $self->classes->{partially_permitted} }){
			$self->classes->{distinct}->{$class_id} = 1;
		}
	}
	elsif (scalar keys %{ $self->classes->{given} }){ #if 0 (meaning any) is given, go with permitted classes
		$self->classes->{distinct} = {};
		foreach my $key (keys %{ $self->classes->{given} }){
			if ($self->classes->{permitted}->{$key} or exists $self->classes->{partially_permitted}->{$key}){
				$self->classes->{distinct}->{$key} = 1;
			}
			else {
				$self->log->warn('Not allowed to query given class ' . $key);
			}
		}
	}
	elsif (scalar keys %{ $self->classes->{distinct} }) {
		foreach my $key (keys %{ $self->classes->{distinct} }){
			unless ($self->classes->{permitted}->{$key} or exists $self->classes->{partially_permitted}->{$key}){
				delete $self->classes->{distinct}->{$key};
			}
		}
	}
	else {
		$self->classes->{distinct} = $self->classes->{permitted};
		foreach my $class_id (keys %{ $self->classes->{partially_permitted} }){
			$self->classes->{distinct}->{$class_id} = 1;
		}
	}
	$self->log->trace('distinct_classes after adjustments: ' . Dumper($self->classes->{distinct}));
	
	if (scalar keys %{ $self->classes->{excluded} }){
		foreach my $class_id (keys %{ $self->classes->{excluded} }){
			$self->log->trace("Excluding class_id $class_id");
			delete $self->classes->{distinct}->{$class_id};
		}
	}
	
	$self->log->debug('attr_terms: ' . Dumper($self->terms->{attr_terms}));
	
	my $num_added_terms = 0;
	my $num_removed_terms = 0;
	
	# Adjust hosts/programs based on permissions
	foreach my $attr qw(host_id program_id node_id){
		# Do we have a blanket allow permission?
		if ($self->user->permissions->{$attr}->{0}){
			$self->log->debug('Permissions grant access to any ' . $attr);
			next;
		}
		else {
			# Need to only allow access to the whitelist in permissions
			
			# Add filters for the whitelisted items
			# If there are no exceptions to the whitelist, no query will succeed
			if (not scalar keys %{ $self->user->permissions->{$attr} }){
				die 'Insufficient privileges for querying any ' . $attr; 
			}
			
			# Remove items not explicitly whitelisted
			foreach my $boolean qw(and or){
				foreach my $op ('', '='){
					next unless $self->terms->{attr_terms}->{$boolean}
						and $self->terms->{attr_terms}->{$boolean}->{$op}
						and $self->terms->{attr_terms}->{$boolean}->{$op}->{0} 
						and $self->terms->{attr_terms}->{$boolean}->{$op}->{0}->{$attr};
					foreach my $id (keys %{ $self->terms->{attr_terms}->{$boolean}->{$op}->{0}->{$attr} }){
						unless($self->user->is_permitted($attr, $id)){
							die "Insufficient permissions to query $id from $attr";
						}
					}
				}
			}
		}
	}
	
	# Optimization: for the any-term fields, only search on the first term and use the rest as filters if the fields are int fields
	foreach my $boolean qw(and not){
		unless (scalar @{ $self->terms->{any_field_terms}->{$boolean} }){
			$self->terms->{any_field_terms}->{$boolean} = {};
			next;
		}
		my %deletion_candidates;
		foreach my $op (keys %{ $self->terms->{attr_terms}->{$boolean} }){
			foreach my $field_name (keys %{ $self->terms->{attr_terms}->{$boolean}->{$op} }){
				foreach my $class_id (keys %{ $self->terms->{attr_terms}->{$boolean}->{$op}->{$field_name} }){
					foreach my $attr (keys %{ $self->terms->{attr_terms}->{$boolean}->{$op}->{$field_name}->{$class_id} }){
						foreach my $raw_value (@{ $self->terms->{attr_terms}->{$boolean}->{$op}->{$field_name}->{$class_id}->{$attr} }){
							my $col = $attr;
							$col =~ s/^attr\_//;
							my $resolved_value = $self->resolve_value($class_id, $raw_value, $col);
							$deletion_candidates{$resolved_value} = 1;
						}
					}
				}
			}
		}
	
		my @keep = shift @{ $self->terms->{any_field_terms}->{$boolean} };
		foreach my $term (@{ $self->terms->{any_field_terms}->{$boolean} }){
			if ($deletion_candidates{$term}){
				$self->log->trace('Optimizing out any-field term search for term ' . $term);
			}
			else {
				push @keep, $term;
			}
		}
		$self->terms->{any_field_terms}->{$boolean} = { map { $_ => 1 } @keep };
	}
	$self->terms->{any_field_terms}->{or} = { map { $_ => 1 } @{ $self->terms->{any_field_terms}->{or} } };
	
	# Check all field terms to see if they are a stopword and warn if necessary
	if ($stopwords and ref($stopwords) and ref($stopwords) eq 'HASH'){
		$self->log->debug('checking terms against ' . (scalar keys %$stopwords) . ' stopwords');
		foreach my $boolean qw(and or not){
			foreach my $class_id (keys %{ $self->terms->{field_terms}->{$boolean} }){
				foreach my $raw_field (keys %{ $self->terms->{field_terms}->{$boolean}->{$class_id} }){
					next unless $self->terms->{field_terms}->{$boolean}->{$class_id}->{$raw_field};
					for (my $i = 0; $i < (scalar @{ $self->terms->{field_terms}->{$boolean}->{$class_id}->{$raw_field} }); $i++){
						my $term = $self->terms->{field_terms}->{$boolean}->{$class_id}->{$raw_field}->[$i];
						if ($stopwords->{$term}){
							my $err = 'Removed term ' . $term . ' which is too common';
							$self->add_warning($err);
							$self->log->warn($err);
							$num_removed_terms++;
							# Drop the term
							if (scalar @{ $self->terms->{field_terms}->{$boolean}->{$class_id}->{$raw_field} } == 1){
								delete $self->terms->{field_terms}->{$boolean}->{$class_id}->{$raw_field};
								last;
							}
							else {
								splice(@{ $self->terms->{field_terms}->{$boolean}->{$class_id}->{$raw_field} }, $i, 1);
							}
						}
					}
				}
			}
			foreach my $term (keys %{ $self->terms->{any_field_terms}->{$boolean} }){ 
				if ($stopwords->{$term}){
					my $err = 'Removed term ' . $term . ' which is too common';
					$self->add_warning($err);
					$self->log->warn($err);
					$num_removed_terms++;
					# Drop the term
					delete $self->terms->{any_field_terms}->{$boolean}->{$term};
				}
			}
		}
	}
	
	# Determine if there are any other search fields.  If there are, then use host as a filter.
	$self->log->debug('field_terms: ' . Dumper($self->terms->{field_terms}));
	$self->log->debug('any_field_terms: ' . Dumper($self->terms->{any_field_terms}));
	my $host_is_filter = 0;
	foreach my $boolean qw(and or){
		foreach my $class_id (keys %{ $self->terms->{field_terms}->{$boolean} }){
			next unless $class_id;
			$host_is_filter++;
		}
		foreach my $term (sort keys %{ $self->terms->{any_field_terms}->{$boolean} }){
			next if $term =~ /^\(\@host \d+\)$/; # Don't count host here
			$host_is_filter++;
		}
	}
	if ($host_is_filter){
		$self->log->trace('Using host as a filter because there were ' . $host_is_filter . ' query terms.');
		foreach my $boolean qw(or and not){
			foreach my $term (sort keys %{ $self->terms->{any_field_terms}->{$boolean} }){
				if ($term =~ /^\(\@host \d+\)$/){
					$self->log->trace('Deleted term ' . $term);
					delete $self->terms->{any_field_terms}->{$boolean}->{$term};
				}
			}
		}
	}
#	elsif (not keys %{ $self->terms->{field_terms} } and not keys %{ $self->terms->{attr_terms} }){
#		foreach my $boolean qw(or and not){
#			foreach	my $candidate_term (keys %{ $self->terms->{any_field_terms}->{$boolean} }){
#				if ($candidate_term =~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/){
#					my $host_int = unpack('N*', inet_aton($candidate_term));
#					if ($self->user->is_permitted('host_id', $host_int)){
#						if ($self->archive){
#							# No good way of handling the and/or booleans for this in archive mode
#							$self->add_warning('Search will not include ' . $candidate_term . ' as a host, use host= for that if desired.');							
#						}
#						else {
#							$self->log->trace('adding host_int ' . $host_int);
#							delete $self->terms->{any_field_terms}->{$boolean}->{$candidate_term};
#							$self->terms->{any_field_terms}->{$boolean}->{'(@host ' . $host_int . '|' . $candidate_term . ')'} = 1; 
#							$self->highlights->{ _term_to_regex($candidate_term) } = 1;
#						}
#					}
#					else {
#						$self->log->warn("Insufficient permissions to query host_int $host_int");
#					}
#				}
#			}
#		}
#	}
	
	$self->log->trace("terms: " . Dumper($self->terms));
	$self->log->trace("classes: " . Dumper($self->classes));
	
	# Verify that we're still going to actually have query terms after the filtering has taken place	
	my $query_term_count = 0;
		
	foreach my $boolean qw(or and){
		$query_term_count += scalar keys %{ $self->terms->{any_field_terms}->{$boolean} }; 
	}
	
	# we might have a class-only query
	foreach my $class (keys %{ $self->classes->{distinct} }){
		unless ($num_removed_terms){ # this query used to have terms, so it wasn't really class-only
			$query_term_count++;
		}
	}
	
	$self->log->debug('query_term_count: ' . $query_term_count . ', num_added_terms: ' . $num_added_terms);
	
	unless ($query_term_count){
		die 'All query terms were stripped based on permissions or they were too common';
	}
	
	if ($self->meta_params->{start}){
		$self->start(sprintf('%d', $self->meta_params->{start}));
	}
	if ($self->meta_params->{end}){
		$self->end(sprintf('%d', $self->meta_params->{end}));
	}
	$self->log->debug('META_PARAMS: ' . Dumper($self->meta_params));
	
	# Adjust query time params as necessary
	if ($self->meta_params->{adjust_query_times}){
		if ($self->start < $self->node_info->{indexes_min}){
			$self->start = $self->node_info->{indexes_min};
			$self->log->warn("Given start time too early, adjusting to " 
				. epoch2iso($self->start));
		}
		elsif ($self->start > $self->node_info->{indexes_max}){
			$self->start = $self->node_info->{indexes_max} - $self->conf->get('sphinx/index_interval');
			$self->log->warn("Given start time too late, adjusting to " 
				. epoch2iso($self->start));
		}
	}
	
	# Failsafe for times
	if ($self->meta_params->{start} or $self->meta_params->{end}){
		unless ($self->start){
			$self->start(0);
			$self->log->trace('set start to 0');
		}
		unless ($self->end){
			$self->end(time());
			$self->log->trace('set end to ' . time());
		}
	}
	
	# Check to see if the query is after the latest end, but not in the future (this happens if the indexing process is backed up)
	if ($self->start and $self->start <= time() and $self->start > $self->node_info->{indexes_max} and $self->start > $self->node_info->{archive_max}){
		my $type = 'indexes';
		if ($self->node_info->{archive_max} > $self->node_info->{indexes_max}){
			$type = 'archive';
		}
		my $new_start_max = $self->node_info->{$type . '_start_max'};
		$self->log->warn('Adjusted start_int ' . $self->start . ' to ' . $new_start_max . ' because it was after ' . $self->node_info->{$type . '_max'});
		$self->start($new_start_max);
	}
	if ($self->end and $self->end <= time() and $self->end > $self->node_info->{indexes_max} and $self->end > $self->node_info->{archive_max}){
		my $type = 'indexes';
		if ($self->node_info->{archive_max} > $self->node_info->{indexes_max}){
			$type = 'archive';
		}
		my $new_max = $self->node_info->{$type . '_max'};
		$self->log->warn('Adjusted end_int ' . $self->end . ' to ' . $new_max);
		$self->end($new_max);
	}
	
	# Final sanity check
	unless (defined $self->start and $self->end and $self->start <= $self->end){
		die('Invalid start or end: ' . (scalar localtime($self->start)) . ' ' . (scalar localtime($self->end)));
	}
	
	$self->log->debug('going with times start: ' . (scalar localtime($self->start)) .  ' (' . $self->start . ') and end: ' .
		(scalar localtime($self->end)) . ' (' . $self->end . ')');
	
	return 1;
}

sub _parse_query_term {
	my $self = shift;
	my $terms = shift;
	my $given_operator = shift;
	
	$self->log->debug('terms: ' . Dumper($terms));
	
	foreach my $operator (keys %{$terms}){
		my $effective_operator = $operator;
		if ($given_operator){
			if ($given_operator eq '-' and ($effective_operator eq '' or $effective_operator eq '+')){
				$effective_operator = '-'; # invert the AND or OR
			}
			elsif ($given_operator eq '+' and $effective_operator eq '-'){
				$effective_operator = '-';
			}
		}
		
		my $arr = $terms->{$operator};
		foreach my $term_hash (@{$arr}){
			next unless defined $term_hash->{value};
			
			# Recursively handle parenthetical directives
			if (ref($term_hash->{value}) eq 'HASH'){
				$self->_parse_query_term($term_hash->{value}, $effective_operator);
				next;
			}
			
			# Escape any digit-dash-word combos (except for host or program)
			#$term_hash->{value} =~ s/(\d+)\-/$1\\\\\-/g unless ($self->archive or $term_hash->{field} eq 'program' or $term_hash->{field} eq 'host');
						
			if (lc($term_hash->{field}) eq 'start'){
				# special case for start/end
				$self->start(UnixDate($term_hash->{value}, "%s"));
				next;
			}
			elsif (lc($term_hash->{field}) eq 'end'){
				# special case for start/end
				$self->end(UnixDate($term_hash->{value}, "%s"));
				next;
			}
			elsif (lc($term_hash->{field}) eq 'limit'){
				# special case for limit
				$self->limit(sprintf("%d", $term_hash->{value}));
				next;
			}
			elsif (lc($term_hash->{field}) eq 'offset'){
				# special case for offset
				$self->offset(sprintf("%d", $term_hash->{value}));
				next;
			}
			elsif (lc($term_hash->{field}) eq 'class'){
				# special case for class
				my $class;
				$self->log->trace('classes: ' . Dumper($self->node_info->{classes}));
				if ($self->node_info->{classes}->{ uc($term_hash->{value}) }){
					$class = lc($self->node_info->{classes}->{ uc($term_hash->{value}) });
				}
				else {
					die("Unknown class $term_hash->{value}");
				}
				
				if ($effective_operator eq '-'){
					# We're explicitly removing this class
					$self->classes->{excluded}->{ $class } = 1;
				}
				else {
					$self->classes->{given}->{ $class } = 1;
				}
				$self->log->debug("Set operator $effective_operator for given class " . $term_hash->{value});		
				next;
			}
			elsif (lc($term_hash->{field}) eq 'groupby'){
				my $field_infos = $self->get_field($term_hash->{value});
				$self->log->trace('$field_infos ' . Dumper($field_infos));
				if ($field_infos or $term_hash->{value} eq 'node'){
					$self->add_groupby(lc($term_hash->{value}));
					foreach my $class_id (keys %$field_infos){
						$self->classes->{groupby}->{$class_id} = 1;
					}
					$self->log->trace("Set groupby " . Dumper($self->groupby));
				}
				next;
			}
			elsif (lc($term_hash->{field}) eq 'node'){
				if ($term_hash->{value} =~ /^[\w\.]+$/){
					if ($effective_operator eq '-'){
						$self->nodes->{excluded}->{ $term_hash->{value} } = 1;
					}
					else {
						$self->nodes->{given}->{ $term_hash->{value} } = 1;
					}
				}
				next;
			}
			elsif (lc($term_hash->{field}) eq 'cutoff'){
				$self->limit($self->cutoff(sprintf("%d", $term_hash->{value})));
				$self->log->trace("Set cutoff " . $self->cutoff);
				next;
			}
			elsif (lc($term_hash->{field}) eq 'datasource'){
				delete $self->datasources->{sphinx}; # no longer using our normal datasource
				$self->datasources->{ $term_hash->{value} } = 1;
				$self->log->trace("Set datasources " . Dumper($self->datasources));
				next;
			}
			elsif (lc($term_hash->{field}) eq 'nobatch'){
				$self->meta_params->{nobatch} = 1;
				$self->log->trace("Set batch override.");
				next;
			}
			elsif (lc($term_hash->{field}) eq 'livetail'){
				$self->meta_params->{livetail} = 1;
				$self->livetail(1);
				$self->log->trace("Set livetail.");
				next;
			}
			
			if ($self->archive){
				# Escape any special chars
				$term_hash->{value} =~ s/([^a-zA-Z0-9\.\_\-\@])/\\$1/g;
			}
			else {
				# Get rid of any non-indexed chars
				$term_hash->{value} =~ s/[^a-zA-Z0-9\.\@\-\_\\]/\ /g unless ($term_hash->{field} eq 'program' or $term_hash->{field} eq 'host');
				# Escape backslashes followed by a letter
				$term_hash->{value} =~ s/\\([a-zA-Z])/\ $1/g unless ($term_hash->{field} eq 'program' or $term_hash->{field} eq 'host');
				#$term_hash->{value} =~ s/\\\\/\ /g; # sphinx doesn't do this for some reason
				# Escape any '@' or sphinx will error out thinking it's a field prefix
				if ($term_hash->{value} =~ /\@/ and not $term_hash->{quote}){
					# need to quote
					$term_hash->{value} = '"' . $term_hash->{value} . '"';
				}
				# Escape any free-standing hypens
				$term_hash->{value} =~ s/^\-$/\\\-/g;
				$term_hash->{value} =~ s/([^a-zA-Z0-9\.\_\-\@])\-([^a-zA-Z0-9\.\_\-\@]*)/$1\\\\\-$2/g;
				# Sphinx can only handle numbers up to 15 places (though this is fixed in very recent versions)
				if ($term_hash->{value} =~ /^[0-9]{15,}$/){
					die('Integer search terms must be 15 or fewer digits, received ' 
						. $term_hash->{value} . ' which is ' .  length($term_hash->{value}) . ' digits.');
				}
				if($term_hash->{quote}){
					$term_hash->{value} = $self->normalize_quoted_value($term_hash->{value});
				}
			}
			$self->log->debug('term_hash value now: ' . $term_hash->{value});
			
			
			my $boolean = 'or';
				
			# Reverse if necessary
			if ($effective_operator eq '-' and $term_hash->{op} eq '!='){
				$boolean = 'and';
			}
			elsif ($effective_operator eq '-' and $term_hash->{op} eq '='){
				$boolean = 'not';
			}
			elsif ($effective_operator eq '+'){
				$boolean = 'and';
			}
			elsif ($effective_operator eq '-'){
				$boolean = 'not';
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
				
				# Set fields for searching
				if ($term_hash->{op} !~ /[\<\>]/){ # ignore ranges
					foreach my $class_id (keys %{ $values->{fields} }){
						foreach my $real_field (keys %{ $values->{fields}->{$class_id} }){
							$self->terms->{field_terms}->{$boolean}->{$class_id}->{$real_field} ||= [];
							push @{ $self->terms->{field_terms}->{$boolean}->{$class_id}->{$real_field} }, 
								$values->{fields}->{$class_id}->{$real_field};
						}	
					}
				}
				
				# Set attributes for searching
				foreach my $class_id (keys %{ $values->{attrs} }){
					# If not a range search, not already a field term, and not a class 0 attr, add text to any field search
					if ($term_hash->{op} !~ /[\<\>]/ and not exists $self->terms->{field_terms}->{$boolean}->{$class_id}
						and lc($term_hash->{field}) ne 'country_code'){ # one-off for weird way country_code works
						push @{ $self->terms->{any_field_terms}->{$boolean} }, $term_hash->{value} if $class_id; #skip class 0
					}
					my $field_info = $self->get_field($term_hash->{field})->{$class_id};
					unless ($field_info->{field_type}){
						$self->log->warn('No field_info for ' . $term_hash->{field} . ': ' . Dumper($field_info));
						next;
					}
					next if $field_info->{field_type} eq 'string'; # skip string attributes
					$self->terms->{attr_terms}->{$boolean}->{ $term_hash->{op} }->{ $term_hash->{field} } ||= {};
					foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
						$self->terms->{attr_terms}->{$boolean}->{ $term_hash->{op} }->{ $term_hash->{field} }->{$class_id}->{$real_field} ||= [];
						push @{ $self->terms->{attr_terms}->{$boolean}->{ $term_hash->{op} }->{ $term_hash->{field} }->{$class_id}->{$real_field} }, $values->{attrs}->{$class_id}->{$real_field};
					}
				}
			}				
				
			# Otherwise there was no field given, search all fields
			elsif (defined $term_hash->{value}){
				push @{ $self->terms->{any_field_terms}->{$boolean} }, $term_hash->{value};
			}
			else {
				die "no field or value given: " . Dumper($term_hash);
			}
		}
	}
	
	return 1;
}

sub cancel {
	my $self = shift;
	
	my ($query, $sth);
	$query = 'UPDATE query_log SET num_results=-2 WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($self->qid);
	return 1;
}

sub check_cancelled {
	my $self = shift;
	my ($query, $sth);
	$query = 'SELECT num_results FROM query_log WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($self->qid);
	my $row = $sth->fetchrow_hashref;
	if (defined $row->{num_results} and $row->{num_results} eq -2){
		$self->log->info('Query ' . $self->qid . ' has been cancelled');
		$self->cancelled(1);
		return 1;
	}
	return 0;
}

sub mark_batch_start {
	my $self = shift;
	# Record that we're starting so no one else starts it
	my ($query, $sth);
	$sth = $self->db->prepare('UPDATE query_log SET num_results=-1 WHERE qid=?');
	$sth->execute($self->qid);
	return $sth->rows;
}

1;
