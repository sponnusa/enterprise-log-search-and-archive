package Query;
use Moose;
with 'MooseX::Traits';
with 'Utils';
with 'Fields';
with 'Warnings';
with 'MooseX::Clone';
use Results;
use Time::HiRes;
use Data::Dumper;
use Search::QueryParser;
use Storable qw(dclone);
use Socket;
use Log::Log4perl::Level;
use Date::Manip;
use Try::Tiny;
use Ouch qw(:trytiny);;
use String::CRC32;

# Object for dealing with user queries

our $Default_limit = 100;

# Required
has 'user' => (is => 'rw', isa => 'User', required => 1);
has 'parser' => (is => 'rw', isa => 'Object', required => 1);

# Required with defaults
has 'meta_params' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'type' => (is => 'rw', isa => 'Str', required => 1, default => 'index');
has 'results' => (is => 'rw', isa => 'Results', required => 1);
has 'start_time' => (is => 'ro', isa => 'Num', required => 1, default => sub { Time::HiRes::time() });
has 'groupby' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { has_groupby => 'count', all_groupbys => 'elements', add_groupby => 'push' });
has 'orderby' => (is => 'rw', isa => 'Str');
has 'orderby_dir' => (is => 'rw', isa => 'Str', required => 1, default => 'ASC');
has 'timeout' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'cancelled' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'archive' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'livetail' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'analytics' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'system' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'batch' => (is => 'rw', isa => 'Bool', required => 1, default => 0, trigger => \&_set_batch);
has 'limit' => (is => 'rw', isa => 'Int', required => 1, default => $Default_limit);
has 'offset' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'start' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'end' => (is => 'rw', isa => 'Int', required => 1, default => sub { time() });
has 'cutoff' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'transforms' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { has_transforms => 'count', all_transforms => 'elements', num_transforms => 'count' });
has 'connectors' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { has_connectors => 'count', all_connectors => 'elements', num_connectors => 'count',
		connector_idx => 'get', add_connector => 'push' });
has 'connector_params' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { has_connector_params => 'count', all_connector_params => 'elements', num_connector_params => 'count',
		connector_params_idx => 'get', add_connector_params => 'push' });
has 'terms' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'nodes' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { { given => {}, excluded => {} } });
has 'hash' => (is => 'rw', isa => 'Str', required => 1, default => '');
has 'highlights' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'stats' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'timezone_difference' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { { start => 0, end => 0 } });
has 'peer_requests' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'import_search_terms' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { 'has_import_search_terms' => 'count', 'all_import_search_terms' => 'elements' });
has 'id_ranges' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { 'has_id_ranges' => 'count', 'all_id_ranges' => 'elements' });
has 'max_query_time' => (is => 'rw', isa => 'Int', required => 1, default => 0);
has 'program_translations' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'use_sql_regex' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'original_timeout' => (is => 'rw', isa => 'Int', required => 1, default => 0);

# Optional
has 'query_string' => (is => 'rw', isa => 'Str');
has 'qid' => (is => 'rw', isa => 'Int');
has 'schedule_id' => (is => 'rw', isa => 'Int');
has 'raw_query' => (is => 'rw', isa => 'Str');
has 'comments' => (is => 'rw', isa => 'Str');
has 'time_taken' => (is => 'rw', isa => 'Num', trigger => \&_set_time_taken);
has 'batch_message' => (is => 'rw', isa => 'Str');
has 'node_info' => (is => 'rw', isa => 'HashRef');
#has 'import_groupby' => (is => 'rw', isa => 'Str');
has 'peer_label' => (is => 'rw', isa => 'Str');
has 'from_peer' => (is => 'rw', isa => 'Str');

sub BUILDARGS {
	my $class = shift;
	my %params = @_;
	
	$params{results} ||= new Results();
	
	return \%params;
}

sub BUILD {
	my $self = shift;
	
	my ($query, $sth);
	
	unless (defined $self->query_string){
		# We may just be constructing this query as scaffolding for other things
		return $self;
	}
	
	if ($self->has_groupby){
		$self->results( new Results::Groupby() );
	}
	
	if ($self->qid){
		$self->resolve_field_permissions($self->user); # finish this up from BUILDARGS now that we're blessed
		# Verify that this user owns this qid
		$query = 'SELECT qid FROM query_log WHERE qid=? AND uid=?';
		$sth = $self->db->prepare($query);
		$sth->execute($self->qid, $self->user->uid);
		my $row = $sth->fetchrow_hashref;
		throw(403, 'User is not authorized for this qid', { user => $self->user->username }) unless $row;
		$self->log->level($ERROR) unless $self->conf->get('debug_all');
	}
	else {
		# Log the query
		$self->db->begin_work;
		$query = 'INSERT INTO query_log (uid, query, system, archive) VALUES (?, ?, ?, ?)';
		$sth = $self->db->prepare($query);
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
	
	$self->hash($self->_get_hash($self->qid));
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
		orderby_dir => $self->orderby_dir,
		query_string => $self->query_string,
		query_meta_params => $self->meta_params,
		hash => $self->hash,
		highlights => $self->highlights,
		stats => $self->stats,
		approximate => $self->results->is_approximate,
	};
	
	$ret->{query_meta_params}->{archive} = 1 if $self->archive;
	$ret->{query_meta_params}->{livetail} = 1 if $self->livetail;
	
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

#sub _set_time_taken {
#	my ( $self, $new_val, $old_val ) = @_;
#	my ($query, $sth);
#	
#	# Update the db to ack
#	$query = 'UPDATE query_log SET num_results=?, milliseconds=? '
#	  		. 'WHERE qid=?';
#	$sth = $self->db->prepare($query);
#	$sth->execute( $self->results->records_returned, $new_val, $self->qid );
#	
#	return $sth->rows;
#}

sub set_directive {
	my $self = shift;
	my $directive = shift;
	my $value = shift;
	my $op = shift;
	$op ||= '=';
	
	if ($directive eq 'start'){
		# special case for start/end
		if ($value =~ /^\d+$/){
			$self->start(int($value));
		}
		else {
			#$self->start(UnixDate(ParseDate($value), "%s"));
			my $tz_diff = $self->parser->timezone_diff($value);
			$self->start(UnixDate(ParseDate($value), "%s") + $tz_diff);
		}
		$self->log->debug('start is now: ' . $self->start .', ' . (scalar localtime($self->start)));
	}
	elsif ($directive eq 'end'){
		# special case for start/end
		if ($value =~ /^\d+$/){
			$self->end(int($value));
		}
		else {
			my $tz_diff = $self->parser->timezone_diff($value);
			$self->end(UnixDate(ParseDate($value), "%s") + $tz_diff);
		}
	}
	elsif ($directive eq 'limit'){
		# special case for limit
		$self->limit(sprintf("%d", $value));
		throw(400, 'Invalid limit', { term => 'limit' }) unless $self->limit > -1;
	}
	elsif ($directive eq 'offset'){
		# special case for offset
		$self->offset(sprintf("%d", $value));
		throw(400, 'Invalid offset', { term => 'offset' }) unless $self->offset > -1;
	}
	elsif ($directive eq 'class'){
		# special case for class
		my $class;
		$self->log->trace('classes: ' . Dumper($self->node_info->{classes}));
		if ($self->node_info->{classes}->{ uc($value) }){
			$class = lc($self->node_info->{classes}->{ uc($value) });
		}
		elsif (uc($value) eq 'ANY'){
			my @classes;
			foreach my $class_name (keys %{ $self->node_info->{classes} }){
				next if $class_name eq 'ANY';
				push @classes, { field => 'class', value => $class_name, op => $op };
			}
			$self->_parse_query_term({ '' => \@classes }, $op);
		}
		else {
			throw(400, "Unknown class $value", { term => $value });
		}
		
		if ($op eq '-'){
			# We're explicitly removing this class
			$self->classes->{excluded}->{ $class } = 1;
		}
		else {
			$self->classes->{given}->{ $class } = 1;
		}
		$self->log->debug("Set operator $op for given class " . $value);		
	}
	elsif ($directive eq 'groupby'){
		my $value = lc($value);
		#TODO implement groupby import with new import system
		my $field_infos = $self->get_field($value);
		$self->log->trace('$field_infos ' . Dumper($field_infos));
		if ($field_infos or $value eq 'node'){
			$self->add_groupby(lc($value));
			foreach my $class_id (keys %$field_infos){
				$self->classes->{groupby}->{$class_id} = 1;
			}
			$self->log->trace("Set groupby " . Dumper($self->groupby));
		}
	}
	elsif ($directive eq 'orderby'){
		my $value = lc($value);
		my $field_infos = $self->get_field($value);
		$self->log->trace('$field_infos ' . Dumper($field_infos));
		if ($field_infos or $value eq 'node'){
			$self->orderby($value);
			foreach my $class_id (keys %$field_infos){
				$self->classes->{groupby}->{$class_id} = 1;
			}
			$self->log->trace("Set orderby " . Dumper($self->orderby));
		}
	}
	elsif ($directive eq 'orderby_dir'){
		if (uc($value) eq 'DESC'){
			$self->orderby_dir('DESC');
		}
	}
	elsif ($directive eq 'node'){
		if ($value =~ /^[\w\.\:]+$/){
			if ($op eq '-'){
				$self->nodes->{excluded}->{ $value } = 1;
			}
			else {
				$self->nodes->{given}->{ $value } = 1;
			}
		}
	}
	elsif ($directive eq 'cutoff'){
		$self->limit($self->cutoff(sprintf("%d", $value)));
		throw(400, 'Invalid cutoff', { term => 'cutoff' }) unless $self->cutoff > -1;
		$self->log->trace("Set cutoff " . $self->cutoff);
	}
	elsif ($directive eq 'datasource'){
		delete $self->datasources->{sphinx}; # no longer using our normal datasource
		$self->datasources->{ $value } = 1;
		$self->log->trace("Set datasources " . Dumper($self->datasources));
	}
	elsif ($directive eq 'nobatch'){
		$self->meta_params->{nobatch} = 1;
		$self->log->trace("Set batch override.");
	}
	elsif ($directive eq 'livetail'){
		$self->meta_params->{livetail} = 1;
		$self->livetail(1);
		$self->archive(1);
		$self->log->trace("Set livetail.");
	}
	elsif ($directive eq 'archive'){
		$self->meta_params->{archive} = 1;
		$self->archive(1);
		$self->log->trace("Set archive.");
		next;
	}
	elsif ($directive eq 'analytics'){
		$self->meta_params->{analytics} = 1;
		$self->analytics(1);
		$self->log->trace("Set analytics.");
	}
	else {
		throw(400, 'Invalid directive', { term => $directive });
	}
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

sub mark_livetail_start {
	my $self = shift;
	# Record that we're starting so no one else starts it
	$self->log->trace('marked livetail start');
	my ($query, $sth);
	$sth = $self->db->prepare('UPDATE query_log SET num_results=-3 WHERE qid=?');
	$sth->execute($self->qid);
	return $sth->rows;
}


sub convert_to_archive {
	my $self = shift;
	$self->query_term_count(0);
	foreach my $boolean (qw(and or not)){
		foreach my $term (keys %{ $self->terms->{any_field_terms}->{$boolean} }){ 
			# Drop the term
			delete $self->terms->{any_field_terms}->{$boolean}->{$term};
			my $sphinx_term = $term;
			# Make sphinx term SQL term
			if ($sphinx_term =~ /^\(\@(class|host|program) (\d+)\)$/){
				$self->terms->{attr_terms}->{$boolean}->{'='}->{0}->{ $Fields::Field_order_to_meta_attr->{ $Fields::Field_to_order->{$1} } } = $2;
			}
			else {
				$self->terms->{any_field_terms_sql}->{$boolean}->{$term} = $sphinx_term;
			}
		}
	}
	# Put the field_terms_sql back to field_terms now that we've done the count
	if ($self->terms->{field_terms_sql}){
		foreach my $boolean (keys %{ $self->terms->{field_terms_sql} }){
			foreach my $class_id (keys %{ $self->terms->{field_terms_sql}->{$boolean} }){
				foreach my $raw_field (keys %{ $self->terms->{field_terms_sql}->{$boolean}->{$class_id} }){
					foreach my $term (@{ $self->terms->{field_terms_sql}->{$boolean}->{$class_id}->{$raw_field} }){
						push @{ $self->terms->{field_terms}->{$boolean}->{$class_id}->{$raw_field} }, $term;
					}
				}
			}
		}
	}
	delete $self->terms->{field_terms_sql};
}

sub dedupe_warnings {
	my $self = shift;
	my %uniq;
	foreach my $warning ($self->all_warnings){
		my $key;
		if (blessed($warning)){
			$key = $warning->code . $warning->message . $self->json->encode($warning->data);
		}
		elsif (ref($warning)){
			$key = $warning->{code} . $warning->{message} . $self->json->encode($warning->{data});
		}
		else {
			$self->log->warn('Improperly formatted warning received: ' . $warning);
			$key = $warning;
			$warning = { code => 500, message => $warning, data => {} };
		}
		$uniq{$key} ||= [];
		push @{ $uniq{$key} }, $warning;
	}
	
	my @dedupe;
	foreach my $key (keys %uniq){
		push @dedupe, $uniq{$key}->[0];
	}
	$self->warnings([@dedupe]);
}

sub filter_stopwords {
	my $self = shift;
	my $record = shift;
	
	# Filter any records which have stopwords
	if (scalar keys %{ $self->terms->{any_field_terms_sql}->{and} }){
		my $to_find = scalar keys %{ $self->terms->{any_field_terms_sql}->{and} };
		STOPWORD_LOOP: foreach my $stopword (keys %{ $self->terms->{any_field_terms_sql}->{and} }){
			my $regex = QueryParser::_term_to_regex($stopword);
			foreach my $field (qw(msg program node host class)){
				if ($record->{$field} =~ qr/$regex/i){
					$self->log->debug('Found stopword: ' . $stopword . ' for term ' . $record->{$field} . ' and field ' . $field);
					$to_find--;
					last STOPWORD_LOOP;
				}
			}
			foreach my $field_hash (@{ $record->{_fields} }){
				if ($field_hash->{value} =~ qr/$regex/i){
					$self->log->debug('Found stopword: ' . $stopword . ' for term ' . $field_hash->{value});
					$to_find--;
					last STOPWORD_LOOP;
				}
			}
		}
		return 0 if $to_find;
	}
	
	if (scalar keys %{ $self->terms->{any_field_terms_sql}->{not} }){
		foreach my $stopword (keys %{ $self->terms->{any_field_terms_sql}->{not} }){
			my $regex = QueryParser::_term_to_regex($stopword);
			foreach my $field (qw(msg program node host class)){
				if ($record->{$field} =~ qr/$regex/i){
					$self->log->debug('Found not stopword: ' . $stopword . ' for term ' . $record->{$field} . ' and field ' . $field);
					return 0;
				}
			}
			foreach my $field_hash (@{ $record->{_fields} }){
				if ($field_hash->{value} =~ qr/$regex/i){
					$self->log->debug('Found not stopword: ' . $stopword . ' for term ' . $field_hash->{value});
					return 0;
				}
			}
		}
	}
	return 1;
}

sub has_stopword_terms {
	my $self = shift;
	my $terms_count = (scalar keys %{ $self->terms->{any_field_terms_sql}->{and} }) + (scalar keys %{ $self->terms->{any_field_terms_sql}->{not} });
	if ($self->terms->{field_terms_sql}){
		foreach my $boolean (keys %{ $self->terms->{field_terms_sql} }){
			foreach my $class_id (keys %{ $self->terms->{field_terms_sql}->{$boolean} }){
				foreach my $raw_field (keys %{ $self->terms->{field_terms_sql}->{$boolean}->{$class_id} }){
					foreach my $term (@{ $self->terms->{field_terms_sql}->{$boolean}->{$class_id}->{$raw_field} }){
						$terms_count++;
					}
				}
			}
		}
	}
	return $terms_count;
}


1;
