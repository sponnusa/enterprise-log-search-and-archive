package ELSA::Node::Web;
use strict;
use warnings;
use Data::Dumper;
use Date::Manip;
use Socket;
use JSON;
use Time::HiRes qw(sleep time);
use IO::Socket::UNIX;
use POE qw(Wheel::Run Filter::Reference);
use Storable qw(dclone);
use IO::Socket;
use POE::Event::Message;
use POE::Filter::Reference;
use Data::Serializer;
use File::Slurp;
use Sphinx::Config;

BEGIN {
	$POE::Event::Message::Filter = new POE::Filter::Reference( 
		Data::Serializer->new(
			serializer => 'YAML::Syck',
			portable => 1,
		)
	);
}

use ELSA;
use ELSA::Exceptions;
use ELSA::Node;
use ELSA::Search;
use ELSA::Indexer;

require Exporter;
our @ISA = qw(Exporter ELSA::Node);

our %Inline_states = %ELSA::Node::Inline_states;
our @Object_states = (@ELSA::Node::Object_states, 
	qw(
		get_form_params
		query
		get_row_content
		archive_query
		node_archive_query
		cancel_node_archive_query
		cancel_query
		get_stats
		node_get_stats
	)
);
our $Timeout = 5;
our $Archive_query_rate = 95_238;

sub new {
	my $class = shift;
	my $self = $class->SUPER::new(@_);
	bless ($self, $class);
	
	$self->{_STOPWORDS} = $self->_get_stopwords();
	
	return $self;
}

sub _start {
	my ($self,$kernel,$heap,$session,$alias,$published_states) = @_[OBJECT,KERNEL,HEAP,SESSION,ARG0,ARG1];
	ELSA::Node::_start(@_);
}

sub _get_stopwords {
	my $self = shift;
	
	no warnings; # Sphinx::Config throws a bunch of uninitialized value warnings here which I can't fix
	my $sphinx_config = new Sphinx::Config();
	
	$sphinx_config->parse($self->conf->get('sphinx/config_file'));
	my $stopwords_file = $sphinx_config->get('index', 'permanent', 'stopwords');
	if ($stopwords_file and -f $stopwords_file){
		my $text = File::Slurp::slurp($stopwords_file);
		my @words = split(/\n/, $text);
		return { map { $_ => 1 } @words };
	}
	else {
		return {};
	}
}

sub get_form_params {
	my ($self,$kernel,$heap,$from,$msg) = @_[OBJECT,KERNEL,HEAP,ARG0..ARG1];
	$self->log->debug("from: " . Dumper($from) . ", msg: " . Dumper($msg));
	my $args = $msg->body();

	$self->log->debug("Getting form params with args " . Dumper($args));
	
	# Find min/max indexes
	my $indexer = new ELSA::Indexer($self->{_CONFIG_FILE});
	my $indexes = $indexer->compile_indexes();
	$indexer->db->disconnect();
	my $min = time();
	my $max = 0;
	foreach my $index_name (keys %$indexes){
		if ($indexes->{$index_name}->{start} < $min){
			$min = $indexes->{$index_name}->{start};
		}
		if ($indexes->{$index_name}->{end} > $max){
			$max = $indexes->{$index_name}->{end};
		}
	}
	
	my $elsa = new ELSA($self->{_CONFIG_FILE});
	$elsa->init_db();
	
	my $form_params = {
		start => epoch2iso($min),
		start_int => $min,
		end => epoch2iso($max),
		end_int => $max,
		classes => $elsa->get_classes(),
		fields => $elsa->get_fields(),
		programs => $elsa->get_programs()
	};
	$elsa->db->disconnect();
	
	if ($args->{permissions}){
		# this is for a user, restrict what gets sent back
		unless ($args->{permissions}->{class_id}->{0}){
			foreach my $class_id (keys %{ $form_params->{classes} }){
				unless ($args->{permissions}->{class_id}->{$class_id}){
					delete $form_params->{classes}->{$class_id};
				}
			}
		
			my $possible_fields = [ @{ $form_params->{fields} } ];
			$form_params->{fields} = [];
			for (my $i = 0; $i < scalar @$possible_fields; $i++){
				my $field_hash = $possible_fields->[$i];
				my $class_id = $field_hash->{class_id};
				if ($args->{permissions}->{class_id}->{$class_id}){
					push @{ $form_params->{fields} }, $field_hash;
				}
			}
		}
		
		unless ($args->{permissions}->{program_id}->{0}){
			foreach my $class_id (keys %{ $form_params->{programs} }){
				foreach my $program_id (keys %{ $form_params->{programs}->{$class_id} }){
					unless ($args->{permissions}->{class_id}->{$class_id} and $args->{permissions}->{program_id}->{$program_id}){
						delete $form_params->{programs}->{$class_id}->{$program_id};
					}	
				}
			}
		}
	}
	
	# Tack on the "ALL" and "NONE" special types
	unshift @{$form_params->{fields}}, 
		{'value' => 'ALL', 'text' => 'ALL', 'class_id' => 0 }, 
		{'value' => 'NONE', 'text' => 'NONE', 'class_id' => 1 };
	
	# Route for async
	$msg->body($form_params);
	$msg->route();
	
	# Return for sync
	return $form_params;
}

sub archive_query {
	my ($self,$kernel,$heap,$from,$msg) = @_[OBJECT,KERNEL,HEAP,ARG0..ARG1];
	my $args = $msg->body();
	
	$self->log->debug("args: " . Dumper($args));
	
	# Broadcast the query to all nodes
	foreach my $node (keys %{ $self->get_peers() }, $self->conf->get('manager/server_name')){
		my $node_msg = dclone($msg);
		my $response = new POE::Event::Message($node_msg, $args);
		$response->addRouteTo('post', 'agent', 'execute', 'web', 'node_archive_query' );
		$response->addRemoteRouteTo($node, $self->conf->get('manager/listen_port'), 'asynch');
		$self->log->debug('about to route: ' . Dumper($response));
		eval {	
			local $SIG{ALRM} = sub { die 'alarm'; };
			alarm $Timeout;
			$response->route();
			alarm 0;
		};
		if ($@){
			my $err = 'Node connection timed out after ' . $Timeout . ' seconds';
			my $response = new POE::Event::Message($msg, $err);
			$response->route();
			$self->log->error($err);
		}
	}
	return {};
}

sub cancel_query {
	my ($self,$kernel,$heap,$from,$msg) = @_[OBJECT,KERNEL,HEAP,ARG0..ARG1];
	my $args = $msg->body();
	
	$self->log->debug("args: " . Dumper($args));
	
	# Broadcast the query to all nodes
	foreach my $node (keys %{ $self->get_peers() }, 'localhost'){
		my $node_msg = dclone($msg);
		my $response = new POE::Event::Message($node_msg, $args);
		$response->addRouteTo('post', 'agent', 'execute', 'web', 'cancel_node_archive_query' );
		$response->addRemoteRouteTo($node, $self->conf->get('manager/listen_port'), 'asynch');
		$self->log->debug('about to route: ' . Dumper($response));
		eval {	
			local $SIG{ALRM} = sub { die 'alarm'; };
			alarm $Timeout;
			$response->route();
			alarm 0;
		};
		if ($@){
			my $err = 'Node connection timed out after ' . $Timeout . ' seconds';
			my $response = new POE::Event::Message($msg, $err);
			$response->route();
			$self->log->error($err);
		}
	}
	
	$msg->route(); # send the response back to the client
	
	return {};
}

sub cancel_node_archive_query {
	my ($self,$kernel,$heap,$from,$msg) = @_[OBJECT,KERNEL,HEAP,ARG0..ARG1];
	my $args = $msg->body();
	eval {
		my $qid = $args->{'qid'} or throw_e error => 'No qid given to cancel';
		my $search = new ELSA::Search($self->{_CONFIG_FILE});
		$search->break($qid, 1);
		$search->db->disconnect();
	};	
	if ($@){
		my $e = $@;
		my $err = $e;
		if (ref($e) and $e->can('as_string')){
			$err = $e->as_string();
		}
		my $response = new POE::Event::Message($msg, $err);
		$response->route();
		$self->log->error($e);
	}
	
	return {};
}	

sub node_archive_query {
	my ($self,$kernel,$heap,$from,$msg) = @_[OBJECT,KERNEL,HEAP,ARG0..ARG1];
	my $args = $msg->body();
	my $qid;
	
	$self->log->debug("args: " . Dumper($args));
	eval {
		my ($query_params, $query_meta_params);
		$qid = $args->{'qid'};
		$query_params = $args->{'query_params'};
		$query_meta_params = $args->{'query_meta_params'};
		
		throw_params param => 'qid', value => $qid
			unless $qid;
		
		throw_params param => 'query_str', value => $query_params
			unless $query_params;	
		
		# Timeout
		my $timeout = $self->conf->get('manager/query_timeout');
		if ($query_meta_params->{timeout}){
			$timeout = $query_meta_params->{timeout};
		}
		
		my $needed = $self->get_peers();
		$needed->{ $self->conf->get('manager/server_name') } = 1;
		my $query_info = { 
			qid => $qid,
			query_params => $query_params,
			query_meta_params => $query_meta_params,
			timestamp => time(),
			timeout => $timeout,
			node => $self->conf->get('manager/server_name'),
			needed => $needed,
		};
		
#		$self->log->debug("qid: $qid, queries: " . Dumper($heap->{queries}) . " rsvp: " 
#			. Dumper($heap->{queries}->{$qid}) . ", kernel id: " 
#			. $kernel->ID . " session_id: " . $_[SESSION]->ID);
	
		my $search = new ELSA::Search($self->{_CONFIG_FILE});
	
		my $ok = $search->query(
			{	
				query_string => $query_params, 
				query_meta_params => $query_meta_params,
				qid => $qid,
			}
		);
		unless ($ok){
			my $response = new POE::Event::Message($msg, {error => 'Unknown query error'});
			$response->routeBack();
			return { ok => 1 };
		}
		
		$query_info->{totalRecords} = $search->total_found();
		$query_info->{recordsReturned} = $search->total_returned();
		
		$query_info->{groups} = $search->groups();
		$query_info->{stats} = { 
			tree => $search->stats->execution_tree(), 
			timeline => $search->stats->timeline(),
		};
		$query_info->{results} = $search->results();
		$self->log->debug("query_info: " . Dumper($query_info));
		$search->db->disconnect();
		
		$msg->body($query_info);
		$msg->route();
	};
	if ($@){
		my $e = $@;
		my $err = $e;
		if (ref($e) and $e->can('as_string')){
			$err = $e->as_string();
		}
		my $response = new POE::Event::Message($msg, {error => $err, qid => $qid});
		$response->route();
		$self->log->error($e);
	}
	
	return {};
}

sub query {
	my ($self,$kernel,$heap,$from,$msg) = @_[OBJECT,KERNEL,HEAP,ARG0..ARG1];
	my $args = $msg->body();
	
	$self->log->debug("args: " . Dumper($args));
	eval {
		my ($qid, $query_params, $query_meta_params);
		$qid = $args->{'qid'};
		$query_params = $args->{'query_params'};
		$query_meta_params = $args->{'query_meta_params'};
		
		throw_params param => 'qid', value => $qid
			unless $qid;
		
		throw_params param => 'query_str', value => $query_params
			unless $query_params;	
		
		# Timeout
		my $timeout = $self->conf->get('manager/query_timeout');
		if ($query_meta_params->{timeout}){
			$timeout = $query_meta_params->{timeout};
		}
		my $indexer = new ELSA::Indexer($self->{_CONFIG_FILE});
		$query_meta_params->{indexes} = $indexer->compile_indexes();
		$indexer->db->disconnect();
		$self->log->debug('compiled indexes: ' . Dumper($query_meta_params->{indexes}));
		
		my $query_info = { 
			qid => $qid,
			query_params => $query_params,
			query_meta_params => $query_meta_params,
			timestamp => time(),
			timeout => $timeout,
			node => $self->conf->get('manager/server_name'),
			needed => {},
		};
		
#		$self->log->debug("qid: $qid, queries: " . Dumper($heap->{queries}) . " rsvp: " 
#			. Dumper($heap->{queries}->{$qid}) . ", kernel id: " 
#			. $kernel->ID . " session_id: " . $_[SESSION]->ID);
	
		my $search = new ELSA::Search($self->{_CONFIG_FILE});
		
		#$self->log->debug('stopwords: ' . Dumper($self->{_STOPWORDS}));
	
		my $ok = $search->query(
			{	
				query_string => $query_params, 
				query_meta_params => $query_meta_params,
				stopwords => $self->{_STOPWORDS},
			}
		);
		unless ($ok){
			my $response = new POE::Event::Message($msg, {error => 'Unknown query error'});
			$response->routeBack();
			return { ok => 1 };
		}
		
		$query_info->{totalRecords} = $search->total_found();
		$query_info->{recordsReturned} = $search->total_returned();
		$query_info->{warnings} = $search->warnings();
		$query_info->{errors} = $search->errors();
		
		$query_info->{groups} = $search->groups();
		$query_info->{stats} = { 
			tree => $search->stats->execution_tree(), 
			timeline => $search->stats->timeline(),
		};
		$self->log->debug("query_info: " . Dumper($query_info));
		
		my $start = time();
		
		# If we're only interested in groups, bypass all of the expensive data collection and sorting
		if ($query_meta_params->{groups_only}){
			my $response = new POE::Event::Message($msg, $query_info);
			$response->routeBack();
			return { ok => 1 };
		}
		
		my $peers = $search->conf->get('peers');
		$search->log->trace("got peers: " . Dumper($peers));
		unless ($peers){
			$peers = {};
		}
	
		my $rows_by_node = $search->sort_by_node({ peers => $peers });
		#$search->log->debug("rows_by_node: " . Dumper($rows_by_node));
		foreach my $node (keys %{$rows_by_node}){
			$query_info->{needed}->{$node} = 1;
		}
		
		if (scalar keys %{$rows_by_node}){
			foreach my $node (keys %{ $rows_by_node }){
				$query_info->{rows} = $rows_by_node->{$node};
				# we have to dclone because if we reuse $msg below, it refers to 
				#  the same instance throughout the loop which causes problems
				my $msg_template = dclone($msg); 
				#$self->log->debug('msg looks like : ' . Dumper($msg_template));
				my $response = new POE::Event::Message($msg_template, $query_info);
				$response->addRouteTo('post', 'agent', 'execute', 'web', 'get_row_content' );
				$response->addRemoteRouteTo($node, $self->conf->get('manager/listen_port'), 'asynch');
				#$self->log->debug('about to route: ' . Dumper($response));
				$response->route();
			}
		}
		else {
			$self->log->debug("no results found");
			$query_info->{results} = $search->results();
			my $response = new POE::Event::Message($msg, $query_info);
			$response->route();
		}
		$search->db->disconnect();
	}; 
	if ($@){
		my $e = $@;
		my $err = $e;
		if (ref($e) and $e->can('as_string')){
			$err = $e->as_string();
		}
		my $response = new POE::Event::Message($msg, $err);
		$response->route();
		$self->log->error($e);
	}
	
	return {};
}

sub get_row_content {
	my ($self, $kernel, $heap, $from, $msg) = @_[OBJECT, KERNEL, HEAP, ARG0..ARG1];
	my $args = $msg->body();
	
	#$self->log->debug('got msg: ' . Dumper($msg));
	$self->log->debug("Getting row content for qid $args->{qid}");
	my $search = new ELSA::Search($self->{_CONFIG_FILE});
	eval {
		$search->get_row_content(
			{
				rows => $args->{rows},
			}
		);
	};
	if ($@){
		my $e = $@;
		my $errstr;
		if ($e->isa('ELSA::Exception::Param')){
			$errstr = sprintf("[%s] Error: %s\nParam: %s, Value; %s\nTrace: %s\n", 
				scalar localtime($e->time), $e->description, 
				$e->param, $e->value, $e->trace->as_string());	
		}
		elsif ($e->isa('ELSA::Exception')){
			$errstr = sprintf("[%s] Error: %s\nTrace: %s\n", 
				scalar localtime($e->time), $e->message, 
				$e->trace->as_string());	
		}
		else {
			$errstr = sprintf("[%s] Error: %s", 
				scalar localtime(), $@);	
		}
		
		$self->log->error("get_row_content produced error: $errstr");
	}
	$self->log->debug("Found " . (scalar @{ $search->results() }) . " rows for qid $args->{qid}");
	#$self->log->debug(Dumper($search->results()));
	
	$args->{results} = $search->results();
	$args->{node} = $self->conf->get('manager/server_name');
	# Strip off the useless raw rows
	delete $args->{rows};
	$msg->body($args);
	#$self->log->debug('routing response: ' . Dumper($msg));
	$msg->route();
	
	$self->log->debug("Finished with get_row_content");
	$search->db->disconnect();
	return $args; #for call/sync
}

sub get_stats {
	my ($self, $kernel, $heap, $from, $msg) = @_[OBJECT, KERNEL, HEAP, ARG0..ARG1];
	my $args = $msg->body();
	
	$self->log->debug("args: " . Dumper($args));
	
	my $data = {};
	
	# Broadcast the query to all nodes
	foreach my $node (keys %{ $self->get_peers() }, $self->conf->get('manager/server_name')){
		my $response = POE::Event::Message->package($args);
		$response->addRouteTo('post', 'agent', 'execute', 'web', 'node_get_stats' );
		$response->addRemoteRouteTo($node, $self->conf->get('manager/listen_port'), 'sync');
		$response->setMode('call');
		#TODO make this parallel
		$self->log->debug('about to route: ' . Dumper($response));
		my $Timeout = 60; # stats might take longer than usual, so override package var
		eval {	
			local $SIG{ALRM} = sub { die 'alarm'; };
			alarm $Timeout;
			my ($ret) = $response->route();
			#$self->log->debug('got ret from node ' . $node . ': ' . Dumper($ret));
			alarm 0;
			$data->{$node} = $ret->body();
		};
		if ($@){
			my $err = 'Node connection timed out after ' . $Timeout . ' seconds';
			my $response = new POE::Event::Message($msg, $err);
			$response->route();
			$self->log->error($err);
		}
	}
	
	$msg->body($data);
	$msg->route();
	
	return $data;
}

sub node_get_stats {
	my ($self, $kernel, $heap, $from, $msg) = @_[OBJECT, KERNEL, HEAP, ARG0..ARG1];
	my $args = $msg->body();
	my $state = $_[STATE];
	
	eval {
		unless ($args->{start}){
			$args->{start} = epoch2iso(0);
		}
		unless ($args->{end}){
			$args->{end} = epoch2iso(time());
		}
		$args->{results} = $self->_get_stats($args);
	};
	if ($@){
		my $e = $@;
		my $errstr;
		if ($e->isa('ELSA::Exception::Param')){
			$errstr = sprintf("[%s] Error: %s\nParam: %s, Value; %s\nTrace: %s\n", 
				scalar localtime($e->time), $e->description, 
				$e->param, $e->value, $e->trace->as_string());	
		}
		elsif ($e->isa('ELSA::Exception')){
			$errstr = sprintf("[%s] Error: %s\nTrace: %s\n", 
				scalar localtime($e->time), $e->message, 
				$e->trace->as_string());	
		}
		else {
			$errstr = sprintf("[%s] Error: %s", 
				scalar localtime(), $@);	
		}
		
		$self->log->error("$state produced error: $errstr");
	}
	
	$msg->body($args);
	#$self->log->debug('routing response: ' . Dumper($msg));
	$msg->route();
	
	$self->log->debug("Finished with $state");
	
	return $args; #for call/sync
}

sub _get_stats {
	my $self = shift;
	my $args = shift;
	throw_params param => 'args', value => $args unless $args and ref($args) and ref($args) eq 'HASH';
	throw_params param => 'start', value => $args unless $args->{start};
	throw_params param => 'end', value => $args unless $args->{end};
	
	my $intervals = 100;
	if ($args->{intervals}){
		$intervals = sprintf('%d', $args->{intervals});
	}
	
	my $indexer = new ELSA::Indexer($self->{_CONFIG_FILE});
	my ($query, $sth, $ret);
	
	my $arch_size = $indexer->get_current_archive_size();
	my $idx_size = $indexer->get_current_index_size();
	my $indexes = $indexer->current_indexes();
	
	# Find min/max archive time range
	my $times = {};
	$query = 'SELECT MIN(start) AS start, MAX(end) AS end, UNIX_TIMESTAMP(MAX(end)) - UNIX_TIMESTAMP(MIN(start)) AS total' . "\n" .
		'FROM tables WHERE table_type_id=(SELECT id FROM table_types WHERE table_type=?)';
	$sth = $indexer->db->prepare($query);
	foreach my $item qw(index archive){
		$sth->execute($item);
		my $row = $sth->fetchrow_hashref;
		$times->{$item} = $row;
		unless ($times->{$item}->{total}){
			$times->{$item}->{total} = 1;
		}
	}
	
	# Get the currently running queries
	$query = 'SHOW FULL PROCESSLIST';
	$sth = $indexer->db->prepare($query);
	$sth->execute();
	my @queries;
	while (my $row = $sth->fetchrow_hashref){
		next if $row->{Info} eq 'SHOW FULL PROCESSLIST';
		push @queries, $row;
	}
	
	# Get load stats
	my $load_stats = {};
	foreach my $item qw(load archive index){
		$load_stats->{$item} = {
			data => {
				x => [],
				LogsPerSec => [],
				KBytesPerSec => [],
			},
		};
		
		$query = 'SELECT MIN(bytes) AS min_bytes, AVG(bytes) AS avg_bytes, MAX(bytes) AS max_bytes,' . "\n" .
			'MIN(count) AS min_count, AVG(count) AS avg_count, MAX(count) AS max_count,' . "\n" .
			'UNIX_TIMESTAMP(MAX(timestamp))-UNIX_TIMESTAMP(MIN(timestamp)) AS total_time, UNIX_TIMESTAMP(MIN(timestamp)) AS earliest' . "\n" .
			'FROM stats WHERE type=? AND timestamp BETWEEN ? AND ?';
		$sth = $indexer->db->prepare($query);
		$sth->execute($item, $args->{start}, $args->{end});
		$load_stats->{$item}->{summary} = $sth->fetchrow_hashref;
		
		$query = 'SELECT UNIX_TIMESTAMP(timestamp) AS ts, timestamp, bytes, count FROM stats WHERE type=? AND timestamp BETWEEN ? AND ?';
		$sth = $indexer->db->prepare($query);
		$sth->execute($item, $args->{start}, $args->{end});
		
		# arrange in the number of buckets requested
		my $bucket_size = ($load_stats->{$item}->{summary}->{total_time} / $intervals);
		while (my $row = $sth->fetchrow_hashref){
			my $ts = $row->{ts} - $load_stats->{$item}->{summary}->{earliest};
			my $bucket = int(($ts - ($ts % $bucket_size)) / $bucket_size);
			# Sanity check the bucket because too large an index array can cause an OoM error
			if ($bucket > $intervals){
				throw_e error => 'Bucket ' . $bucket . ' with bucket_size ' . $bucket_size . ' and ts ' . $row->{ts} . ' was greater than intervals ' . $intervals;
			}
			unless ($load_stats->{$item}->{data}->{x}->[$bucket]){
				$load_stats->{$item}->{data}->{x}->[$bucket] = $row->{timestamp};
			}
			
			unless ($load_stats->{$item}->{data}->{LogsPerSec}->[$bucket]){
				$load_stats->{$item}->{data}->{LogsPerSec}->[$bucket] = 0;
			}
			$load_stats->{$item}->{data}->{LogsPerSec}->[$bucket] += ($row->{count} / $bucket_size);
			
			unless ($load_stats->{$item}->{data}->{KBytesPerSec}->[$bucket]){
				$load_stats->{$item}->{data}->{KBytesPerSec}->[$bucket] = 0;
			}
			$load_stats->{$item}->{data}->{KBytesPerSec}->[$bucket] += ($row->{bytes} / 1024 / $bucket_size);
		}
	}	
	
	$ret = {
		archive => $arch_size,
		index => $idx_size,
		allocated_space => $self->conf->get('log_size_limit'),
		archive_ratio => ($arch_size / ($idx_size + 1)), # add 1 to make sure we don't div by zero
		capacity => ($self->conf->get('log_size_limit') - ($arch_size + $idx_size)),
		archive => {
			bytes_per_second => ($arch_size / $times->{archive}->{total}),
			start_time => $times->{archive}->{start},
			end_time => $times->{archive}->{end},
			time_span => $times->{archive}->{total},
		},
		index => {
			bytes_per_second => ($idx_size / $times->{index}->{total}),
			time_span => $times->{index}->{total},
			start_time => $times->{index}->{start},
			end_time => $times->{index}->{end},
		},
		sql_queries => \@queries,
		load_stats => $load_stats,
	};	
	
	
	$self->log->debug('Found stats: ' . Dumper($ret));
	return $ret;
	
	$indexer->db->disconnect();
}

__END__
