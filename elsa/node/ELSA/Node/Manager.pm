package ELSA::Node::Manager;
use strict;
use warnings;
use Data::Dumper;
use Time::HiRes qw(sleep time);
use POE qw(Wheel::Run Filter::Reference );

use ELSA::Exceptions;
use ELSA::Node;
use ELSA::Writer;
use ELSA::Search;
use ELSA::Indexer;

require Exporter;
our @ISA = qw(Exporter ELSA::Node);
our %Inline_states = %ELSA::Node::Inline_states;
our @Published_states = qw( dequeue batch_processed records_loaded 
	got_ELSA_stats get_stats 
	enqueue_dir get_oldest_in_progress
	get_msg_ids add_to_cache exec_callbacks
	get_current_ids
	get_master recv_master 
	count_records recv_record_count 
	sync_ids sync_complete update_ids
	get_peers
	relay_cache_reply
	drop_indexes drop_records
	peer_connection_timeout
	 );
our @Object_states = (
	@ELSA::Node::Object_states,
	qw(
		load_buffers
		_load_buffers
		get_stats
		load_records 
		archive_records
		rotate_logs
		add_programs
		consolidate_indexes
		get_indexes
		update_indexes
		current_indexes
		get_all_indexes
		_get_all_indexes
	 )
);
our $Max_consecutive_buffer_loads = 5;

sub new {
	my $class = shift;
	my $self = $class->SUPER::new(@_);
	$self->{_START_TIME} = time();
	$self->{_BATCH_COUNTERS} = {};
	$self->{_LOAD_COUNTER} = 1;
	$self->{_STATS} = { 
		records_processed => {},
		queue_size => {},
	};
	return bless($self, $class);
}



sub _start {
	my @orig_args = @_;
	my ($self,$kernel,$heap,$session,$alias,$published_states) = @_[OBJECT,KERNEL,HEAP,SESSION,ARG0,ARG1];
	ELSA::Node::_start(@orig_args);
	
	# Setup our signals
	$kernel->sig( 'DIE', 'handle_exception');
	$kernel->sig( 'TERM', '_stop');
	$kernel->sig( 'INT', '_stop');
	$kernel->sig( 'CHLD', '_child' );
	
	$kernel->yield('_load_buffers');
	# Get initial indexes from our peers and start the get_all_indexes loop
	$kernel->yield('_get_all_indexes');
}

sub _stop {
	my @orig_args = @_;
	my ($self, $kernel, $heap) = @_[OBJECT,KERNEL,HEAP];
	ELSA::Node::_stop(@orig_args);
	
	my $run_time = time() - $self->{_START_TIME};
	$self->log->info("Total Run Time: $run_time");
	my $overall_records = 0;
	foreach my $class_id (sort keys %{$self->{_STATS}->{records_processed}}){
		$overall_records += $self->{_STATS}->{records_processed}->{$class_id};
		$self->log->info(sprintf("Class $class_id records processed: %d, (%.5f RPS)",
			$self->{_STATS}->{records_processed}->{$class_id},
			$self->{_STATS}->{records_processed}->{$class_id} / $run_time
		));	
	}
	$self->log->info(sprintf("Total records: %d, (%.5f RPS)",
		$overall_records, $overall_records / $run_time));
	exit;
}

sub load_buffers {
	my ($self,$kernel,$heap,$session,$from,$msg) = @_[OBJECT,KERNEL,HEAP,SESSION,ARG0..ARG1];
	
	eval {
		my ($query, $sth);
		my $elsa = new ELSA($self->{_CONFIG_FILE});
		$elsa->init_db();
		
		$query = sprintf('SELECT id, filename FROM %s.buffers WHERE ISNULL(pid) ORDER BY id ASC LIMIT ?', $ELSA::Meta_db_name);
		$sth = $elsa->db->prepare($query);
		$sth->execute($Max_consecutive_buffer_loads);
		$query = sprintf('UPDATE %s.buffers SET pid=? WHERE id=?', $ELSA::Meta_db_name);
		
		my @rows;
		while (my $row = $sth->fetchrow_hashref){
			push @rows, $row;
			my $sth = $elsa->db->prepare($query);
			$sth->execute($$, $row->{id});
			$sth->finish();
		}
		$elsa->db->disconnect();
		
		foreach my $row (@rows){	
			# Send to index load records
			if ($self->conf->get('sphinx/perm_index_size')){
				my $msg = POE::Event::Message->package( { file => $row->{filename} } );
				$msg->addRouteTo('asynch', 'agent', 'execute', 'manager', 'load_records');
				$msg->addRemoteRouteTo('localhost', $self->conf->get('manager/listen_port'), 'asynch');
				$self->log->trace('Sending file ' . $row->{filename} . ' to load_records');
				$msg->route();
			}
			
			# Send to archive
			if ($self->conf->get('archive/percentage')){
				my $msg = POE::Event::Message->package( { file => $row->{filename} } );
				$msg->addRouteTo('asynch', 'agent', 'execute', 'manager', 'archive_records');
				$msg->addRemoteRouteTo('localhost', $self->conf->get('manager/listen_port'), 'asynch');
				$self->log->trace('Sending file ' . $row->{filename} . ' to archive_records');
				$msg->route();
			}
		}
		
		# Queue a rotate logs job
		my $msg = POE::Event::Message->package( {} );
		$msg->addRouteTo('asynch', 'agent', 'execute', 'manager', 'rotate_logs');
		$msg->addRemoteRouteTo('localhost', $self->conf->get('manager/listen_port'), 'asynch');
		$msg->route();
	};
	if ($@){
		my $e = $@;
		if (ref($e) and $e->can('as_string')){
			$self->log->error('Got exception: ' . $e->as_string());
		}
		else {
			$self->log->error('Got exception: ' . Dumper($e));
		}
	}
	return {};
}

sub _load_buffers {
	my ($self,$kernel,$heap,$session,$state) = @_[OBJECT,KERNEL,HEAP,SESSION,STATE];

	# Queue a forked load buffers job
	my $msg = POE::Event::Message->package( {} );
	$msg->addRouteTo('asynch', 'agent', 'execute', 'manager', 'load_buffers');
	$msg->addRemoteRouteTo('localhost', $self->conf->get('manager/listen_port'), 'asynch');
	$msg->route();

	# Reset the schedule alarm
	$kernel->alarm_add($state, $self->conf->get('sphinx/index_interval') + ((time() - (time() % $self->conf->get('sphinx/index_interval')))));
}

sub rotate_logs {
	my ($self,$kernel,$heap,$session,$from,$msg) = @_[OBJECT,KERNEL,HEAP,SESSION,ARG0..ARG1];
	
	eval {
		# Validate input
		unless ($msg->body and ref($msg->body) eq 'HASH'){ 
			throw_e error => 'Invalid args: ' . Dumper($msg->body);
		}
		
		# Rotate logs as necessary
		my $indexer = new ELSA::Indexer($self->{_CONFIG_FILE});
		$indexer->rotate_logs();
		$indexer->db->disconnect();
	};
	if ($@){
		my $e = $@;
		if (ref($e) and $e->can('as_string')){
			$self->log->error('Got exception: ' . $e->as_string());
		}
		else {
			$self->log->error('Got exception: ' . Dumper($e));
		}
	}
	
	return {};
}

sub load_records {
	my ($self,$kernel,$heap,$session,$from,$msg) = @_[OBJECT,KERNEL,HEAP,SESSION,ARG0..ARG1];
	
	eval {
		# Validate input
		unless ($msg->body and ref($msg->body) eq 'HASH'){ 
			throw_e error => 'Invalid args: ' . Dumper($msg->body);
		}
		unless (-f $msg->body->{file}){
			throw_e error => 'File not found: ' . Dumper($msg->body->{file});
		}
				
		# Load and index our new batch of records if necessary
		my $indexer = new ELSA::Indexer($self->{_CONFIG_FILE});
		my $hash = $indexer->load_records($msg->body);
		
		if ($hash and ref($hash) eq 'HASH'){
			#$first_id = $hash->{first_id};
			#$last_id = $hash->{last_id};
		}
		else {
			$self->log->error('Load records apparently failed with ret ' . Dumper($hash));
			#TODO build in some sort of retry mechanism here in case load data eventually succeeds
		}
		
		# Now do the indexing if necessary
		if ($self->conf->get('sphinx/perm_index_size') and $hash->{last_id}){
			$indexer->index_records($hash);
		}
		
		my ($query, $sth);
		
		$indexer->get_lock('directory');
		
		# Check to see if we're low on temporary indexes and need to consolidate
		if ($indexer->_over_num_index_limit()){
			$self->log->warn('Over the temp index limit, engaging emergency consolidation');
			
			# Find out how many temp indexes we've got
			$query = sprintf("SELECT MIN(first_id) AS min_id, MAX(last_id) AS max_id,\n" .
				"MIN(start) AS start, MAX(end) AS end\n" .
				"FROM %s.indexes WHERE ISNULL(locked_by) AND type=\"temporary\"\n",
				$ELSA::Meta_db_name);
			$sth = $indexer->db->prepare($query);
			$sth->execute();
			my $row = $sth->fetchrow_hashref;
			my ($min_id, $max_id) = (0,0);
			if ($row){
				$min_id = $row->{min_id};
				$max_id = $row->{max_id};
				
				# We need to run the aggregate indexing to create permanent indexes from temp indexes	
				# Recurse and index the greater swath of records.  This will mean there will be indexes replaced.
				my $msg = POE::Event::Message->package( { first_id => $min_id, last_id => $max_id } );
				$msg->addRouteTo('asynch', 'agent', 'execute', 'manager', 'consolidate_indexes');
				$msg->addRemoteRouteTo('localhost', $self->conf->get('manager/listen_port'), 'asynch');
				$self->log->trace('Sending min_id ' . $min_id . ', max_id ' . $max_id . ' to consolidate_records');
				$msg->route();
			}
		}
		
		# Check to see if we need to consolidate any tables
		$query = sprintf('SELECT table_name, SUM(locked_by) AS locked, COUNT(DISTINCT id) AS num_indexes, ' . "\n"
			. 'min_id, max_id, max_id-min_id AS num_rows ' . "\n"
			. 'FROM %s.v_directory' . "\n"
			. 'WHERE ISNULL(table_locked_by) AND table_type="index"' . "\n"
			. 'GROUP BY table_name' . "\n"
			. 'HAVING ISNULL(locked) OR MOD(locked, ?)=0', $ELSA::Meta_db_name);
		$sth = $indexer->db->prepare($query);
		$sth->execute($$);
		my @rows;
		while (my $row = $sth->fetchrow_hashref){
			push @rows, $row;
		}
		
		foreach my $row (@rows){
			if ($row->{num_rows} >= $self->conf->get('sphinx/perm_index_size') and $row->{num_indexes} > 1){
				$self->log->debug('Table ' . $row->{table_name} . ' needs to be consolidated.');
				# Verify that there are no indexes in progress on this table
				$query = sprintf('SELECT COUNT(*) AS count FROM %s.v_directory WHERE table_name=? AND NOT ISNULL(locked_by)', $ELSA::Meta_db_name);
				$sth = $indexer->db->prepare($query);
				$sth->execute($row->{table_name});
				my $check_row = $sth->fetchrow_hashref;
				if (not $check_row->{count}){
					# Lock the table
					$query = sprintf('UPDATE %s.tables SET table_locked_by=? WHERE table_name=?', $ELSA::Meta_db_name);
					$sth = $indexer->db->prepare($query);
					$sth->execute($$, $row->{table_name});
					$self->log->debug('Locked table ' . $row->{table_name});
					my $msg = POE::Event::Message->package( { first_id => $row->{min_id}, last_id => $row->{max_id} } );
					$msg->addRouteTo('asynch', 'agent', 'execute', 'manager', 'consolidate_indexes');
					$msg->addRemoteRouteTo('localhost', $self->conf->get('manager/listen_port'), 'asynch');
					$self->log->trace('Sending min_id ' . $row->{min_id} . ', max_id ' . $row->{max_id} . ' to consolidate_indexes');
					$msg->route();
				}
				else {
					$self->log->debug('Table ' . $row->{table_name} . ' is locked, not consolidating');
				}
			}
		}
		$indexer->release_lock('directory');
		$indexer->db->disconnect();
	};
	if ($@){
		my $e = $@;
		if (ref($e) and $e->can('as_string')){
			$self->log->error('Got exception: ' . $e->as_string());
		}
		else {
			$self->log->error('Got exception: ' . Dumper($e));
		}
	}
	
	return {};
}

sub archive_records {
	my ($self,$kernel,$heap,$session,$from,$msg) = @_[OBJECT,KERNEL,HEAP,SESSION,ARG0..ARG1];
	
	eval {
		# Validate input
		unless ($msg->body and ref($msg->body) eq 'HASH'){ 
			throw_e error => 'Invalid args: ' . Dumper($msg->body);
		}
		unless (-f $msg->body->{file}){
			throw_e error => 'File not found: ' . Dumper($msg->body->{file});
		}
		
		my $first_id = 0;
		my $last_id = 0;
		my $args = $msg->body;
		
		# Load and index our new batch of records if necessary
		my $indexer = new ELSA::Indexer($self->{_CONFIG_FILE});
		my $hash = $indexer->archive_records($msg->body);
		$indexer->db->disconnect();
		if ($hash and ref($hash) eq 'HASH'){
			$first_id = $hash->{first_id};
			$last_id = $hash->{last_id};
		}
		else {
			$self->log->error('Archive records apparently failed with ret ' . Dumper($hash));
			#TODO build in some sort of retry mechanism here in case load data eventually succeeds
		}
	};
	if ($@){
		my $e = $@;
		if (ref($e) and $e->can('as_string')){
			$self->log->error('Got exception: ' . $e->as_string());
		}
		else {
			$self->log->error('Got exception: ' . Dumper($e));
		}
	}
	
	return {};
}

sub consolidate_indexes {
	my ($self,$kernel,$heap,$session,$from,$msg) = @_[OBJECT,KERNEL,HEAP,SESSION,ARG0..ARG1];
	
	eval {
		# Validate input
		unless ($msg->body and ref($msg->body) eq 'HASH'){ 
			throw_e error => 'Invalid args: ' . Dumper($msg->body);
		}
		unless ($msg->body->{first_id} and $msg->body->{last_id}){
			throw_e error => 'first/last id not given: ' . Dumper($msg->body);
		}
		my $args = $msg->body;
		
		# Consolidate indexes
		if ($self->conf->get('sphinx/perm_index_size') and $args->{last_id}){
			my $indexer = new ELSA::Indexer($self->{_CONFIG_FILE});
			$indexer->consolidate_indexes($args);
			$indexer->db->disconnect();
		}
		else {
			$self->log->info('Not configured for indexing');
		}
	};
	if ($@){
		my $e = $@;
		if (ref($e) and $e->can('as_string')){
			$self->log->error('Got exception: ' . $e->as_string());
		}
		else {
			$self->log->error('Got exception: ' . Dumper($e));
		}
	}
	
	return {};
}

sub add_programs {
	my ($self,$kernel,$heap,$session,$from,$msg) = @_[OBJECT,KERNEL,HEAP,SESSION,ARG0..ARG1];
	
	unless ($msg and $msg->body and ref($msg->body) eq 'HASH'){
		$self->log->error('Invalid load cache msg: ' . Dumper($msg));
		return { error => 'Invalid load cache msg' };
	}
	my $programs = $msg->body;
	$self->log->trace('Adding programs: ' . Dumper($programs));
	
	eval {
		my $elsa = new ELSA($self->{_CONFIG_FILE});
		$elsa->init_db();
		my ($query, $sth);
		$query = sprintf('INSERT INTO %s.programs (id, program) VALUES(?,?) ON DUPLICATE KEY UPDATE id=?', $ELSA::Meta_db_name);
		$sth = $elsa->db->prepare($query);
		$query = sprintf('REPLACE INTO %s.class_program_map (class_id, program_id) VALUES(?,?)', $ELSA::Meta_db_name);
		my $sth_map = $elsa->db->prepare($query);
		foreach my $program (keys %{ $programs }){
			$sth->execute($programs->{$program}->{id}, $program, $programs->{$program}->{id});
			if ($sth->rows){ # this was not a duplicate, proceed with the class map insert
				$sth_map->execute($programs->{$program}->{class_id}, $programs->{$program}->{id});
			}
		}
		$elsa->db->disconnect();
	};
	if ($@){
		my $e = $@;
		if (ref($e) and $e->can('as_string')){
			$self->log->error('Got exception: ' . $e->as_string());
		}
		else {
			$self->log->error('Got exception: ' . Dumper($e));
		}
	}
	return {};
}

sub get_stats {
	my ($self,$kernel,$heap,$session,$args) = @_[OBJECT,KERNEL,HEAP,SESSION,ARG0];
	
	my $stats_str = "Stats:\n";	
	foreach my $stat_type (keys %{ $self->{_STATS} }){
		throw_e error => "Invalid stat type" . Dumper($stat_type)
			unless (ref($stat_type) eq 'HASH');
		foreach my $stat_name (keys %{ $self->{STATS}->{$stat_type} }){
			throw_e error => "Invalid stat name" . Dumper($stat_name)
				unless (ref($stat_name) eq 'HASH');
			$stats_str .= "Type: $stat_type, name: $stat_name, value: "
				. $self->{_STATS}->{$stat_type}->{$stat_name} . "\n";
		}
	}
	return $stats_str;
}

sub get_indexes {
	my ($self,$kernel,$heap,$session,$from,$msg) = @_[OBJECT,KERNEL,HEAP,SESSION,ARG0..ARG1];
	unless ($msg and $msg->body() and ref($msg->body()) eq 'HASH'
		and $msg->body()->{start} and $msg->body()->{end}){
		$self->log->error('Invalid msg: ' . Dumper($msg));
		return { error => 'Invalid msg' };
	}
	my $args = $msg->body();
	#$self->log->trace('args: ' . Dumper($args));
	
	my $ret = { node => $self->conf->get('manager/server_name') };
	eval {
		my $search = new ELSA::Search($self->{_CONFIG_FILE});
		$ret->{indexes} = $search->get_indexes($msg->body());
		#$self->log->trace('got ret: ' . Dumper($ret));
		$search->db->disconnect(); # explicitly disconnect here since InactiveDestroy is enabled and the disconnect won't happen automatically like usual
	};
	if ($@){
		my $e = $@;
		if (ref($e) and $e->can('as_string')){
			$self->log->error('Got exception get_indexes: ' . $e->as_string());
			$ret = { error => 'Got exception get_indexes: ' . $e->as_string() };
		}
		else {
			$self->log->error('Got exception get_indexes: ' . Dumper($e));
			$ret = { error => 'Got exception get_indexes: ' . Dumper($e) };
		}
	}
	
	my $response = new POE::Event::Message($msg, $ret);
	$response->route();
	
	return $ret;
}

sub _get_all_indexes {
	my ($self,$kernel,$heap,$session,$state) = @_[OBJECT,KERNEL,HEAP,SESSION,STATE];

	# Queue a forked load buffers job
	my $msg = POE::Event::Message->package( {} );
	$msg->addRouteTo('asynch', 'agent', 'execute', 'manager', 'get_all_indexes');
	$msg->addRemoteRouteTo('localhost', $self->conf->get('manager/listen_port'), 'asynch');
	$msg->route();

	$kernel->alarm_add($state, $self->conf->get('sphinx/index_interval') + ((time() - (time() % $self->conf->get('sphinx/index_interval')))));
}

sub get_all_indexes {
	my ($self,$kernel,$heap,$session,$from,$msg) = @_[OBJECT,KERNEL,HEAP,SESSION,ARG0..ARG1];
	
	# Only do the update if we have stale index data
	my $current_indexes;
	my $ret;
	my $latest_time = 0;
	eval {
		my ($query, $sth);
		my $indexer = new ELSA::Indexer($self->{_CONFIG_FILE});
		$current_indexes = $indexer->current_indexes();
		$indexer->db->disconnect();
		foreach my $node (keys %$current_indexes){
			foreach my $index_name (keys %{ $current_indexes->{$node} }){
				if ($current_indexes->{$node}->{$index_name}->{end} > $latest_time){
					$latest_time = $current_indexes->{$node}->{$index_name}->{end}
				}
			}	
		}
		$self->log->trace('$latest_time: ' . $latest_time);
	};
	if ($@ or not $current_indexes){
		my $e = $@;
		$self->log->error('error getting indexes: ' . $e);
		$ret = { error => $e };
	}
	#$self->log->trace('time - index time: ' . (time() - $current_indexes->{timestamp}));
	elsif ((time() - $latest_time) > $self->conf->get('sphinx/index_interval')){
		my $send_args = { start => 1, end => 2**32 };
		$self->log->trace('sending to remote nodes for get_indexes: ' . Dumper($send_args));
		# Asynchronously get remote indexes from everyone including ourselves
		foreach my $node (keys %{ $self->get_peers() }, $self->conf->get('manager/server_name')){
			# Tell the other nodes (including ourselves) that they should send us updates
			my $msg = POE::Event::Message->package( $send_args );
			$msg->addRouteTo('post', 'agent', 'execute', 'manager', 'update_indexes' );
			$msg->addRemoteRouteTo($self->conf->get('manager/server_name'), $self->conf->get('manager/listen_port'), 'asynch');
			# Then add the route to the remote node (or perhaps ourselves)
			$msg->addRouteTo('post', 'agent', 'execute', 'manager', 'get_indexes');
			$msg->addRemoteRouteTo($node, $self->conf->get('manager/listen_port'), 'asynch');
			$msg->route();
			$self->log->trace('msg routed');
		}
		$ret = { ok => 1 };
	}
	else {
		$ret = { ok => 2 };
	}
	return $ret;
}

sub update_indexes {
	my ($self,$kernel,$heap,$session,$state,$from,$msg) = @_[OBJECT,KERNEL,HEAP,SESSION,STATE,ARG0..ARG1];
	unless ($msg and ref($msg) eq 'POE::Event::Message' and $msg->body() and ref($msg->body()) eq 'HASH'
		and $msg->body()->{indexes}){
		$self->log->error('Invalid msg: ' . Dumper($msg));
		return { error => 'Invalid msg' };
	}
	my $args = $msg->body();
	#$self->log->trace('args: ' . Dumper($args));
	eval {
		my $indexer = new ELSA::Indexer($self->{_CONFIG_FILE});
		$indexer->update_indexes($args->{node}, $args->{indexes});
		$indexer->db->disconnect();
	};
	my $ret;
	if ($@){
		my $e = $@;
		$ret = {error => $e};
	}
	else {
		$ret = {ok => 1};
	}
	$msg->route($ret);
	
	return $ret;
}

sub current_indexes {
	my ($self,$kernel,$heap,$session,$state,$from,$msg) = @_[OBJECT,KERNEL,HEAP,SESSION,STATE,ARG0..ARG1];
	unless ($msg){
		$self->log->error('Invalid msg: ' . Dumper($msg));
		return { error => 'Invalid msg' };
	}
	
	my $indexes;
	eval {
		my $indexer = new ELSA::Indexer($self->{_CONFIG_FILE});
		$indexes = $indexer->compile_indexes();
		$indexer->db->disconnect();
	};
	if ($@){
		my $e = $@;
		$self->log->error('error getting current indexes: ' . $e);
		$indexes = {};
	}
	my $response = new POE::Event::Message($msg, $indexes);
	$response->route();
	
	return $indexes;
}

1;

__END__
