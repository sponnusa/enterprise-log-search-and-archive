package Janus;
use strict;
use Data::Dumper;
use Time::HiRes qw(time);
use DBI;
#sub POE::Kernel::ASSERT_DEFAULT { 1 };
#sub POE::Kernel::TRACE_EVENTS { 1 };
use EV; #makes this a POE::Loop::EV
use POE qw(Component::Server::TCP );
use JSON -convert_blessed_universally;
use Log::Log4perl;
use Config::JSON;
use Net::LDAP::Express;
use Net::LDAP::FilterBuilder;
use Module::Pluggable require => 1, search_path => [ qw( Export Info ) ];
use URI::Escape;
use Date::Manip;
use Digest::HMAC_SHA1;
use Mail::Internet;
use Exporter;
our @ISA = qw( Exporter );
use Socket qw(inet_aton inet_ntoa);
use FindBin;
use Storable qw(dclone);
use File::Slurp;
use MIME::Base64;

use POE::Event::Message;
use POE::Filter::Reference;
use Data::Serializer;
BEGIN {
	$POE::Event::Message::Filter = new POE::Filter::Reference( 
		Data::Serializer->new(
			serializer => 'YAML::Syck',
			portable => 1,
		)
	);
}

use constant DEFAULT_CLASSES_BIT  => 1;
use constant DEFAULT_HOSTS_BIT    => 2;
use constant DEFAULT_PROGRAMS_BIT => 4;

use constant ERROR => -1;
use constant REVALIDATE => -2;

our @EXPORT = qw(
  DEFAULT_CLASSES_BIT
  DEFAULT_HOSTS_BIT
  DEFAULT_PROGRAMS_BIT
);

our %_Auth_exempt_states = map { $_ => 1 } qw( get_user_info );
our %_Admin_required_states = map { $_ => 1 } qw( set_permissions get_permissions set_default_permission );
our $Default_pcap_cols = [ qw(timestamp frame.protocols ip.src ip.dst expert.message) ];

#our @_Published_states = qw(
#  query poll recv_results publish_states
#  set_permissions get_group_info get_previous_queries get_query_auto_complete get_user_info
#);
our @_Object_states = qw(
  query
  query_results
  set_permissions
  get_user_info
  get_group_info
  get_previous_queries
  get_query_auto_complete
  validate
  export
  schedule_query
  save_results
  get_saved_result
  get_saved_queries
  get_schedule_actions
  get_scheduled_queries
  update_scheduled_query
  delete_scheduled_query
  delete_saved_results
  get_form_params
  get_permissions
  get_exceptions
  set_default_permissions
  set_permissions_exception
  get_running_archive_query
  pcap_query
  pcap_query_results
  get_pcap
  get_packet
  get_stream
  get_raw_pcap
  get_stats
  get_log_info
  _start
  _default
  _error
  _get_group_info
  _get_previous_queries
  _get_query_auto_complete
  _stop
  _get_user_info
  _validate_user_info
  _create_user
  _rpc_send
  _schedule_query
  _run_schedule
  _save_results
  _get_saved_queries
  _get_saved_query
  _alert
  _open_ticket
  _get_host_info
  _send_email
  _get_permissions
  _revalidate_group
  _get_group_members
  _batch_notify
  _get_stats
  _return_results
  _get_streams
);

sub new {
	my $class = shift;
	my $config_file_name = ( shift or 'janus.conf' );

	my $conf = new Config::JSON($config_file_name)
	  or die("Could not open config file $config_file_name");
	#my $log_file = $conf->get('log_file');
	Log::Log4perl::init_once( $conf->get('log4perl.conf') )
	  or die("Unable to init logger\n");
	my $logger = Log::Log4perl::get_logger(__PACKAGE__)
	  or die("Unable to init logger\n");

	my $self = {
		_CONF   => $conf,
		_LOGGER => $logger,
	};

	bless( $self, $class );
	
	# init plugins
	$self->plugins();
	
	return $self;
}

sub conf {
	my $self = shift;
	return $self->{_CONF};
}

sub log {
	my $self = shift;
	return $self->{_LOGGER};
}

sub run {
	my $self = shift;

	POE::Component::Server::TCP->new(
		Alias => "socket",
		Port  => $self->conf->get('Janus/port'),
		ClientInput => sub {
			my $msg = $_[ARG0];
			$self->log->trace('ClientInput got msg: ' . Dumper($msg));
			unless ($msg and ref($msg) and $msg->can('header')){
				$self->log->error('Invalid msg: ' . Dumper($msg));
				$_[HEAP]->{client}->put({error => 'Invalid msg'});
				return;
			}
			
			my ($msg_status, $msg_errmsg) = $msg->status();
			if ($msg_status == ERROR){
				$self->log->trace('Message has error ' . $msg_errmsg . ', routing back');
				$msg->routeBack();
				return;
			}
			
			if ($msg->getMode() eq 'call'){
				$msg->setMode('post'); # reset this
				$msg->addRouteBack('post', undef, 'sync_respond' );
				$self->log->trace('ClientInput sync routing msg: ' . Dumper($msg));
			}
			
			# Some states are exempt from validation
			my $next_state = $msg->hasRouteTo() ? $msg->hasRouteTo()->[4] : 0;
			$self->log->trace('next_state: ' . Dumper($next_state));
			unless ($_Auth_exempt_states{ $next_state }){
				$msg->addRouteTo('post', $self->conf->get('Janus/session'), 'validate', $next_state);	
			}
			
			# Adjust the timeout if necessary
			if (ref($msg->body) eq 'HASH' and defined $msg->body->{timeout}){
				$_[KERNEL]->yield('adjust_timeout', sprintf('%d', $msg->body->{timeout}));
			}
			
			$msg->route();
		},
		ClientConnected => sub {
			$self->log->trace('Session ' . $_[SESSION]->ID() . ' from client ' .
					$_[HEAP]{remote_ip} . ':' . $_[HEAP]{remote_port} . ' connected');
			$_[HEAP]{_session_start} = time();
			# Set a timeout
			$_[HEAP]{alarm_id} = $_[KERNEL]->delay_set('timeout', $self->conf->get('Janus/timeout'));
		},
		ClientDisconnected => sub {
			$self->log->debug('Session ' . $_[SESSION]->ID() . ' from client ' .
					$_[HEAP]{remote_ip} . ':' . $_[HEAP]{remote_port} 
					. ' disconnected, duration: ' . (time() - $_[HEAP]{_session_start}) );
			# Remove timeout
			$_[KERNEL]->alarm_remove_all();
		},
		ClientFilter => $POE::Event::Message::Filter,
		InlineStates => {
			sync_respond => sub {
				# Remove timeout
				$_[KERNEL]->alarm_remove_all(); # this only applies to this session
				if ($_[ARG1] and ref($_[ARG1]) eq 'ARRAY' and $_[ARG1]->[0]){
					my $response = $_[ARG1]->[0];
					$_[HEAP]->{client} and $_[HEAP]->{client}->put($response);
				}
				else {
					my $response = POE::Event::Message->new(undef, undef);
					$response->setErr(ERROR, 'Invalid args for response received');
					$_[HEAP]->{client} and $_[HEAP]->{client}->put($response);
				}
			},
			timeout => sub {
				$self->log->error('Session ' . $_[SESSION]->ID() . ' from client ' .
					$_[HEAP]{remote_ip} . ':' . $_[HEAP]{remote_port} 
					. ' timed out after ' .
					$self->conf->get('Janus/timeout') . ' seconds');
				my $response =  POE::Event::Message->new(undef, undef);
				$response->setErr(ERROR, 'Query timed out after ' . $self->conf->get('Janus/timeout') . ' seconds');
				$_[HEAP]->{client} and $_[HEAP]->{client}->put($response);
				$_[KERNEL]->yield('shutdown');
				return;
			},
			adjust_timeout => sub{
				$self->log->debug('Adjusting socket timeout to be +' . $_[ARG0] . ' seconds');
				$_[KERNEL]->delay_adjust($_[HEAP]{alarm_id}, $_[ARG0]);
			}
		}
	);

	# Client to the syslog cluster
	POE::Session->create(
		options       => { trace  => 1,  debug => 1, default => 1 },
		inline_states => { _child => sub { }, 
			exception => sub { 
				warn 'exception: ' . Dumper($_[ARG1]->{error_str});
				$poe_kernel->sig_handled(); 
			} 
		},
		object_states => [ $self => [@_Object_states], ],
		heap          => {
			qids       => {},
			must_revalidate => {},
		}
	);
	$poe_kernel->run();
}

sub _start {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	$kernel->alias_set($self->conf->get('Janus/session'));
	
	$kernel->sig('DIE', 'exception' );
	
	$heap->{dbh} = DBI->connect(
		$self->conf->get('meta_db/dsn'),
		$self->conf->get('meta_db/username'),
		$self->conf->get('meta_db/password'),
		{ RaiseError => 1 }
	) or die($DBI::errstr);
	$heap->{dbh}->{HandleError} = \&_dbh_error_handler;
	$heap->{dbh}->{mysql_auto_reconnect} = 1; # we will auto-reconnect on disconnect
	
	my ($query, $sth);
	# Make sure we don't have any uncompleted queries still in the log on startup
	$query = 'UPDATE query_log SET num_results=0 WHERE archive=1 AND ISNULL(num_results)';
	$heap->{dbh}->do($query);
	
	$kernel->yield('_run_schedule');
}

sub _default {
	my ($self,$kernel, $heap, $session, $event, $args) = @_[OBJECT,KERNEL,HEAP,SESSION, ARG0, ARG1];
    my ($postback_args, $state_args) = @$args;
    my $msg = $state_args->[0];
    #$kernel->yield('_error', 'No such event ' . $event, $msg);
    # We don't know what this is, default route it to the cluster
    $kernel->yield('_rpc_send', $msg, $event);
}

sub _error {
	my ( $self, $kernel, $heap, $err, $msg ) = @_[ OBJECT, KERNEL, HEAP, ARG0..ARG1 ];
	$err = "Error from " . $_[CALLER_STATE] . ": " . $err;
	$self->log->error($err);
	$msg->setErr(ERROR, $err);
	$msg->routeBack();
}

sub validate {
	my ($self, $kernel, $session, $heap, $next_state) = 
		@_[OBJECT,KERNEL,SESSION,HEAP,ARG0];
	
	my ($msg, $args) = _get_msg(@_);
	$self->log->trace('got msg: ' . Dumper($msg));
	$self->log->trace('got args: ' . Dumper($args));
	
	if ($msg->param('_user') and $heap->{must_revalidate}->{ $msg->param('_user')->{uid} }){
		unless ($msg->param('_action_params') 
			and ref($msg->param('_action_params') eq 'HASH'
			and $msg->param('_action_params')->{query_schedule_id})){ #scheduled queries don't have to revalidate
			$self->log->warn('Client ' . $msg->param('_user')->{username} . ' must revalidate');
			$msg->setErr(REVALIDATE, 'CLIENT MUST REVALIDATE');
			$msg->routeBack();
			return;
		}
	}
	
	my $admin_required = $_Admin_required_states{ $next_state };
	my $user = $kernel->call($session, '_validate_user_info', $msg->param('_user'), $admin_required);
	if ( ref($user) ne 'HASH' ) {
		$kernel->yield('_error', 'Invalid user: ' . Dumper($args), $msg);
		return;
	}

	$msg->route();
	return;
}

sub _validate_user_info {
	my ( $self, $kernel, $heap, $user, $admin_required ) =
	  @_[ OBJECT, KERNEL, HEAP, ARG0 .. ARG1 ];
	$self->log->trace( "Got user: " . Dumper($user) );
	unless ( $user and ref($user) eq 'HASH' ) {
		return "Invalid args";
	}
	unless ( $user->{username} ) {
		return "No username";
	}

	if ( $admin_required and not $user->{is_admin} ) {
		return "Insufficient privileges";
	}
#	$self->log->debug( "user: " . Dumper($user) );
	return $user;
}

sub _rpc_send {
	my ( $self, $kernel, $session, $heap, $msg, $method ) = 
		@_[ OBJECT, KERNEL, SESSION, HEAP, ARG0..ARG1 ];
	
	# Final routeBack is already done up in the ClientInput sub
	$msg->addRemoteRouteBack(
		$self->conf->get('Janus/server'), 
		$self->conf->get('Janus/port'), 
		'asynch');
	$msg->addRouteTo('post', 'agent', 'execute', 'web', $method);
	$msg->addRemoteRouteTo(
		$self->conf->get('cluster/server'),
		$self->conf->get('cluster/port'),
		'asynch',
	);
	$self->log->debug('routing query msg: ' . Dumper($msg));
	
	# Do this with a native Perl alarm since the POE alarm mechanism doesn't work here for some reason
	eval {	
		local $SIG{ALRM} = sub { die 'alarm'; };
		alarm $self->conf->get('Janus/timeout');
		$msg->route();
		alarm 0;
	};
	if ($@){
		$self->log->error('Cluster connection timed out after ' . $self->conf->get('Janus/timeout') . ' seconds');
		$msg->setErr(ERROR, 'Cluster connection timed out after ' . $self->conf->get('Janus/timeout') . ' seconds');
		$self->log->trace('Routing back: ' . Dumper($msg));
		$msg->routeBack();
	}
}

sub export {
	my ( $self, $kernel, $session, $heap ) = @_[ OBJECT, KERNEL, SESSION, HEAP ];

	my ($msg,$args) = _get_msg(@_);
	
	if ( $args and ref($args) eq 'HASH' and $args->{data} and $args->{plugin} ) {
		my $decode;
		eval {
			$decode = decode_json(uri_unescape($args->{data}));
			$self->log->debug( "Decoded data as : " . Dumper($decode) );
		};
		if ($@){
			$kernel->yield( '_error', "invalid args, error: $@, args: " . Dumper($args), $msg );
			return;
		}
		
		my $results_obj = $self->_export($args->{plugin}, $decode);
		if ($results_obj){
			$msg->body({ ret => $results_obj->results(), mime_type => $results_obj->get_mime_type() });
			$msg->route();
			return;
		}
		
		$self->log->error('Unable to build results object from args');
		$kernel->yield( '_error', "failed to find plugin " . $args->{plugin} . ', only have plugins ' .
			join(', ', $self->plugins()) . ' ' . Dumper($args), $msg );
	}
	else {
		$self->log->error('Invalid args: ' . Dumper($args));
		$kernel->yield( '_error', "invalid args: " . Dumper($args), $msg );
	}
}

sub _export {
	my $self = shift;
	my $plugin = shift;
	my $data = shift;
	
	#TODO fork here so this doesn't block
	my $plugin_fqdn = 'Export::' . $plugin;
	foreach my $plugin ($self->plugins()){
		if ($plugin eq $plugin_fqdn){
			$self->log->debug('loading plugin ' . $plugin);
			my $results_obj = $plugin->new($data);
			$self->log->debug('results_obj:' . Dumper($results_obj));
			return $results_obj;
		}
	}
}

sub query {
	my ( $self, $kernel, $session, $heap ) = @_[ OBJECT, KERNEL, SESSION, HEAP ];

	my ($msg,$args) = _get_msg(@_);
	my $user = $msg->param('_user');
	my ($query, $sth);

	if ( $args and ref($args) eq 'HASH' and $args->{q} ) {
		#my $decode = decode_json( $args->{q} );
		my $decode = decode_json($args->{q});
		$self->log->debug( "Decoded as : " . Dumper($decode) );
		if (    ref($decode) eq 'HASH'
			and $decode->{'query_params'}
			and $decode->{'query_meta_params'} )
		{
			
			
			my $system = 0;
			my $to_state = 'query';
			my $is_archive = 0;
			if ($decode->{'query_meta_params'}->{archive_query}){ # no system-initiated queries for archive
				# Check to see if this user is already running an archive query
				$query = 'SELECT qid, uid FROM query_log WHERE archive=1 AND ISNULL(num_results)';
				$sth = $heap->{dbh}->prepare($query);
				$sth->execute();
				my $counter = 0;
				while (my $row = $sth->fetchrow_hashref){
					if ($row->{uid} eq $user->{uid}){
						$self->log->error('User ' . $user->{username} . ' already has an archive query running');
						$kernel->yield( '_error', 'Archive query already in progress in query id ' . $row->{qid}, $msg);
						return;
					}
					$counter++;
					if ($counter >= $self->conf->get('max_concurrent_archive_queries')){
						#TODO create a queuing mechanism for this
						$self->log->error('There are already ' . $self->conf->get('max_concurrent_archive_queries') . ' queries running');
						$kernel->yield( '_error', 'There are already ' . $self->conf->get('max_concurrent_archive_queries') . ' queries running', $msg);
						return;
					}
				}
				$to_state = 'archive_query';
				$msg->param('_action_params', { action => '_batch_notify' });
				$is_archive = 1;
			}
			else { 
				# Is this a system-initiated query?
				if ($msg->param('_action_params') 
					and ref($msg->param('_action_params')) eq 'HASH'
					and $msg->param('_action_params')->{query_schedule_id}){
					$system = 1;
				}
				elsif ($user->{username} eq 'system'){
					$system = 1;
				}
			}

			# Log the query
			$heap->{dbh}->begin_work;
			$query = 'INSERT INTO query_log (uid, query, system, archive) VALUES (?, ?, ?, ?)';
			$sth   = $heap->{dbh}->prepare($query);
			$sth->execute( $user->{uid}, $args->{q}, $system, $is_archive );
			$query = 'SELECT MAX(qid) AS qid FROM query_log';
			$sth   = $heap->{dbh}->prepare($query);
			$sth->execute();
			my $row = $sth->fetchrow_hashref;
			my $qid = $row->{qid};
			$heap->{dbh}->commit;

			$self->log->debug( "Received query with qid $qid at " . time() );
			
			if ($is_archive){
				# Craft a msg to send back to the client so it knows not to wait around
				my $msg_template = dclone($msg);
				my $ack_msg = new POE::Event::Message($msg_template, { batch_query => $qid });
				$ack_msg->route(); # routes back to client
			}

			# Record this for later reference
			$heap->{sessions}->{ $args->{session_id} }->{qids}->{$qid} = 
				{ 
					time => Time::HiRes::time(),
					results => [],
					nodes_received => {},
					totalRecords => 0,
					recordsReturned => 0,
					startIndex => 0,
					groups => {},
				};

			$decode->{'query_meta_params'}->{permissions} = $user->{'permissions'};
			my $args = {
				'qid'               => $qid,
				'query_params'      => $decode->{'query_params'},
				'query_meta_params' => $decode->{'query_meta_params'},
			};
			
			$msg->body($args);
			$msg->addRouteBack('post', $self->conf->get('Janus/session'), 'query_results');
			$msg->addRemoteRouteBack(
				$self->conf->get('Janus/server'), 
				$self->conf->get('Janus/port'), 
				'asynch');
			$msg->addRouteTo('post', 'agent', 'execute', 'web', $to_state);
			$msg->addRemoteRouteTo(
				$self->conf->get('cluster/server'),
				$self->conf->get('cluster/port'),
				'asynch',
			);
			$self->log->debug('routing query msg: ' . Dumper($msg));
			eval {	
				local $SIG{ALRM} = sub { die 'alarm'; };
				alarm $self->conf->get('Janus/timeout');
				$msg->route();
				alarm 0;
			};
			if ($@){
				$self->log->error('Cluster connection timed out after ' . $self->conf->get('Janus/timeout') . ' seconds');
				$kernel->yield( '_error', 'Cluster connection timed out after ' . $self->conf->get('Janus/timeout') . ' seconds', $msg);
				return;
			}

			$self->log->debug("Posted args to web/query");
			
		}
		else {
			$self->log->error('Invalid args: ' . Dumper($args));
			$kernel->yield( '_error', "invalid args: " . Dumper($args), $msg );
			return;
		}
	}
	else {
		$self->log->error('Invalid args: ' . Dumper($args));
		$kernel->yield( '_error', "invalid args: " . Dumper($args), $msg );
		return;
	}
}

sub get_log_info {
	my ( $self, $kernel, $session, $heap ) = @_[ OBJECT, KERNEL, SESSION, HEAP ];

	my ($msg,$args) = _get_msg(@_);
	my $user = $msg->param('_user');
	
	my $decode;
	eval {
		$decode = decode_json(decode_base64($args->{q}));
	};
	if ($@){
		my $errmsg = 'Invalid JSON args: ' . Dumper($args);
		$self->log->error($errmsg);
		$kernel->yield( '_error', $errmsg, $msg);
		return;
	}
	
	unless ($decode and ref($decode) eq 'HASH'){
		my $errmsg = 'Invalid args: ' . Dumper($decode);
		$self->log->error($errmsg);
		$kernel->yield( '_error', $errmsg, $msg);
		return;
	}
	
	my $data;
	
	unless ($decode->{class} and $self->conf->get('plugins/' . $decode->{class})){
		$self->log->debug('no plugins for class ' . $decode->{class});
		$data =  { summary => 'No info.', urls => [], plugins => [] };
		$msg->body($data);
		$msg->route();
		return;
	}
	
	eval {
		my $plugin = $self->conf->get('plugins/' . $decode->{class})->new({conf => $self->conf, data => $decode});
		$data =  { summary => $plugin->summary, urls => $plugin->urls, plugins => $plugin->plugins };
	};
	if ($@){
		my $e = $@;
		my $errmsg = 'Error creating plugin ' . $self->conf->get('plugins/' . $decode->{class}) . ': ' . $e;
		$self->log->error($errmsg);
		$kernel->yield('_error', $errmsg, $msg);
		return;
	}
		
	unless ($data){
		my $errmsg = 'Unable to find info from args: ' . Dumper($decode);
		$self->log->error($errmsg);
		$kernel->yield( '_error', $errmsg, $msg);
		return;
	}
	
	$msg->body($data);
	$msg->route();
	
	return;
}

sub query_results {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg,$ret) = _get_msg(@_);
	$self->log->debug( "Got ret: " . Dumper($ret) );
	if ( $ret and ref($ret) eq 'HASH' and $ret->{qid} ) {
		my $qid = $ret->{qid};
		# Find the hash these results belong to and add the results
		foreach my $session ( keys %{ $heap->{sessions} } ) {
			if ( $heap->{sessions}->{$session}->{qids}->{$qid} ) {
				if ( $ret->{results} and ref( $ret->{results} ) eq 'ARRAY' ) {
					push @{ $heap->{sessions}->{$session}->{qids}->{$qid}->{results} },	@{ $ret->{results} };
				}
				if ( $ret->{groups} and ref( $ret->{groups} ) eq 'HASH' ) {
					foreach my $group_name (keys %{ $ret->{groups} }){
						push @{ $heap->{sessions}->{$session}->{qids}->{$qid}->{groups}->{$group_name} }, @{ $ret->{groups}->{$group_name} };
					}
				}
				foreach my $item qw(recordsReturned totalRecords startIndex warnings){
					if ($ret->{$item}){
						$heap->{sessions}->{$session}->{qids}->{$qid}->{$item} = $ret->{$item};
					}
				}
				$heap->{sessions}->{$session}->{qids}->{$qid}->{stats} ||= { nodes => {} };
				$heap->{sessions}->{$session}->{qids}->{$qid}->{stats}->{nodes}->{ $ret->{node} } = $ret->{stats};
				# Record that these results arrived
				$heap->{sessions}->{$session}->{qids}->{$qid}->{nodes_received}->{ $ret->{node} } = 1;
				
				# Set an alarm to return the results we've got if other nodes timeout
				$heap->{sessions}->{$session}->{qids}->{$qid}->{alarm_id} = 
					$kernel->delay_set('_return_results', $self->conf->get('Janus/timeout'), $session, $qid, $msg, $ret);
				
				# Check to see if all results are in
				if ( (scalar keys %{ $heap->{sessions}->{$session}->{qids}->{$qid}->{nodes_received} } ) >=
					( scalar keys %{ $ret->{needed} }) ){
					# All results are in!
					
					# Remove the alarm since all results are in
					$kernel->alarm_remove($heap->{sessions}->{$session}->{qids}->{$qid}->{alarm_id});
					
					$kernel->yield('_return_results', $session, $qid, $msg, $ret);
				}
				last;
			}
		}
	}
	else {
		my $errstr = 'ERROR: ' . Dumper($ret);
		if ($ret and ref($ret) eq 'HASH' and $ret->{error}){
			$errstr = 'ERROR: ' . $ret->{error};
		}
		$self->log->error($errstr);
		$kernel->yield('_error', $errstr, $msg );
	}
}

sub _return_results {
	my ( $self, $kernel, $heap, $session, $qid, $msg, $ret ) = @_[ OBJECT, KERNEL, HEAP, ARG0..ARG3 ];
	
	my $results = delete $heap->{sessions}->{$session}->{qids}->{$qid};
	
	if ( (scalar keys %{ $results->{nodes_received} } ) < ( scalar keys %{ $ret->{needed} }) ){
		$self->log->warn('received: ' . Dumper($results->{nodes_received}) . ' needed: ' . Dumper($ret->{needed}));
		if ($results->{warnings} and ref($results->{warnings}) eq 'ARRAY'){
			push @{ $results->{warnings} }, 'Timed out waiting for one or more nodes, results may be incomplete.';
		}
	}
	
	my $execute_milliseconds = int(
		(Time::HiRes::time() - $results->{time}) * 1000
	);
	$results->{qid} = $qid; # make sure this gets passed along too
	$results->{hash} = $self->_get_hash($qid); #tack on the hash for permalinking on the frontend
	$results->{totalTime} = $execute_milliseconds;
	
	# Handle local groupby grouping
	if ($ret->{query_meta_params}->{local_groupby}){
		foreach my $local_groupby_col (@{ $ret->{query_meta_params}->{local_groupby} }){
			eval {
				#$self->log->debug("pre results: " . Dumper($results));
				$results->{groups}->{$local_groupby_col} = $self->_local_groupby($results->{results}, $local_groupby_col);
				#$self->log->debug('got results for ' . $local_groupby_col . ' as ' . Dumper($results->{results}->{groups}->{$local_groupby_col}));
			};
			if ($@){
				$self->log->error('Error for groupby ' . $local_groupby_col . ': ' . $@);
			}
		}
	}
	elsif ($ret->{query_meta_params}->{archive_query} and scalar keys %{ $results->{groups} }){
		# Sort these in descending order
		foreach my $group_name (keys %{ $results->{groups} }){
			my %uniq;
			foreach my $row (@{ $results->{groups}->{$group_name} }){
				if ($uniq{ $row->{groupby} }){
					$uniq{ $row->{groupby} } += $row->{count};
				}
				else {
					$uniq{ $row->{groupby} } = $row->{count};
				}
			}
			my @ret;
			foreach my $groupby (sort { $uniq{$b} <=> $uniq{$a} } keys %uniq){
				push @ret, { '@groupby' => $groupby, '@count' => $uniq{$groupby} };
			}
			$results->{groups}->{$group_name} = [ @ret ];
			last;
		}
	}
	
	$self->log->debug('returning ' . Dumper($results));
	$msg->body( $results );
	
	$heap->{dbh}->begin_work;
	my $query = 'UPDATE query_log SET num_results=?, milliseconds=? '
	  		. 'WHERE qid=?';
	my $sth = $heap->{dbh}->prepare($query);
	# Sanitize totalRecords
	$results->{totalRecords} = $results->{totalRecords} ? $results->{totalRecords} : 0;
	$sth->execute( $results->{totalRecords}, $execute_milliseconds, $qid );
	$heap->{dbh}->commit;
	
	# Check to see if there are any actions to take with the results
	if ($msg->param('_action_params') 
		and ref($msg->param('_action_params')) eq 'HASH' 
		and $msg->param('_action_params')->{action}){
		
		# This is not going back to the web client
		my $action_msg = POE::Event::Message->package($results); # copy the original msg
		$action_msg->param('_user', { %{ $msg->param('_user') } }); 
		$action_msg->addRouteTo('post', undef, $msg->param('_action_params')->{action});
		$action_msg->route();
	}
	else {
		# Route normally
		$msg->route(); # only route the last one, even though all partials will have valid routes yet
	}	
}

sub get_permissions {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	
	# Get form params from backend so we can resolve ID's
	my $backend_msg = POE::Event::Message->package({});
	$backend_msg->setMode('call');
	$backend_msg->addRouteTo('post', 'agent', 'execute', 'web', 'get_form_params');
	$backend_msg->addRemoteRouteTo(
		$self->conf->get('cluster/server'),
		$self->conf->get('cluster/port'),
		'sync',
	);
	
	my $form_params_msg;
	eval {	
		local $SIG{ALRM} = sub { die 'alarm'; };
		alarm $self->conf->get('cluster/timeout');
		($form_params_msg) = $backend_msg->route();
		alarm 0;
	};
	if ($@){
		$self->log->error('Cluster connection timed out after ' . $self->conf->get('cluster/timeout') . ' seconds');
		$kernel->yield( '_error', 'Cluster connection timed out after ' . $self->conf->get('cluster/timeout') . ' seconds', $msg);
		return;
	}
	
	my $form_params = $form_params_msg->body();
	
	# Build programs hash
	my $programs = {};
	foreach my $class_id (keys %{ $form_params->{programs} }){
		foreach my $program_name (keys %{ $form_params->{programs}->{$class_id} }){
			$programs->{ $form_params->{programs}->{$class_id}->{$program_name} } = $program_name;
		}
	}
	
	my ($query, $sth);
	
	$query = 'SELECT t3.uid, username, t1.gid, groupname, COUNT(DISTINCT(t4.attr_id)) AS has_exceptions' . "\n" .
		'FROM groups t1' . "\n" .
		'LEFT JOIN users_groups_map t2 ON (t1.gid=t2.gid)' . "\n" .
		'LEFT JOIN users t3 ON (t2.uid=t3.uid)' . "\n" .
		'LEFT JOIN permissions t4 ON (t1.gid=t4.gid)' . "\n" .
		'GROUP BY t1.gid' . "\n" .
		'ORDER BY t1.gid ASC';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute();
	my @ldap_entries;
	while (my $row = $sth->fetchrow_hashref){
		push @ldap_entries, $row;
	}
	
	$query = 'SELECT t2.groupname, t1.gid, attr, attr_id' . "\n" .
		'FROM permissions t1' . "\n" .
		'JOIN groups t2 ON (t1.gid=t2.gid)' . "\n" .
		'WHERE t1.gid=?';
	$sth = $heap->{dbh}->prepare($query);
	foreach my $ldap_entry (@ldap_entries){
		$sth->execute($ldap_entry->{gid});
		my %exceptions;
		while (my $row = $sth->fetchrow_hashref){
			$self->log->debug('got row: ' . Dumper($row));
			if ($row->{attr}){
				$exceptions{ $row->{attr} } ||= {};
				if ($row->{attr} eq 'class_id'){
					$row->{attr_value} = $form_params->{classes}->{ $row->{attr_id} };
					if ($row->{attr_value}){
						$exceptions{ $row->{attr} }->{ $row->{attr_value} } = $row->{attr_id};
					}
				}
				elsif ($row->{attr} eq 'program_id'){
					$row->{attr_value} = $programs->{ $row->{attr_id} };
					if ($row->{attr_value}){
						$exceptions{ $row->{attr} }->{ $row->{attr_value} } = $row->{attr_id};
					}
				}
				elsif ($row->{attr} eq 'host_id'){
					# Must be host_id == IP or IP range
					if ($row->{attr_id} =~ /^\d+$/){
						$row->{attr_value} = inet_ntoa(pack('N*', $row->{attr_id}));
					}
					elsif ($row->{attr_id} =~ /(\d+)\s*\-\s*(\d+)/){
						$row->{attr_value} = inet_ntoa(pack('N*', $1)) . '-' . inet_ntoa(pack('N*', $2));
					}
					else {
						$kernel->yield( '_error',
							'bad host: ' . Dumper($args), $msg );
						return;
					}
					$exceptions{ $row->{attr} }->{ $row->{attr_value} } = $row->{attr_id};
				}
				else {
					$kernel->yield( '_error', 'unknown attr: ' . Dumper($args), $msg );
					return;
				}
				$self->log->debug('attr=' . $row->{attr} . ', attr_id=' . $row->{attr_id} . ', attr_value=' . $row->{attr_value});
			}
		}
		$ldap_entry->{_exceptions} = { %exceptions };
	}
	
	my $permissions = {
		totalRecords => scalar @ldap_entries,
		records_returned => scalar @ldap_entries,
		results => [ @ldap_entries ],	
	};
	
	$permissions->{form_params} = $form_params;
	
	$msg->body($permissions);
	$msg->route();
}

sub get_exceptions {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	
	# Get form params from backend so we can resolve ID's
	my $backend_msg = POE::Event::Message->package({});
	$backend_msg->setMode('call');
	$backend_msg->addRouteTo('post', 'agent', 'execute', 'web', 'get_form_params');
	$backend_msg->addRemoteRouteTo(
		$self->conf->get('cluster/server'),
		$self->conf->get('cluster/port'),
		'sync',
	);
	
	my $form_params_msg;
	eval {	
		local $SIG{ALRM} = sub { die 'alarm'; };
		alarm $self->conf->get('cluster/timeout');
		($form_params_msg) = $backend_msg->route();
		alarm 0;
	};
	if ($@){
		$self->log->error('Cluster connection timed out after ' . $self->conf->get('cluster/timeout') . ' seconds');
		$kernel->yield( '_error', 'Cluster connection timed out after ' . $self->conf->get('cluster/timeout') . ' seconds', $msg);
		return;
	}
	my $form_params = $form_params_msg->body();
	
	# Build programs hash
	my $programs = {};
	foreach my $class_id (keys %{ $form_params->{programs} }){
		foreach my $program_name (keys %{ $form_params->{programs}->{$class_id} }){
			$programs->{ $form_params->{programs}->{$class_id}->{$program_name} } = $program_name;
		}
	}
	
	my ($query, $sth);
	
	$query = 'SELECT t2.groupname, t1.gid, attr, attr_id' . "\n" .
		'FROM permissions t1' . "\n" .
		'JOIN groups t2 ON (t1.gid=t2.gid)' . "\n" .
		'WHERE t1.gid=?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($args->{gid});
	my @rows;
	while (my $row = $sth->fetchrow_hashref){
		if ($row->{attr}){
			if ($row->{attr} eq 'class_id'){
				$self->log->debug('getting class from ' . Dumper($form_params) . ' with id ' . $row->{attr_id} );
				$row->{attr_value} = $form_params->{classes}->{ $row->{attr_id} };
			}
			elsif ($row->{attr} eq 'program_id'){
				$row->{attr_value} = $programs->{ $row->{attr_id} };
			}
			else {
				# Must be host_id == IP or IP range
				if ($row->{attr_id} =~ /^\d+$/){
					$row->{attr_value} = inet_ntoa(pack('N*', $row->{attr_id}));
				}
				elsif ($row->{attr_id} =~ /(\d+)\s*\-\s*(\d+)/){
					$row->{attr_value} = inet_ntoa(pack('N*', $1)) . '-' . inet_ntoa(pack('N*', $2));
				}
				else {
					$kernel->yield( '_error',
						'bad host: ' . Dumper($args), $msg );
					return;
				}
			}
		}
		push @rows, $row;
	}
	my $exceptions = {
		totalRecords => scalar @rows,
		records_returned => scalar @rows,
		results => [ @rows ],	
	};
	$exceptions->{form_params} = $form_params;
	
	$msg->body($exceptions);
	$msg->route();
}

sub set_default_permissions {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	
	$self->log->debug('args: ' . Dumper($args));
	
	unless ($args and ref($args) eq 'HASH' and $args->{gid} and (defined $args->{class} or defined $args->{program} or defined $args->{host})){
		$kernel->yield( '_error',
			'Invalid permissions args: ' . Dumper($args), $msg );
		return;
	}
	
	my ($query, $sth);
	$query = 'SELECT default_permissions_allow FROM groups WHERE gid=?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($args->{gid});
	my $row = $sth->fetchrow_hashref;
	unless ($row){
		$kernel->yield( '_error',
			'Invalid gid: ' . Dumper($args), $msg );
		return;
	}
	my $current = $row->{default_permissions_allow};
	my $cur_program = $current & DEFAULT_PROGRAMS_BIT;
	my $cur_host = $current & DEFAULT_HOSTS_BIT;
	my $cur_class = $current & DEFAULT_CLASSES_BIT;
	
	my $perm = 0;
	my $attr;
	if (defined $args->{program}){
		$perm = ($args->{program} * DEFAULT_PROGRAMS_BIT) + $cur_host + $cur_class;
		$attr = 'program_id';
	}
	elsif (defined $args->{host}){
		$perm = ($args->{host} * DEFAULT_HOSTS_BIT) + $cur_program + $cur_class;
		$attr = 'host_id';
	}
	elsif (defined $args->{class}){
		$perm = ($args->{class} * DEFAULT_CLASSES_BIT) + $cur_program + $cur_host;
		$attr = 'class_id';
	}
	$self->log->debug('new: ' . $perm . ', current: ' . $current);	
	
	$query = 'UPDATE groups SET default_permissions_allow=? WHERE gid=?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($perm, $args->{gid});
	my $rows_updated = $sth->rows;
	
	# delete all of the exceptions associated with this group and attr since they no longer make sense
	$query = 'DELETE FROM permissions WHERE gid=? AND attr=?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($args->{gid}, $attr);
	my $groups_deleted = $sth->rows;
	
	$msg->body({success => $rows_updated, groups_deleted => $groups_deleted, default_permissions_allow => $perm, attr => $attr});
	$msg->route();
	
	# Make sure all affected users revalidate
	$query = 'SELECT uid FROM users_groups_map WHERE gid=?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($args->{gid});
	while (my $row = $sth->fetchrow_hashref){
		$heap->{must_revalidate}->{ $row->{uid} } = 1;
	}
}

sub set_permissions {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	
	unless ($args->{action} and ($args->{action} eq 'add' or $args->{action} eq 'delete')){
		$kernel->yield( '_error',
			'No set permissions action given: ' . Dumper($args), $msg );
		return;
	}
	eval { $args->{permissions} = decode_json( $args->{permissions} ); };
	$self->log->debug('args: ' . Dumper($args));
	if ($@) {
		$kernel->yield(
			'_error',
			'Error decoding permissions args: ' 
			  . $@ . ' : '
			  . Dumper($args),
			$msg 
		);
		return;
	}
	unless ( $args->{permissions} and ref( $args->{permissions} ) eq 'ARRAY' ) {
		$kernel->yield( '_error',
			'Invalid permissions args: ' . Dumper($args), $msg );
		return;
	}
	
	my ($query, $sth);
	if ($args->{action} eq 'add'){
		$query = 'INSERT INTO permissions (gid, attr, attr_id) VALUES (?,?,?)';
	}
	elsif ($args->{action} eq 'delete'){
		$query = 'DELETE FROM permissions WHERE gid=? AND attr=? AND attr_id=?';
	}
	my $rows_updated = 0;
	$sth = $heap->{dbh}->prepare($query);
	foreach my $perm (@{ $args->{permissions} }){
		$self->log->info('Changing permissions: ' . join(', ', $args->{action}, $perm->{gid}, $perm->{attr}, $perm->{attr_id}));
		$sth->execute($perm->{gid}, $perm->{attr}, $perm->{attr_id});
		$rows_updated += $sth->rows;
		if ($sth->rows){
			$kernel->yield('_revalidate_group', $perm->{gid});	
		}
	}
	
	$msg->body({success => $rows_updated, groups_deleted => $rows_updated});	
	$msg->route();	
}

sub _revalidate_group {
	my ( $self, $kernel, $heap, $session, $gid ) = @_[ OBJECT, KERNEL, HEAP, SESSION, ARG0 ];
	
	my $members = $kernel->call($session, '_get_group_members', $gid);
	unless ($members and ref($members) eq 'ARRAY' and scalar @$members){
		$self->log->error('No members found for gid ' . $gid);
		return;
	}
	my ($query, $sth);
	$query = 'SELECT uid FROM users WHERE username=?';
	$sth = $heap->{dbh}->prepare($query);
	foreach my $member (@$members){
		$sth->execute($member);
		my $row = $sth->fetchrow_hashref;
		if ($row){
			$heap->{must_revalidate}->{ $row->{uid} } = 1;
			$self->log->info('User ' . $member . ' must revalidate');
		}
	}
}

sub _get_group_members {
	my ( $self, $kernel, $heap, $gid ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
	my ($query, $sth);
	$query = 'SELECT groupname FROM groups WHERE gid=?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($gid);
	my $row = $sth->fetchrow_hashref;
	unless ($row){
		$self->log->error('Unknown group for gid ' . $gid);
		return;
	}
	my $group_search = $row->{groupname};
	my @ret;
	
	if ( $self->conf->get('auth_method') eq 'LDAP' ) {
		$self->log->error('Not implemented');
		return;
		# this will be a per-org implementation
#		my $ldap = $self->_get_ldap();
#		unless ($ldap) {
#			$self->log->error('Unable to connect to LDAP server');
#			return;
#		}
#	
#		# Whittle the group name down to just the cn
#		my @filter_parts = split(/[a-zA-Z0-9]{2}\=/, $group_search);
#		$self->log->debug('filter_parts: ' . Dumper(\@filter_parts));
#		my $cn = $filter_parts[1];
#		chop($cn); # strip the trailing comma
#		unless (scalar @filter_parts > 1){
#			$self->log->error('Invalid filter: ' . $group_search);
#			return;
#		}
#		my $filter = sprintf( '(&(objectclass=group)(cn=%s))', $cn );
#		$self->log->debug('filter: ' . $filter);
#		my $result = $ldap->search( sizelimit => 2, filter => $filter );
#		my @entries = $result->entries();
#		$self->log->debug('entries: ' . Dumper(\@entries));
#		if ( scalar @entries < 1 ) {
#			$self->log->error(
#				'No entries found in LDAP server:' . $ldap->error() );
#			return;
#		}
	}
	elsif ($self->conf->get('auth_method') eq 'db'){
		$query = 'SELECT username FROM users t1 JOIN users_groups_map t2 ON (t1.uid=t2.uid) WHERE t2.gid=?';
		$sth = $heap->{dbh}->prepare($query);
		$sth->execute($gid);
		while (my $row = $sth->fetchrow_hashref){
			push @ret, $row->{username};
		}
	}
	else {
		$self->log->error('No auth_method');
		return;
	}
	$self->log->debug('Found members: ' . Dumper(\@ret));
	return \@ret;
}

sub _get_ldap {
	my $self = shift;
	my $ldap = new Net::LDAP::Express(
		host        => $self->conf->get('ldap/host'),
		bindDN      => $self->conf->get('ldap/bindDN'),
		bindpw      => $self->conf->get('ldap/bindpw'),
		base        => $self->conf->get('ldap/base'),
		searchattrs => [ $self->conf->get('ldap/searchattrs') ],
	);
	unless ($ldap) {
		$self->log->error('Unable to connect to LDAP server');
		return;
	}
	return $ldap;
}

sub get_user_info {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);

	my $user_info =
	  $kernel->call( $_[SESSION], '_get_user_info', $args );

	$msg->body($user_info);
	$self->log->debug('Routing: ' . Dumper($msg));
	$msg->route();
	
	# Mark that this user has revalidated
	if ($heap->{must_revalidate}->{ $user_info->{uid} }){
		$self->log->info('User has revalidated: ' . Dumper($user_info));
		delete $heap->{must_revalidate}->{ $user_info->{uid} };
	}
	
}

sub _get_user_info {
	my ( $self, $kernel, $session, $heap, $username ) = @_[ OBJECT, KERNEL, SESSION, HEAP, ARG0 ];

	my $user_info = {
		'username'    => $username,
		'extra_attrs' => {},
		'groups' => [$username],   # all users belong to a group with their name
	};
	if ($username eq 'system'){
		$user_info->{uid} = 1;
		$user_info->{is_admin} = 1;
		$user_info->{qids} = {};
		$user_info->{permissions} = {
			class_id => {
				0 => 1,
			},
			host_id => {
				0 => 1,
			},
			program_id => {
				0 => 1,
			},
			filter => '',
		};
		return $user_info;
	}

	# Get the groups this user is a part of
	if ( $self->conf->get('auth_method') eq 'LDAP' ) {
		my $ldap = $self->_get_ldap();
		unless ($ldap) {
			$self->log->error('Unable to connect to LDAP server');
			return;
		}
		my $filter = sprintf( '(&(%s=%s))',
			$self->conf->get('ldap/searchattrs'), $username );
		my $result = $ldap->search( filter => $filter );
		my @entries = $result->entries();
		if ( scalar @entries > 1 ) {
			$self->log->error('Ambiguous response from LDAP server');
			return;
		}
		elsif ( scalar @entries < 1 ) {
			$self->log->error(
				'User ' . $username . ' not found in LDAP server' );
			return;
		}

		my $entry       = $entries[0];
		my $attr_map    = $self->conf->get('ldap/attr_map');
		my $extra_attrs = $self->conf->get('ldap/extra_attrs');
		ATTR_LOOP: foreach my $attr ( $entry->attributes() ) {
			$self->log->debug('checking attr: ' . $attr . ', val: ' . $entry->get_value($attr));
			foreach my $normalized_attr ( keys %{$attr_map} ) {
				if ( $attr eq $attr_map->{$normalized_attr} ) {
					$user_info->{$normalized_attr} = $entry->get_value($attr);
					next ATTR_LOOP;
				}
			}
			foreach my $normalized_attr ( keys %{$extra_attrs} ) {
				if ( $attr eq $extra_attrs->{$normalized_attr} ) {
					$user_info->{extra_attrs}->{$normalized_attr} =
					  $entry->get_value($attr);
					next ATTR_LOOP;
				}
			}
			if ( $attr eq $self->conf->get('ldap/groups_attr') ) {
				push @{ $user_info->{groups} }, $entry->get_value($attr);

				# Is the group this user is a member of a designated admin group?
				foreach my $group (@{ $user_info->{groups} }){
					if ( $self->conf->get('ldap/admin_groups')->{ $group } ){
						$self->log->debug( 'user ' . $user_info->{username} . ' is a member of admin group ' . $group );
						$user_info->{is_admin} = 1;
						next ATTR_LOOP;
					}
				}
			}
		}
	}
	elsif ($self->conf->get('auth_method') eq 'db'){
		die('No admin groups listed in admin_groups') unless $self->conf->get('admin_groups');
		my ($query, $sth);
		$query = 'SELECT groupname FROM groups t1 JOIN users_groups_map t2 ON (t1.uid=t2.uid) JOIN users t3 ON (t2.uid=t3.uid) WHERE t3.username=?';
		$sth = $heap->{dbh}->prepare($query);
		$sth->execute($username);
		while (my $row = $sth->fetchrow_hashref){
			push @{ $user_info->{groups} }, $row->{groupname};
		}
		# Is the group this user is a member of a designated admin group?
		foreach my $group (@{ $user_info->{groups} }){
			if ( $self->conf->get('admin_groups')->{ $group } ){
				$self->log->debug( 'user ' . $username . ' is a member of admin group ' . $group );
				$user_info->{is_admin} = 1;
			}
		}
	}
	else {
		$self->log->error('No auth_method');
		return;
	}

	# Get the uid
	my ( $query, $sth );
	$query = 'SELECT uid FROM users WHERE username=?';
	$sth   = $heap->{dbh}->prepare($query);
	$sth->execute( $user_info->{username} );
	my $row = $sth->fetchrow_hashref;
	if ($row) {
		$user_info->{uid} = $row->{uid};
	}
	else {
		# UID not found, so this is a new user and the corresponding user group,
		$self->log->debug('Creating user from : ' . Dumper($user_info));
		$user_info = $kernel->call( $_[SESSION], '_create_user', $user_info );
	}
	
	unless ($user_info){
		$self->log->error('Undefined user');
		return;
	}
	$self->log->debug('User info thus far: ' . Dumper($user_info));

	$user_info->{permissions} = $kernel->call($_[SESSION], '_get_permissions', $user_info->{groups})
		or ($self->log->error('Unable to get permissions') and return 0);
	$self->log->debug('got permissions: ' . Dumper($user_info->{permissions}));

	# Tack on a place to store queries
	$user_info->{qids} = {};

	# Record when the session started for timeout purposes
	$user_info->{session_start_time} = time();

	return $user_info;
}

sub _get_permissions {
	my ( $self, $kernel, $heap, $groups ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
	my ($query, $sth);
	
	# Find group permissions
	my %permissions;
	ATTR_LOOP: foreach my $attr qw(class_id host_id program_id){
		$query =
		  'SELECT DISTINCT attr_id' . "\n" .
		  'FROM groups t1' . "\n" .
		  'LEFT JOIN permissions t2 ON (t1.gid=t2.gid)' . "\n" .
		  'WHERE attr=? AND t1.groupname IN (';
		my @values = ( $attr );
		my @placeholders;
		foreach my $group ( @{ $groups } ) {
			push @placeholders ,       '?';
			push @values, $group;
		}
		$query .= join( ', ', @placeholders ) . ')';
		$sth = $heap->{dbh}->prepare($query);
		$sth->execute(@values);
		$permissions{$attr} = [];
		my @arr;
		while (my $row = $sth->fetchrow_hashref){
			# If at any point we get a zero, that means that all are allowed, no exceptions, so bug out to the next attr loop iter
			if ($row->{attr_id} eq '0' or $row->{attr_id} eq 0){
				$permissions{$attr} = { 0 => 1 };
				next ATTR_LOOP;
			}
			push @arr, $row->{attr_id};
		}
		$permissions{$attr} = { map { $_ => 1 } @arr };
	}
	
	# Get filters using the values/placeholders found above
	my @values;
	my @placeholders;
	foreach my $group ( @{ $groups } ) {
		push @values,       '?';
		push @placeholders, $group;
	}
	$query =
	    'SELECT filter FROM filters ' . "\n"
	  . 'JOIN groups ON (filters.gid=groups.gid) ' . "\n"
	  . 'WHERE groupname IN (';
	$query .= join( ', ', @values ) . ')';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute(@placeholders);
	$permissions{filter} = '';
	while ( my $row = $sth->fetchrow_hashref ) {
		$permissions{filter} .= ' ' . $row->{filter};
	}
	
	$self->log->debug('permissions: ' . Dumper(\%permissions));
	
	return \%permissions;
	
}

sub get_group_info {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg,$args) = _get_args(@_);

	my $info = $kernel->call( $_[SESSION], '_get_group_info', $args->{group} );
	$self->log->debug( "group info: " . Dumper($info) );

	$msg->body( $info );
	$msg->route();
}

sub _get_group_info {
	my ( $self, $kernel, $heap, $group ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];

	my ( $query, $sth );

	# Find group permissions
	$query = 'SELECT default_permissions_allow FROM groups WHERE groupname=?';
	$sth   = $heap->{dbh}->prepare($query);
	$sth->execute($group);

	my $row                      = $sth->fetchrow_hashref;
	my $default_permissions_bits = $row->{default_permissions_allow};
	$self->log->debug(
		"found default permissions bits $default_permissions_bits");

	my $permissions = {
		class_id => {
			default_allow => DEFAULT_CLASSES_BIT & $default_permissions_bits,
			exceptions    => {},
		},
		host_id => {
			default_allow => DEFAULT_HOSTS_BIT & $default_permissions_bits,
			exceptions    => {},
		},
		program_id => {
			default_allow => DEFAULT_PROGRAMS_BIT & $default_permissions_bits,
			exceptions    => {},
		}
	};

	# Get filters using the values/placeholders found above
	$query =
	    'SELECT filter FROM filters ' . "\n"
	  . 'JOIN groups ON (filters.gid=groups.gid) ' . "\n"
	  . 'WHERE groupname=?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($group);
	$permissions->{filter} = '';
	$row = $sth->fetchrow_hashref;
	$permissions->{filter} = $row->{filter};

	$query =
	    'SELECT attr, attr_id, allow ' . "\n"
	  . 'FROM groups t1' . "\n"
	  . 'LEFT JOIN permissions t2 ON (t1.gid=t2.gid)' . "\n"
	  . 'WHERE groupname=?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($group);

	while ( my $row = $sth->fetchrow_hashref ) {
		$permissions->{ $row->{attr} }->{exceptions}->{ $row->{attr_id} } = $row->{allow};
	}

	return $permissions;
}

sub _get_user_group_info {
	my ( $self, $kernel, $heap, $user_info ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];

	my ( $query, $sth );

	$query =
	    'SELECT groupname, attr, attr_id, allow ' . "\n"
	  . 'FROM groups t1' . "\n"
	  . 'LEFT JOIN permissions t2 ON (t1.gid=t2.gid)' . "\n"
	  . 'WHERE groupname IN (';
	my @values;
	my @placeholders;
	foreach my $group ( @{ $user_info->{groups} } ) {
		push @values,       '?';
		push @placeholders, $group;
	}
	$query .= join( ', ', @values ) . ')';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute(@placeholders);

	my $permissions = {};
	while ( my $row = $sth->fetchrow_hashref ) {
		$permissions->{ $row->{groupname} } = {}
		  unless $permissions->{ $row->{groupname} };
		$permissions->{ $row->{groupname} }->{ $row->{attr} } = {}
		  unless $permissions->{ $row->{groupname} }->{ $row->{attr} };
		$permissions->{ $row->{groupname} }->{ $row->{attr} }
		  ->{ $row->{attr_id} } = $row->{allow};
	}

	return $permissions;
}

sub _create_user {
	my ( $self, $kernel, $heap, $user_info ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];

	$self->log->info("Creating user $user_info->{username}");
	my ( $query, $sth );
	eval {
		$heap->{dbh}->begin_work;
		$query = 'INSERT INTO users (username) VALUES (?)';
		$sth   = $heap->{dbh}->prepare($query);
		$sth->execute( $user_info->{username} );
		$query = 'INSERT INTO groups (groupname) VALUES (?)';
		$sth   = $heap->{dbh}->prepare($query);
		$sth->execute( $user_info->{username} );
		$query =
		    'INSERT INTO users_groups_map (uid, gid) SELECT ' . "\n"
		  . '(SELECT uid FROM users WHERE username=?),' . "\n"
		  . '(SELECT gid FROM groups WHERE groupname=?)';
		$sth = $heap->{dbh}->prepare($query);
		$sth->execute( $user_info->{username}, $user_info->{username} );

		# TODO optimize this
		my $select  = 'SELECT groupname FROM groups WHERE groupname=?';
		my $sel_sth = $heap->{dbh}->prepare($select);
		$query = 'INSERT INTO groups (groupname) VALUES (?)';
		$sth   = $heap->{dbh}->prepare($query);
		foreach my $group ( @{ $user_info->{groups} } ) {
			$sel_sth->execute($group);
			my $row = $sel_sth->fetchrow_hashref;

			# Only do the insert if a previous entry did not exist
			unless ($row) {
				$sth->execute($group);
			}
		}

		$query = 'SELECT uid FROM users WHERE username=?';
		$sth   = $heap->{dbh}->prepare($query);
		$sth->execute( $user_info->{username} );
		my $row = $sth->fetchrow_hashref;
		if ($row) {
			$user_info->{uid} = $row->{uid};
		}
		else {
			$self->log->error(
				'Unable to find uid for user ' . $user_info->{username} );
			$heap->{dbh}->rollback;
			return;
		}
		
		$heap->{dbh}->commit;
	};
	if ($@) {
		$self->log->error( 'Database error: ' . $@ );
		return;
	}
	return $user_info;
}

sub _dbh_error_handler {
	my $errstr = shift;
	my $dbh    = shift;
	my $query  = $dbh->{Statement};

	$errstr .= " QUERY: $query";
	Log::Log4perl::get_logger(__PACKAGE__)->error($errstr);
	foreach my $sth (grep { defined } @{$dbh->{ChildHandles}}){
		$sth->rollback; # in case there was an active transaction
	}
	
	return 0;
}

sub get_stats {
	my ( $self, $kernel, $heap, $session ) = @_[ OBJECT, KERNEL, HEAP, SESSION ];
	my ($msg, $args) = _get_msg(@_);
	my $user = $msg->param('_user');
	
	# Get form params from backend
	my $backend_msg = POE::Event::Message->package($args);
	$backend_msg->setMode('call');
	$backend_msg->addRouteTo('post', 'agent', 'execute', 'web', 'get_stats');
	$backend_msg->addRemoteRouteTo(
		$self->conf->get('cluster/server'),
		$self->conf->get('cluster/port'),
		'sync',
	);
	
	my $ret = {};
	my $timeout = $self->conf->get('cluster/timeout') * 3; # give us more time for this than usual since it is sync 
	eval {	
		local $SIG{ALRM} = sub { die 'alarm'; };
		alarm $timeout;
		my ($node_ret) = $backend_msg->route();
		$ret->{nodes} = $node_ret->body();
		alarm 0;
	};
	if ($@){
		my $errmsg;
		if ($@ eq 'alarm'){
			$errmsg = 'Cluster connection timed out after ' . $timeout . ' seconds'
		}
		else {
			$errmsg = 'Error talking with peer nodes: ' . $@;
		}
		$self->log->error();
		$kernel->yield( '_error', $errmsg, $msg);
		return;
	}
	
	# Combine the stats info for the two nodes
	my $combined = {};
	$self->log->debug('got ret: ' . Dumper($ret->{nodes}));
	
	foreach my $stat qw(load index archive){
		$combined->{$stat} = { x => [], LogsPerSec => [], KBytesPerSec => [] };
		foreach my $node (keys %{ $ret->{nodes} }){
			if ($ret->{nodes}->{$node} and $ret->{nodes}->{$node}->{results} 
				and $ret->{nodes}->{$node}->{results}->{load_stats}){ 
				my $load_data = $ret->{nodes}->{$node}->{results}->{load_stats}->{$stat}->{data};
				next unless $load_data;
				for (my $i = 0; $i < (scalar @{ $load_data->{x} }); $i++){
					next unless $load_data->{x}->[$i];
					unless ($combined->{$stat}->{x}->[$i]){
						$combined->{$stat}->{x}->[$i] = $load_data->{x}->[$i];
					}
					$combined->{$stat}->{LogsPerSec}->[$i] += $load_data->{LogsPerSec}->[$i];
					$combined->{$stat}->{KBytesPerSec}->[$i] += $load_data->{KBytesPerSec}->[$i];
				}
			}	
		}
	}
	$ret->{combined_load_stats} = $combined;
	
	# Get query info
	$ret->{query_stats} = $kernel->call($session, '_get_stats');
	
	$self->log->debug('got stats: ' . Dumper($ret));
	$msg->body($ret);
	$msg->route();
}

sub _get_stats {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	
	my ($query, $sth);
	my $stats = {};
	my $days_ago = 7;
	my $limit = 20;
	
	# Queries per user
	$query = 'SELECT username, COUNT(*) AS count FROM query_log t1 JOIN users t2 ON (t1.uid=t2.uid)' . "\n" .
		'WHERE timestamp > DATE_SUB(NOW(), INTERVAL ? DAY)' . "\n" .
		'GROUP BY t1.uid ORDER BY count DESC LIMIT ?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($days_ago, $limit);
	$stats->{queries_per_user} = { x => [], User => [] };
	while (my $row = $sth->fetchrow_hashref){
		push @{ $stats->{queries_per_user}->{x} }, $row->{username};
		push @{ $stats->{queries_per_user}->{User} }, $row->{count};
	}
	
	# General query stats
	$query = 'SELECT DATE_FORMAT(timestamp, "%Y-%m-%d") AS x, COUNT(*) AS Count, AVG(milliseconds) AS Avg_Time, ' . "\n" .
		'SUM(num_results) AS Results, AVG(num_results) AS Avg_Results' . "\n" .
		'FROM query_log WHERE timestamp > DATE_SUB(NOW(), INTERVAL ? DAY) GROUP BY x LIMIT ?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($days_ago, $limit);
	$stats->{query_stats} = { x => [], Count => [], Avg_Time => [], Avg_Results => [] };
	while (my $row = $sth->fetchrow_hashref){
		foreach my $col (keys %{ $stats->{query_stats} }){
			push @{ $stats->{query_stats}->{$col} }, $row->{$col};
		}
	}
	
	return $stats;
}

sub get_form_params {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	my $user = $msg->param('_user');
		
	# Get form params from backend
	my $backend_msg = POE::Event::Message->package({ permissions => $user->{permissions} });
	$backend_msg->setMode('call');
	$backend_msg->addRouteTo('post', 'agent', 'execute', 'web', 'get_form_params');
	$backend_msg->addRemoteRouteTo(
		$self->conf->get('cluster/server'),
		$self->conf->get('cluster/port'),
		'sync',
	);
	
	my $ret;
	eval {	
		local $SIG{ALRM} = sub { die 'alarm'; };
		alarm $self->conf->get('cluster/timeout');
		($ret) = $backend_msg->route();
		alarm 0;
	};
	if ($@){
		$self->log->error('Cluster connection timed out after ' . $self->conf->get('cluster/timeout') . ' seconds');
		$kernel->yield( '_error', 'Cluster connection timed out after ' . $self->conf->get('cluster/timeout') . ' seconds',$msg);
		return;
	}
	$self->log->debug('got form params: ' . Dumper($ret));
	
	my ($query, $sth);
	$query = 'SELECT action_id, action FROM query_schedule_actions';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute();
	my @schedule_actions;
	while (my $row = $sth->fetchrow_hashref){
		push @schedule_actions, $row;
	}
	undef $sth;
	
	$ret->body()->{schedule_actions} = \@schedule_actions;
	
	if ($self->conf->get('inventory')){
		my $dbh = DBI->connect($self->conf->get('inventory/dsn'), 
			$self->conf->get('inventory/username'), 
			$self->conf->get('inventory/password'));
		unless ($dbh){
			$kernel->yield('_error', 'Invalid inventory db', $msg);
			return;
		}
		
		# This is designed for HP Service Manager
		$query = 'SELECT name FROM RMS_ASSIGNMENTM1 ORDER BY name ASC';
		$sth = $dbh->prepare($query);
		$sth->execute();
		my @assignments = ( 'AUTO' );
		while (my $row = $sth->fetchrow_hashref){
			push @assignments, $row->{name};
		}
		
		$ret->body()->{assignments} = \@assignments;
		
		#TODO find database location for these so they aren't hardcoded
		$ret->body()->{priority_codes} = {
			1 => '1-CRITICAL',
			2 => '2-URGENT',
			3 => '3-NORMAL',
			4 => '4-HOLD',
		};
	}
		
	$msg->body($ret->body());
	$msg->route();
}

sub get_schedule_actions {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	
	my ($query, $sth);
	$query = 'SELECT action_id, action FROM query_schedule_actions';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute();
	my @ret;
	while (my $row = $sth->fetchrow_hashref){
		push @ret, $row;
	}
	$msg->body(\@ret);
	$msg->route();
}

sub get_scheduled_queries {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	
	if ($args and ref($args) ne 'HASH'){
		$kernel->yield('_error', 'Invalid args: ' . Dumper($args), $msg);
		return;
	}
	elsif (not $args){
		$args = {};
	}
	
	my $offset = 0;
	if ( $args->{startIndex} ){
		$offset = sprintf('%d', $args->{startIndex});
	}
	my $limit = 10;
	if ( $args->{results} ) {
		$limit = sprintf( "%d", $args->{results} );
	}
	
	my ($query, $sth);
	
	$query = 'SELECT COUNT(*) AS totalRecords FROM query_schedule' . "\n" .
		'WHERE uid=?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($msg->param('_user')->{uid});
	my $row = $sth->fetchrow_hashref;
	my $totalRecords = $row->{totalRecords};
	
	$query = 'SELECT t1.id, query, frequency, start, end, action, action_params, enabled, UNIX_TIMESTAMP(last_alert) As last_alert, alert_threshold' . "\n" .
		'FROM query_schedule t1' . "\n" .
		'JOIN query_schedule_actions t2 ON (t1.action_id=t2.action_id)' . "\n" .
		'WHERE uid=?' . "\n" .
		'ORDER BY t1.id DESC' . "\n" .
		'LIMIT ?,?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($msg->param('_user')->{uid}, $offset, $limit);
	my @rows;
	while (my $row = $sth->fetchrow_hashref){
		push @rows, $row;
	}
	my $ret = {
		'results' => [ @rows ],
		'totalRecords' => $totalRecords,
		'recordsReturned' => scalar @rows,
	};
	$msg->body($ret);
	$msg->route();
}

sub schedule_query {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	
	foreach my $item qw(qid days time_unit action_id){	
		unless (defined $args->{$item}){
			$kernel->yield('_error', 'Invalid args, missing arg: ' . $item, $msg);
			return;
		}
	}
	
	# Make sure these params are ints
	foreach my $item qw(qid days time_unit count action_id){
		next unless $args->{$item};
		$args->{$item} = sprintf('%d', $args->{$item});
	}
	$args->{uid} = sprintf('%d', $msg->param('_user')->{uid});
	
	my %standard_vars = map { $_ => 1 } qw(uid qid days time_unit count action_id threshold_count threshold_time_unit);
	my $schedule_query_params = { action_params => {} };
	foreach my $item (keys %{$args}){
		if ($standard_vars{$item}){
			$schedule_query_params->{$item} = $args->{$item};
		}
		else {
			$schedule_query_params->{action_params}->{$item} = $args->{$item};
		}
	}
	$schedule_query_params->{action_params} = encode_json($schedule_query_params->{action_params});
	
	my $ret = $kernel->call( $_[SESSION], '_schedule_query', $schedule_query_params);

	$msg->body({return => $ret});
	$msg->route();
}

sub _schedule_query {
	my ( $self, $kernel, $heap, $args ) =
	  @_[ OBJECT, KERNEL, HEAP, ARG0 ];
	$self->log->debug('got args ' . Dumper($args));
	my @frequency;
	for (my $i = 1; $i <= 7; $i++){
		if ($i eq $args->{time_unit}){
			$frequency[$i-1] = 1;
		}
		else {
			$frequency[$i-1] = 0;
		}
	}
	my $freq_str = join(':', @frequency);
	$self->log->debug('freq_str: ' . $freq_str);
	
	my ($query, $sth);
	$query = 'INSERT INTO query_schedule (uid, query, frequency, start, end, action_id, action_params, last_alert, alert_threshold) VALUES (?, ' . "\n" .
		'(SELECT query FROM query_log WHERE qid=?), ?, ?, ?, ?, ?, "1970-01-01 00:00:00", ?)';
	$sth = $heap->{dbh}->prepare($query);
	my $days = $args->{days};
	unless ($days){
		$days = 2^32;
	}
	my $alert_threshold = 0;
	my $time_unit_map = {
		1 => (60 * 60 * 24 * 365),
		2 => (60 * 60 * 24 * 30),
		3 => (60 * 60 * 24 * 7),
		4 => (60 * 60 * 24),
		5 => (60 * 60),
		6 => (60),
	};
	if ($args->{threshold_count} and $args->{threshold_time_unit}){
		$alert_threshold = $time_unit_map->{ $args->{threshold_time_unit} } * $args->{threshold_count};
	}
	$sth->execute($args->{uid}, $args->{qid}, $freq_str, time(), (86400 * $days) + time(), 
		$args->{action_id}, $args->{action_params}, $alert_threshold);
	my $ok = $sth->rows;
	
	return $ok;
}

sub _run_schedule {
	my ( $self, $kernel, $session, $heap, $state ) = @_[ OBJECT, KERNEL, SESSION, HEAP, STATE ];
	
	$self->log->debug('Current number of events in queue: ' . $kernel->get_event_count());
	
	# Reset the schedule alarm
	$kernel->delay($state, $self->conf->get('schedule_interval'));
	
	my ($query, $sth);
	
	# Find the last run time from the bookmark table
	$query = 'SELECT UNIX_TIMESTAMP(last_run) FROM schedule_bookmark';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute();
	my $row = $sth->fetchrow_arrayref;
	my $last_run_bookmark = $self->conf->get('schedule_interval'); # init to interval here so we don't underflow if 0
	if ($row){
		$last_run_bookmark = $row->[0];
	}
	
	# Get form params from backend so we can get the latest available index time
	my $backend_msg = POE::Event::Message->package({});
	$backend_msg->setMode('call');
	$backend_msg->addRouteTo('post', 'agent', 'execute', 'web', 'get_form_params');
	$backend_msg->addRemoteRouteTo(
		$self->conf->get('cluster/server'),
		$self->conf->get('cluster/port'),
		'sync',
	);
	
	my $form_params_msg;
	eval {	
		local $SIG{ALRM} = sub { die 'alarm'; };
		alarm $self->conf->get('cluster/timeout');
		($form_params_msg) = $backend_msg->route();
		alarm 0;
	};
	if ($@){
		$self->log->error('Cluster connection timed out after ' . $self->conf->get('cluster/timeout') . ' seconds');
		$kernel->yield( '_error', 'Cluster connection timed out after ' . $self->conf->get('cluster/timeout') . ' seconds', $backend_msg);
		
		return;
	}
	my $form_params = $form_params_msg->body();
	
	# Expire schedule entries
	$query = 'SELECT id, query, username FROM query_schedule JOIN users ON (query_schedule.uid=users.uid) WHERE end < UNIX_TIMESTAMP() AND enabled=1';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute();
	my @ids;
	while (my $row = $sth->fetchrow_hashref){
		push @ids, $row->{id};
		
		my $user_info = $kernel->call( $session, '_get_user_info', $row->{username} );
		
		my $decode = decode_json($row->{query});
		
		my $headers = {
			To => $user_info->{email},
			From => $self->conf->get('email/display_address') ? $self->conf->get('email/display_address') : 'system',
			Subject => 'ELSA alert has expired for query ' . $decode->{query_params},
		};
		my $body = 'The alert set for query ' . $decode->{query_params} . ' has expired and has been disabled.  ' .
			'If you wish to continue receiving this query, please log into ELSA, enable the query, and set a new expiration date.';
		
		$kernel->yield('_send_email', $headers, $body);
	}
	if (scalar @ids){
		$self->log->info('Expiring query schedule for ids ' . join(',', @ids));
		$query = 'UPDATE query_schedule SET enabled=0 WHERE id IN (' . join(',', @ids) . ')';
		$sth = $heap->{dbh}->prepare($query);
		$sth->execute;
	}
	
	# Run schedule	
	$query = 'SELECT t1.id AS query_schedule_id, username, t1.uid, query, frequency, start, end, action_subroutine, action_params' . "\n" .
		'FROM query_schedule t1' . "\n" .
		'JOIN users ON (t1.uid=users.uid)' . "\n" .
		'JOIN query_schedule_actions t2 ON (t1.action_id=t2.action_id)' . "\n" .
		'WHERE start <= ? AND end >= ? AND enabled=1' . "\n" .
		'AND UNIX_TIMESTAMP(NOW()) - UNIX_TIMESTAMP(last_alert) > alert_threshold';  # we won't even run queries we know we won't alert on
	$sth = $heap->{dbh}->prepare($query);
	
	my $cur_time = $form_params->{end_int};
	$sth->execute($cur_time, $cur_time);
	
	my $user_info_cache = {};
	
	while (my $row = $sth->fetchrow_hashref){
		my @freq_arr = split(':', $row->{frequency});
		my $last_run;
		my $farthest_back_to_check = $cur_time - $self->conf->get('schedule_interval');
		my $how_far_back = $self->conf->get('schedule_interval');
		while (not $last_run and $farthest_back_to_check > ($cur_time - (86400 * 366 * 2))){ # sanity check
			$self->log->debug('$farthest_back_to_check:' . $farthest_back_to_check);
			my @prev_dates = ParseRecur($row->{frequency}, 
				ParseDate(scalar localtime($cur_time)), 
				ParseDate(scalar localtime($farthest_back_to_check)),
				ParseDate(scalar localtime($cur_time - 1))
			);
			if (scalar @prev_dates){
				$self->log->debug('prev: ' . Dumper(\@prev_dates));
				$last_run = UnixDate($prev_dates[$#prev_dates], '%s');
				$self->log->debug('last_run:' . $prev_dates[$#prev_dates]);
			}
			else {
				# Keep squaring the distance we'll go back to find the last date
				$farthest_back_to_check -= $how_far_back;
				$self->log->debug('how_far_back: ' . $how_far_back);
				$how_far_back *= $self->conf->get('schedule_interval');
			}
		}
		unless ($last_run){
			$self->log->error('Could not find the last time we ran, aborting');
			next;
		}
		# If the bookmark is earlier, use that because we could've missed runs between them
		if ($last_run_bookmark < $last_run){
			$self->log->info('Setting last_run to ' . $last_run_bookmark . ' because it is before ' . $last_run);
			$last_run = $last_run_bookmark;
		}
		my @dates = ParseRecur($row->{frequency}, 
			ParseDate(scalar localtime($cur_time)), 
			ParseDate(scalar localtime($cur_time)),
			ParseDate(scalar localtime($cur_time + $self->conf->get('schedule_interval')))
		);
		$self->log->debug('dates: ' . Dumper(\@dates) . ' row: ' . Dumper($row));
		if (scalar @dates){
			# Adjust the query time to avoid time that is potentially unindexed by offsetting by the schedule interval
			my $query = decode_json($row->{query});
			$query->{query_meta_params}->{start} = ($last_run - $self->conf->get('schedule_interval'));
			$query->{query_meta_params}->{end} = ($cur_time - $self->conf->get('schedule_interval'));
			
			my $args = { 
				q => encode_json($query),
				session_id => 1,
			};
			my $msg = POE::Event::Message->package($args);
			if (!$user_info_cache->{ $row->{uid} }){
				$user_info_cache->{ $row->{uid} } = $kernel->call( $session, '_get_user_info', $row->{username} );
				$self->log->debug('Got user info: ' . Dumper($user_info_cache->{ $row->{uid} }));
			}
			else {
				$self->log->debug('Using existing user info');
			}
			
			$msg->param('_user', $user_info_cache->{ $row->{uid} });
			$msg->addRouteBack('post', undef, $row->{action_subroutine});
			#TODO find a better, non-duck-tape way of doing this
			my $action_params = { 
				comments => 'Scheduled Query ' . $row->{query_schedule_id}, 
				query_schedule_id => $row->{query_schedule_id},
				query => $query
			};
			if ($row->{action_params}){
				my $stored_params = decode_json($row->{action_params});
				foreach my $stored_param (keys %{ $stored_params }){
					$action_params->{$stored_param} = $stored_params->{$stored_param};
				}
			}
			$msg->param('_action_params', $action_params);
			$msg->addRouteTo('post', undef, 'query');
			$self->log->debug('routing query msg: ' . Dumper($msg));
			$msg->route();
		} 
	}
	
	# Update our bookmark to the current run
	$query = 'UPDATE schedule_bookmark SET last_run=FROM_UNIXTIME(?)';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($cur_time);
	unless ($sth->rows){
		$query = 'INSERT INTO schedule_bookmark (last_run) VALUES (FROM_UNIXTIME(?))';
		$sth = $heap->{dbh}->prepare($query);
		$sth->execute($cur_time);
	}
}

sub delete_saved_results {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	$self->log->debug('args: ' . Dumper($args));
	unless ($args->{qid}){
		$kernel->yield('_error', 'Invalid args, no qid: ' . Dumper($args), $msg);
		return;
	}
	my ($query, $sth);
	# Verify this query belongs to the user
	$query = 'SELECT uid FROM query_log WHERE qid=?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($args->{qid});
	my $row = $sth->fetchrow_hashref;
	unless ($row){
		$kernel->yield('_error', 'Invalid args, no results found for qid: ' . Dumper($args), $msg);
		return;
	}
	unless ($row->{uid} eq $msg->param('_user')->{uid} or $msg->param('_user')->{is_admin}){
		$kernel->yield('_error', 'Unable to alter these saved results based on your authorization: ' . Dumper($args), $msg);
		return;
	}
	$query = 'DELETE FROM saved_results WHERE qid=?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($args->{qid});
	if ($sth->rows){
		$msg->body({deleted => $sth->rows});
		$msg->route();
	}
	else {
		$kernel->yield('_error', 'Query ID ' . $args->{qid} . ' not found!', $msg);
	}
}

sub delete_scheduled_query {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	$self->log->debug('args: ' . Dumper($args));
	unless ($args->{id}){
		$kernel->yield('_error', 'Invalid args, no id: ' . Dumper($args), $msg);
		return;
	}
	my ($query, $sth);
	$query = 'DELETE FROM query_schedule WHERE uid=? AND id=?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($msg->param('_user')->{uid}, $args->{id});
	if ($sth->rows){
		$msg->body({deleted => $sth->rows});
		$msg->route();
	}
	else {
		$kernel->yield('_error', 'Schedule ID ' . $args->{id} . ' not found!', $msg);
	}
}

sub update_scheduled_query {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	$self->log->debug('args: ' . Dumper($args));
	unless ($args->{id}){
		$kernel->yield('_error', 'Invalid args, no id: ' . Dumper($args), $msg);
		return;
	}
	my $attr_map = {};
	foreach my $item qw(query frequency start end action action_params enabled alert_threshold){
		$attr_map->{$item} = $item;
	}
	my ($query, $sth);
	my $new_args = {};
	foreach my $given_arg (keys %{ $args }){
		next if $given_arg eq 'id';
		unless ($attr_map->{$given_arg}){
			$kernel->yield('_error', 'Invalid arg: ' . $given_arg, $msg);
			return;
		}
		# Chop quotes
		$args->{$given_arg} =~ s/^['"](.+)['"]$/$1/;
		
		# Adjust timestamps if necessary
		if ($given_arg eq 'start' or $given_arg eq 'end'){
			$args->{$given_arg} = UnixDate($args->{$given_arg}, '%s');
		}
		
		# Convert action to action_id
		if ($given_arg eq 'action'){
			$given_arg = 'action_id';
			$args->{'action_id'} = delete $args->{'action'};
		}
		
		$self->log->debug('given_arg: ' . $given_arg . ': ' . $args->{$given_arg});
		$query = sprintf('UPDATE query_schedule SET %s=? WHERE id=?', $given_arg);
		$sth = $heap->{dbh}->prepare($query);
		$sth->execute($args->{$given_arg}, $args->{id});
		$new_args->{$given_arg} = $args->{$given_arg};
	}
	
	$msg->body($new_args);
	$msg->route();
}

sub save_results {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	$self->log->debug(Dumper($msg));
	my $comments = $args->{comments};
	eval {
		$args = decode_json($args->{results});
	};
	if ($@){
		$kernel->yield('_error', $@, $msg);
		return;
	}
	unless ($args->{qid} and $args->{results} and ref($args->{results}) eq 'ARRAY'){
		$kernel->yield('_error', 'Invalid args: ' . Dumper($args),  $msg);
		return;
	}
	$args->{comments} = $comments;
	
	$msg->body($args);
	$msg->addRouteTo('post', undef, '_save_results');
	$msg->route();
}

sub _save_results {
	my ( $self, $kernel, $session, $heap ) = @_[ OBJECT, KERNEL, SESSION, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	$self->log->debug('got results to save: ' . Dumper($msg->body()));
	unless (ref($msg->body()) eq 'HASH' 
		and $msg->body()->{results} 
		and ref($msg->body()->{results}) eq 'ARRAY'){
		$self->log->info('Invalid results for query');
		$kernel->yield('_error', 'No results to save', $msg);
		return 0;
	}
	
	my $meta_info = {};
	my $results;
	
	if (scalar @{ $msg->body()->{results} }){
		$results = [ @{ $msg->body()->{results} } ];
	}
	elsif (scalar keys %{ $msg->body()->{groups} }){
		foreach my $group_name (keys %{ $msg->body()->{groups} }){
			$meta_info->{groupby} = $group_name;
			$results = [ @{ $msg->body()->{groups}->{$group_name} } ];
			last; # only do the first one
		}
	}
	else {
		$self->log->info('No results for query');
		$kernel->yield('_error', 'No results to save', $msg);
		return 0;
	}
	
	my ($query, $sth);
	
	$heap->{dbh}->begin_work;
	$query = 'INSERT INTO saved_results (qid, meta_info, comments) VALUES(?,?,?)';
	$sth = $heap->{dbh}->prepare($query);
	
	if ($msg->param('_action_params') and ref($msg->param('_action_params')) eq 'HASH'){
		foreach my $key (keys %{ $msg->param('_action_params') }){
			$meta_info->{$key} = $msg->param('_action_params')->{$key};
		}
	}
	
	$meta_info->{totalRecords} = (scalar @{ $results });
	$meta_info->{qid} = $msg->body()->{qid};
	
	eval {
		$sth->execute($msg->body()->{qid}, encode_json($meta_info), $msg->body()->{comments});
		$query = 'INSERT INTO saved_results_rows (qid, data) VALUES (?,?)';
		$sth = $heap->{dbh}->prepare($query);
		foreach my $row (@{ $results }){
			$sth->execute($meta_info->{qid}, encode_json($row));
		}
	};
	if ($@){
		$heap->{dbh}->rollback;
		$kernel->yield('_error', $@, $msg);
		return;
	}
	
	$heap->{dbh}->commit;
	
	$msg->body(1);
	$msg->route();
}

sub get_saved_result {
	my ( $self, $kernel, $session, $heap ) = @_[ OBJECT, KERNEL, SESSION, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	unless ($args and ref($args) eq 'HASH' and $args->{qid}){
		$kernel->yield('_error', 'Invalid args: ' . Dumper($args), $msg);
		return;
	}
	
	# Authenticate the hash if given (so that the uid doesn't have to match)
	if ($args->{hash} and $args->{hash} ne $self->_get_hash($args->{qid}) ){
		$kernel->yield('_error', q{You are not authorized to view another user's saved queries}, $msg);
		return;
	}
	
	my @values = ($args->{qid});
	
	my ($query, $sth);
	$query = 'SELECT t2.uid, t2.query, meta_info FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid)' . "\n" .
		'WHERE t1.qid=?';
	if (not $args->{hash}){
		$query .= ' AND uid=?';
		push @values, $msg->param('_user')->{uid};
	}
	
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute(@values);
	my $row = $sth->fetchrow_hashref;
	unless ($row){
		$kernel->yield('_error', 'No saved results for qid ' . $args->{qid} . ' found.', $msg);
		return;
	}
	my $results = decode_json($row->{meta_info});
	my $saved_query = decode_json($row->{query});
	foreach my $item qw(query_params query_meta_params){
		$results->{$item} = $saved_query->{$item};
	}
	$results->{results} = [];
	if ($results->{groupby}){
		$results->{groups} = {};
		$results->{groups}->{ $results->{groupby} } = [];
	}
	
	$query = 'SELECT data FROM saved_results_rows WHERE qid=? ORDER BY rowid ASC';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($args->{qid});
	while (my $row = $sth->fetchrow_hashref){
		if ($results->{groupby}){
			push @{ $results->{groups}->{ $results->{groupby} } }, decode_json($row->{data}); 
		}
		else {	
			push @{ $results->{results} }, decode_json($row->{data});
		}
	}
	$msg->body($results);
	$msg->route();
}

sub get_saved_queries {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	
	if ($args and ref($args) ne 'HASH'){
		$kernel->yield('_error', 'Invalid args: ' . Dumper($args), $msg);
		return;
	}
	elsif (not $args){
		$args = {};
	}
	
	my $offset = 0;
	if ( $args->{startIndex} ){
		$offset = sprintf('%d', $args->{startIndex});
	}
	my $limit = 10;
	if ( $args->{results} ) {
		$limit = sprintf( "%d", $args->{results} );
	}
	
	my $uid = $msg->param('_user')->{uid};
	if ($args->{uid}){
		$uid = sprintf('%d', $args->{uid});
	}
	if ($uid ne $msg->param('_user')->{uid} and not $msg->param('_user')->{is_admin}){
		$kernel->yield('_error', q{You are not authorized to view another user's saved queries}, $msg);
		return;	
	}
	
	
	my $saved_queries;
	if ($args->{qid} and not ($args->{startIndex} or $args->{results})){
		# We're just getting one known query
		$saved_queries = $kernel->call( $_[SESSION], '_get_saved_query', sprintf('%d', $args->{qid}) );
	}
	else {
		$saved_queries = $kernel->call( $_[SESSION], '_get_saved_queries', $uid, $offset, $limit );
	}
	

	$self->log->debug( "saved_queries: " . Dumper($saved_queries) );
	$msg->body($saved_queries);
	$msg->route();
}

sub _get_saved_query {
	my ( $self, $kernel, $heap, $qid ) =
	  @_[ OBJECT, KERNEL, HEAP, ARG0 ];
	
	my ( $query, $sth, $row );
	
	$query = 'SELECT t1.qid, t2.query, comments, meta_info' . "\n" .
			'FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid)' . "\n" .
			'WHERE t2.qid=?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($qid);
	
	return $sth->fetchrow_hashref or {error => 'QID ' . $qid . ' not found.'};
}

sub _get_saved_queries {
	my ( $self, $kernel, $heap, $uid, $offset, $limit ) =
	  @_[ OBJECT, KERNEL, HEAP, ARG0 .. ARG2 ];
	$limit = 100 unless $limit;

	my ( $query, $sth, $row );
	
	# First find total number
	$query =
	    'SELECT COUNT(*) AS totalRecords ' . "\n"
	  . 'FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid)' . "\n"
	  . 'WHERE uid=?'; #AND comments!=\'_alert\'';
	$sth = $heap->{dbh}->prepare($query) or die( $heap->{dbh}->errstr );
	$sth->execute( $uid );
	$row = $sth->fetchrow_hashref;
	my $totalRecords = $row->{totalRecords} ? $row->{totalRecords} : 0;

	# Find our type of database and use the appropriate query
	my $db_type = $heap->{dbh}->get_info(17);    #17 == SQL_DBMS_NAME
	if ( $db_type =~ /Microsoft SQL Server/ ) {
		# In MS-SQL, we don't have the niceties of OFFSET, so we have to do this via subqueries
		my $outer_top = $offset + $limit;
		$query = 'SELECT * FROM ' . "\n" .
			'(SELECT TOP ? * FROM ' . "\n" .
			'(SELECT TOP ? t1.qid, t2.query, comments, meta_info FROM ' . "\n" .
			'FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid) ' . "\n" .
		  	'WHERE uid=? ' . "\n" .
		  	'ORDER BY qid DESC) OverallTop ' . "\n" .
		  	'ORDER BY qid ASC) TopOfTop ' . "\n" .
		  	'ORDER BY qid DESC';
		$sth = $heap->{dbh}->prepare($query) or die( $heap->{dbh}->errstr );
		$sth->execute($limit, ($offset + $limit), $uid);  
	}
	else {
		$query =
		    'SELECT t1.qid, t2.query, comments, num_results, UNIX_TIMESTAMP(timestamp) AS timestamp, meta_info ' . "\n"
		  . 'FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid) ' . "\n"
		  . 'WHERE uid=?' . "\n"
		  . 'ORDER BY qid DESC LIMIT ?,?';
		$sth = $heap->{dbh}->prepare($query) or die( $heap->{dbh}->errstr );
		$sth->execute( $uid, $offset, $limit );
	}

	my $queries = [];    # only save the latest unique query
	while ( my $row = $sth->fetchrow_hashref ) {
		# we have to decode this to make sure it doesn't end up as a string
		my $meta_info = decode_json( $row->{meta_info} );
		my $decode = decode_json($row->{query});
		my $query = $decode->{query_params};
		push @{$queries}, { 
			qid => $row->{qid},
			timestamp => $row->{timestamp}, #$meta_info->{time},
			query => $query, 
			num_results => $row->{num_results}, #$meta_info->{recordsReturned}, 
			comments => $row->{comments},
			hash => $self->_get_hash($row->{qid}),
		};
	}
	return { 
		totalRecords => $totalRecords,
		recordsReturned => scalar @$queries,
		results => [ @{$queries} ] 
	};
}

sub get_previous_queries {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	
	my $offset = 0;
	if ( $args->{startIndex} ){
		$offset = sprintf('%d', $args->{startIndex});
	}
	my $limit = $self->conf->get('previous_queries_limit');
	if ( $args->{results} ) {
		$limit = sprintf( "%d", $args->{results} );
	}
	my $dir = 'DESC';
	if ( $args->{dir} and $args->{dir} eq 'asc'){
		$dir = 'ASC';
	}
	my $previous_queries =
	  $kernel->call( $_[SESSION], '_get_previous_queries',
		$msg->param('_user')->{uid}, $offset, $limit, $dir );

	$self->log->debug( "previous_queries: " . Dumper($previous_queries) );
	$msg->body($previous_queries);
	$msg->route();
}

sub _get_previous_queries {
	my ( $self, $kernel, $heap, $uid, $offset, $limit, $dir ) =
	  @_[ OBJECT, KERNEL, HEAP, ARG0 .. ARG3 ];
	$limit = 100 unless $limit;

	my ( $query, $sth, $row );
	
	# First find total number
	$query =
	    'SELECT COUNT(*) AS totalRecords ' . "\n"
	  . 'FROM query_log ' . "\n"
	  . 'WHERE uid=?';
	$sth = $heap->{dbh}->prepare($query) or die( $heap->{dbh}->errstr );
	$sth->execute( $uid );
	$row = $sth->fetchrow_hashref;
	my $totalRecords = $row->{totalRecords} ? $row->{totalRecords} : 0;

	# Find our type of database and use the appropriate query
	my $db_type = $heap->{dbh}->get_info(17);    #17 == SQL_DBMS_NAME
	if ( $db_type =~ /Microsoft SQL Server/ ) {
		# In MS-SQL, we don't have the niceties of OFFSET, so we have to do this via subqueries
		my $outer_top = $offset + $limit;
		$query = 'SELECT * FROM ' . "\n" .
			'(SELECT TOP ? qid, query, timestamp, num_results, milliseconds FROM ' . "\n" .
			'(SELECT TOP ? qid, query, timestamp, num_results, milliseconds FROM ' . "\n" .
			'FROM query_log ' . "\n" .
		  	'WHERE uid=?' . "\n" .
		  	'ORDER BY qid DESC) OverallTop ' . "\n" .
		  	'ORDER BY qid ASC) TopOfTop ' . "\n" .
		  	'ORDER BY qid DESC';
		$sth = $heap->{dbh}->prepare($query) or die( $heap->{dbh}->errstr );
		$sth->execute($limit, ($offset + $limit), $uid);
	}
	else {
		$query =
		    'SELECT qid, query, timestamp, num_results, milliseconds ' . "\n"
		  . 'FROM query_log ' . "\n"
		  . 'WHERE uid=? AND system=0' . "\n"
		  . 'ORDER BY qid ' . $dir . ' LIMIT ?,?';
		$sth = $heap->{dbh}->prepare($query) or die( $heap->{dbh}->errstr );
		$sth->execute( $uid, $offset, $limit );
	}

	my $queries = [];    # only save the latest unique query
	while ( my $row = $sth->fetchrow_hashref ) {
		if ( $row->{query} ) {

			# we have to decode this to make sure it doesn't end up as a string
			my $prev_query = decode_json( $row->{query} );
			if (    $prev_query
				and ref($prev_query) eq 'HASH'
				and $prev_query->{query_params} )
			{
				push @{$queries},
				  {
					qid          => $row->{qid},
					query        => $prev_query->{query_params},
					query_obj    => $prev_query,
					timestamp    => $row->{timestamp},
					num_results  => $row->{num_results},
					milliseconds => $row->{milliseconds},
				  };

			}
		}
	}
	return { 
		totalRecords => $totalRecords,
		recordsReturned => scalar @$queries,
		results => [ @{$queries} ] 
	};
}

sub get_query_auto_complete {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	
	if ( not $args->{query} ) {
		$kernel->yield( '_error', "No query specified", $msg );
		return;
	}

	my $limit = $self->conf->get('previous_queries_limit');
	if ( $args->{limit} ) {
		$limit = sprintf( "%d", $args->{limit} );
	}
	my $auto_completes =
	  $kernel->call( $_[SESSION], '_get_query_auto_complete',
		$msg->param('_user')->{uid},
		$args->{query}, $limit );

	$msg->body({ results => $auto_completes });
	$msg->route();
}

sub _get_query_auto_complete {
	my ( $self, $kernel, $heap, $uid, $queries_to_find, $limit ) =
	  @_[ OBJECT, KERNEL, HEAP, ARG0 .. ARG2 ];

	# Sadly, we must sprintf this limit and inline it since FreeTDS won't allow a placeholder for TOP
	$limit = sprintf( "%d", $limit );
	$limit = 100 unless $limit;
	my $like = q/%},"query_params":"/ . $queries_to_find . '%';
	my ( $query, $sth );

	# Find our type of database and use the appropriate query
	my $db_type = $heap->{dbh}->get_info(17);    #17 == SQL_DBMS_NAME
	if ( $db_type =~ /Microsoft SQL Server/ ) {
		$query =
		    'SELECT TOP ' 
		  . $limit
		  . ' qid, query, timestamp, num_results, milliseconds ' . "\n"
		  . 'FROM query_log ' . "\n"
		  . 'WHERE uid=? AND query LIKE ?' . "\n"
		  . 'ORDER BY qid DESC';
		$sth = $heap->{dbh}->prepare($query) or die( $heap->{dbh}->errstr );
		$sth->execute( $uid, $like );
	}
	else {
		$query =
		    'SELECT qid, query, timestamp, num_results, milliseconds ' . "\n"
		  . 'FROM query_log ' . "\n"
		  . 'WHERE uid=? AND query LIKE ? ' . "\n"
		  . 'ORDER BY qid DESC LIMIT ?';
		$sth = $heap->{dbh}->prepare($query) or die( $heap->{dbh}->errstr );
		$sth->execute( $uid, $like, $limit );
	}

	my $queries = {};    # only save the latest unique query
	while ( my $row = $sth->fetchrow_hashref ) {
		if ( $row->{query} ) {
			my $prev_query = decode_json( $row->{query} );
			if (    $prev_query
				and ref($prev_query) eq 'HASH'
				and $prev_query->{query_params} )
			{
				unless (
					    $queries->{ $prev_query->{query_params} }
					and $queries->{ $prev_query->{query_params} }->{timestamp}
					cmp    # stored date is older
					$row->{timestamp} < 0
				  )
				{
					$queries->{ $prev_query->{query_params} } = {
						qid          => $row->{qid},
						query        => $prev_query->{query_params},
						timestamp    => $row->{timestamp},
						num_results  => $row->{num_results},
						milliseconds => $row->{milliseconds},
					};
				}
			}
		}
	}
	return [ values %{$queries} ];
}

sub _stop {
	my ( $self, $kernel, $heap, $args ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
	$self->log->debug( '_stop got args: ' . Dumper($args) );
}

sub _get_msg {
	my ($msg, $args);
	if ($_[ARG1] 
		and ref($_[ARG1]) eq 'ARRAY' 
		and $_[ARG1]->[0] 
		and ref($_[ARG1]->[0]) eq 'POE::Event::Message'){
		$msg = $_[ARG1]->[0];
		$args = $msg->body();
	}
	else {
		die('Invalid args: ' . Dumper($_[ARG0]) . Dumper($_[ARG1]));
	}
	return ($msg,$args);
}

sub _get_hash {
	my ($self, $data) = @_;
	
	my $digest = new Digest::HMAC_SHA1($self->conf->get('link_key'));
	$digest->add($data);
	return $digest->hexdigest();
}

sub _send_email {
	my ( $self, $kernel, $session, $heap, $headers, $body ) = @_[ OBJECT, KERNEL, SESSION, HEAP, ARG0..ARG1 ];
	
	$self->log->debug('headers: ' . Dumper($headers));
	
	# Send the email
	my $email_headers = new Mail::Header();
	$email_headers->header_hashref($headers);
	my $email = new Mail::Internet( Header => $email_headers, Body => [ split(/\n/, $body) ] );
	
	$self->log->debug('email: ' . $email->as_string());
	$email->smtpsend(
		Host => $self->conf->get('email/smtp_server'), 
		Debug => 1, 
		MailFrom => $self->conf->get('email/display_address')
	);
	$self->log->debug('done sending email');
}

sub _open_ticket {
	my ( $self, $kernel, $session, $heap ) = @_[ OBJECT, KERNEL, SESSION, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	$self->log->debug('got results to create ticket on: ' . Dumper($msg));
	unless (ref($msg->body()) eq 'HASH' 
		and $msg->body()->{results} 
		and ref($msg->body()->{results}) eq 'ARRAY'
		and scalar @{ $msg->body()->{results} }){
		$self->log->info('No results for query');
		$msg->setErr(ERROR, 'No results for query');
		$msg->route();
		return 0;
	}
	
	my $meta_info = { %{ $msg->body() } };
	if ($msg->param('_action_params') and ref($msg->param('_action_params')) eq 'HASH'){
		foreach my $key (keys %{ $msg->param('_action_params') }){
			$meta_info->{$key} = $msg->param('_action_params')->{$key};
		}
	}
	
	$self->log->debug('meta_info: ' . Dumper($meta_info));
	
	unless ($self->conf->get('ticketing/email')){
		$self->log->error('No ticketing config setup.');
		return;
	}
	
	my $headers = {
		To => $self->conf->get('ticketing/email'),
		From => $self->conf->get('email/display_address') ? $self->conf->get('email/display_address') : 'system',
		Subject => $self->conf->get('email/subject') ? $self->conf->get('email/subject') : 'system',
	};
	my $body = sprintf($self->conf->get('ticketing/template'), $meta_info->{query}->{query_params},
		sprintf('%s/get_results?qid=%d&hash=%s', 
			$self->conf->get('email/base_url') ? $self->conf->get('email/base_url') : 'http://localhost',
			$meta_info->{qid},
			$meta_info->{hash},
		)
	);
	
	$kernel->yield('_send_email', $meta_info->{query_schedule_id}, $headers, $body, $msg->body());
}

sub _alert {
	my ( $self, $kernel, $session, $heap ) = @_[ OBJECT, KERNEL, SESSION, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	$self->log->debug('got results to alert on: ' . Dumper($msg));
	
	# Route back if something is wrong
	if ($msg->status()){
		$self->log->error('Got msg status: ' . $msg->status() . ' from msg ' . Dumper($msg));
		$msg->routeBack();
		return;
	}
	unless (ref($msg->body()) eq 'HASH' 
		and $msg->body()->{results} 
		and ref($msg->body()->{results}) eq 'ARRAY'
		and scalar @{ $msg->body()->{results} }){
		$self->log->info('No results for query');
		$msg->setErr(ERROR, 'No results for query');
		$msg->route();
		return 0;
	}
	
	my $meta_info = { %{ $msg->body() } };
	if ($msg->param('_action_params') and ref($msg->param('_action_params')) eq 'HASH'){
		foreach my $key (keys %{ $msg->param('_action_params') }){
			$meta_info->{$key} = $msg->param('_action_params')->{$key};
		}
	}
	
	$self->log->debug('meta_info: ' . Dumper($meta_info));
	
	my $headers = {
		To => $msg->param('_user')->{email},
		From => $self->conf->get('email/display_address') ? $self->conf->get('email/display_address') : 'system',
		Subject => $self->conf->get('email/subject') ? $self->conf->get('email/subject') : 'system',
	};
	my $body = sprintf('%d results for query %s', $meta_info->{recordsReturned}, $meta_info->{query}->{query_params}) .
		"\r\n" . sprintf('%s/get_results?qid=%d&hash=%s', 
			$self->conf->get('email/base_url') ? $self->conf->get('email/base_url') : 'http://localhost',
			$meta_info->{qid},
			$meta_info->{hash},
	);
	
	my ($query, $sth);
	$query = 'SELECT UNIX_TIMESTAMP(last_alert) AS last_alert, alert_threshold FROM query_schedule WHERE id=?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($meta_info->{query_schedule_id});
	my $row = $sth->fetchrow_hashref;
	if ((time() - $row->{last_alert}) < $row->{alert_threshold}){
		$self->log->warn('Not alerting because last alert was at ' . (scalar localtime($row->{last_alert})) 
			. ' and threshold is at ' . $row->{alert_threshold} . ' seconds.' );
		return;
	}
	else {
		$query = 'UPDATE query_schedule SET last_alert=NOW() WHERE id=?';
		$sth = $heap->{dbh}->prepare($query);
		$sth->execute($meta_info->{query_schedule_id});
	}
	
	$kernel->yield('_send_email', $headers, $body, $msg->body());
	
	# Save these results asynchronously
	my $save_msg = POE::Event::Message->package({results => $msg->body()->{results}, qid => $meta_info->{qid} });
	$save_msg->param('_action_params', { comments => 'Scheduled Query ' . $meta_info->{query_schedule_id} });
	$save_msg->addRouteTo('post', undef, '_save_results');
	$save_msg->route();
	
	
	# Make sure this continues to go wherever it needs to
	$msg->route();
}

sub _batch_notify {
	my ( $self, $kernel, $session, $heap ) = @_[ OBJECT, KERNEL, SESSION, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	$self->log->debug('got results for batch: ' . Dumper($msg));
	
	# Route back if something is wrong
	if ($msg->status()){
		$self->log->error('Got msg status: ' . $msg->status() . ' from msg ' . Dumper($msg));
		$msg->routeBack();
		return;
	}
	unless (ref($msg->body()) eq 'HASH' 
		and $msg->body()->{results} 
		and ref($msg->body()->{results}) eq 'ARRAY'){
		my $errmsg = 'Did not get a valid msg body back from batched query';
		$self->log->error($errmsg);
		$msg->setErr(ERROR, $errmsg);
		$msg->route();
		return 0;
	}
	
	my $meta_info = { %{ $msg->body() } };
	if ($msg->param('_action_params') and ref($msg->param('_action_params')) eq 'HASH'){
		foreach my $key (keys %{ $msg->param('_action_params') }){
			$meta_info->{$key} = $msg->param('_action_params')->{$key};
		}
	}
	
	$self->log->debug('meta_info: ' . Dumper($meta_info));
	
	my $headers = {
		To => $msg->param('_user')->{email},
		From => $self->conf->get('email/display_address') ? $self->conf->get('email/display_address') : 'system',
		Subject => sprintf('ELSA batch query %d complete with %d results', $meta_info->{qid}, $meta_info->{recordsReturned}),
	};
	my $body = sprintf('%d results for query %s', $meta_info->{recordsReturned}, $meta_info->{query}->{query_params}) .
		"\r\n" . sprintf('%s/get_results?qid=%d&hash=%s', 
			$self->conf->get('email/base_url') ? $self->conf->get('email/base_url') : 'http://localhost',
			$meta_info->{qid},
			$meta_info->{hash},
	);
	
	$kernel->yield('_send_email', $headers, $body, $msg->body());
	
	# Save these results asynchronously
	my $save_msg = POE::Event::Message->package($meta_info);
	$save_msg->param('_action_params', { comments => 'Batch Query ' . $meta_info->{qid} });
	$save_msg->addRouteTo('post', undef, '_save_results');
	$save_msg->route();
	
	# Make sure this continues to go wherever it needs to
	$msg->route();
}

sub _get_host_info {
	my ( $self, $kernel, $session, $heap, $ip ) = @_[ OBJECT, KERNEL, SESSION, HEAP, ARG0 ];
	unless ($self->conf->get('inventory/dsn')){
		$self->log->error('No inventory db');
		return;
	}
	my $dbh = DBI->connect($self->conf->get('inventory/dsn'), 
		$self->conf->get('inventory/username'), 
		$self->conf->get('inventory/password'));
	unless ($dbh){
		$self->log->error('Invalid inventory db');
		return;
	}
	my ($query, $sth);
	$query = 'SELECT * FROM ' . $self->conf->get('inventory/table') 
		. ' WHERE ' . $self->conf->get('inventory/ip_column') .
		'=?';
	$self->log->debug('query: ' . $query);
	$sth = $dbh->prepare($query);
	$sth->execute($ip);
	return $sth->fetchrow_hashref;
}

sub _local_groupby {
	my $self = shift;
	my $results = shift;
	my $groupby = shift;
	
	my $grouped_results = [];
	my $values = {};
	
	# Loop through all results and extract the field by name.  Tedious, but we can't count on field order.
	foreach my $result (@$results){
		foreach my $field_hash (@{ $result->{_fields} }){
			if ($field_hash->{field} eq $groupby){
				$values->{ $field_hash->{value} } ||= 0;
				$values->{ $field_hash->{value} }++;
				last;
			}
		}
	}
	
	$self->log->debug('values: ' . Dumper($values));
	
	foreach my $value (sort { $values->{$b} <=> $values->{$a} } keys %$values){
		push @$grouped_results, { '@count' => $values->{$value}, '@groupby' => $value };
	}
	
	$self->log->debug('returning grouped results: ' . Dumper($grouped_results));
	
	return $grouped_results;
}

sub set_permissions_exception {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	
	unless ($args->{action} and ($args->{action} eq 'add' or $args->{action} eq 'delete')){
		$kernel->yield( '_error',
			'Invalid args, missing action: ' . Dumper($args), $msg );
		return;
	}
	
	eval { $args->{exception} = decode_json( $args->{exception} ); };
	$self->log->debug('args: ' . Dumper($args));
	if ($@) {
		$kernel->yield(
			'_error',
			'Error decoding permissions args: ' 
			  . $@ . ' : '
			  . Dumper($args),
			$msg 
		);
		return;
	}
	unless ( $args->{exception} and ref( $args->{exception} ) eq 'HASH' ) {
		$kernel->yield( '_error',
			'Invalid permissions args: ' . Dumper($args), $msg );
		return;
	}
	
	my ($query, $sth);
	
	# we need to massage inbound hostnames
	if ($args->{exception}->{attr} eq 'host_id'){
		if ($args->{exception}->{attr_id} =~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/){
			$args->{exception}->{attr_id} = unpack('N*', inet_aton($args->{exception}->{attr_id}));
		}
		elsif ($args->{exception}->{attr_id} =~ /(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s*\-\s*(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/){
			$args->{exception}->{attr_id} = unpack('N*', inet_aton($1)) . '-' . unpack('N*', inet_aton($2));
			$self->log->debug('attr_id: ' . $args->{exception}->{attr_id} . ', 1: ' . $1 . ', 2:' . $2);
		}
		else {
			$kernel->yield( '_error',
				'Invalid permissions args, bad host: ' . Dumper($args), $msg );
			return;
		}
	}
	
	if ($args->{action} eq 'add'){
		$query = 'INSERT INTO permissions (gid, attr, attr_id, allow) VALUES(?,?,?,?)';
		$sth = $heap->{dbh}->prepare($query);
		$sth->execute(
			$args->{exception}->{gid}, 
			$args->{exception}->{attr}, 
			$args->{exception}->{attr_id},
			$args->{exception}->{allow});
	}
	elsif ($args->{action} eq 'delete') {
		$query = 'DELETE FROM permissions WHERE gid=? AND attr_id=?';
		$sth = $heap->{dbh}->prepare($query);
		$sth->execute(
			$args->{exception}->{gid}, 
			$args->{exception}->{attr_id});
	}
	my $ret;
	if ($sth->rows){
		$ret = { success => $sth->rows };
		$kernel->yield('_revalidate_group', $args->{exception}->{gid});		
	}
	else {
		$ret = { error => 'Database was not altered with args ' . Dumper($args) };
	}
	$msg->body($ret);
	$msg->route();
}

sub get_running_archive_query {
	my ( $self, $kernel, $heap ) = @_[ OBJECT, KERNEL, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	
	my ($query, $sth);
	$query = 'SELECT qid, query FROM query_log WHERE uid=? AND archive=1 AND ISNULL(num_results)';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($msg->param('_user')->{uid});
	my $row = $sth->fetchrow_hashref;
	if ($row){
		my $query_params = decode_json($row->{query});
		$msg->body({ qid => $row->{qid}, query => $query_params->{query_params} });
	}
	else {
		$msg->body({qid => 0});
	}
	
	$msg->route();
}

1;

__END__

