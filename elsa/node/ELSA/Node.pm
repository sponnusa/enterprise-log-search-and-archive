package ELSA::Node;
use strict;
use warnings;
use Data::Dumper;
use Time::HiRes qw(sleep time);
use Storable qw(freeze);
$Storable::canonical = 1;
sub POE::Kernel::ASSERT_DEFAULT () { 1 }
sub POE::Kernel::TRACE_DEFAULT () { 1 }
#sub POE::Kernel::TRACE_EVENTS  () { 0 }
use POE qw(Wheel::Run Filter::Reference Component::PreforkDispatch );
use POSIX;
use Devel::StackTrace;

use ELSA;
use IO::Socket;
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
use ELSA::Exceptions;
use ELSA::Indexer;

our @ISA = qw( Exporter ELSA );
our %Inline_states = ( );
our @Object_states = qw( 
	_start
	handle_exception 
	_stop
	_child
	_default
	dispatch
);

sub new {
	my $class = shift;
	my $self = $class->SUPER::new(@_);
	return bless($self, $class);
}

sub _start {
	my ($self,$kernel,$heap,$session,$alias,$published_states) = @_[OBJECT,KERNEL,HEAP,SESSION,ARG0,ARG1];
	
	$self->log->debug("Setting alias $alias with published_states " . Dumper($published_states));
	$kernel->alias_set($alias);
	$heap->{alias} = $alias;
	$kernel->sig('DIE' => 'handle_exception');
	$self->{_JOBS} = {};
	
	POE::Component::PreforkDispatch->create(
		max_forks => $self->conf->get('num_workers/' . $alias),
		pre_fork  => $self->conf->get('num_workers/' . $alias),
		alias => 'PreforkDispatch_' . $alias,
		upon_result => sub {
			my ($request, $response) = @_;
#			$self->log->trace('Finished forked request: ' . Dumper($request) . ' response: ' . Dumper($response));
			my $msg = $request->{params}->[0];
			if ($msg->getMode() eq 'call'){
				$self->log->debug('routing reponse to: ' . Dumper($msg->header()));
				my $msg_response = new POE::Event::Message( $msg, $response);
				if (ref($response) eq 'HASH' and $response->{error}){
					$msg_response->setErr(ELSA::ERROR, $response->{error});
				}
				$msg_response->route(); # should go to sync_respond for socket comm
			}
			$self->log->debug('Job ' . $request->{method} . ' finished in ' 
				. $request->{elapsed} . ' seconds');
			delete $self->{_JOBS}->{ $request->{method} };
			$self->log->debug('Jobs: ' . Dumper($self->{_JOBS}));
		},
		talkback => sub {
			$self->log->debug(shift);
		},
		verbose => 1,
		methods => {},
	);
}

sub dispatch {
	my ($self,$kernel,$heap,$method,$msg) = @_[OBJECT,KERNEL,HEAP,ARG0..ARG1];
#	$self->log->debug('Executing with msg: ' . Dumper($msg) .
#		", method: $method");
	
	$self->log->trace('dispatch');
	$self->{_JOBS}->{$method} = $$;
	if ($method and $msg){
		$kernel->post('PreforkDispatch_' . $heap->{alias}, 'new_request', {
			method => $method,
			params => [ $msg ],
		});
	}
	else {
		$self->log->error("Invalid args: method: $method, msg:" . Dumper($msg));
	}
}

sub _stop {
	my ($self, $kernel, $heap) = @_[OBJECT,KERNEL,HEAP];
	$self->log->info("Shutting down...");
	# Make sure none of them restart
	$heap->{restart} = {};
	foreach my $pid (keys %{$heap->{wheels_by_pid}}){
		$self->log->debug("Sending TERM to pid $pid");
		kill SIGTERM, $pid;
	}
}

sub handle_exception {
	my ($self, $kernel, $heap, $signal, $err) = @_[OBJECT,KERNEL,HEAP,ARG0..ARG1];
	# $err = { source_session => POE::Session, error_str => scalar, file => scalar, 
	#	from_state => scalar, event => scalar, dest_session => POE::Session}
	#warn 'got exception: ' . Dumper($err);
	$self->log->error('Stacktrace: ' . Devel::StackTrace->new->as_string());
	if ($err and ref($err)){
		$self->log->error("Got exception: " . $err->{error_str});
		return { error => $err->{error_str} };
	}
	else {
		$self->log->error("Got exception: $err");
		return { error => $err };
	}
}

sub _default {
	my ($self,$kernel, $heap, $session, $event, $args) = @_[OBJECT,KERNEL,HEAP,SESSION, ARG0, ARG1];
    $self->log->warn(
      "Session " . $session->ID . ' ' . $kernel->alias_list($session->ID) .
      " caught unhandled event $event with (@$args).\n"
    );
    if ($args->[1] and ref($args->[1]) and $args->[1]->can('route') ){
    	$args->[1]->setErr(ELSA::ERROR, 'No such event ' . $event);
    	$args->[1]->route();
    }
    return { error => 'No such event ' . $event };
}

sub _child {
	my ($self,$kernel, $heap,$session, $reason, $child, $from_start) = 
		@_[OBJECT,KERNEL,HEAP,SESSION, ARG0, ARG1, ARG2 ];
	if ($reason eq 'create'){
    	$self->log->debug("action: $reason, session: " . $child->ID() . ", from start: $from_start");
	}
	else {
		$self->log->debug("action: $reason, session: " . $child->ID());
	}
}

1;