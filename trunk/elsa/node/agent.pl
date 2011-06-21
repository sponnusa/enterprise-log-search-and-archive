#!/usr/bin/perl
use strict;
use Data::Dumper;
use FindBin;
use Getopt::Std;
use Net::Server::Daemonize qw(daemonize);
#sub POE::Kernel::TRACE_EVENTS { 1 }
#sub POE::Kernel::TRACE_SIGNALS { 1 }
use EV;
use POE qw( Component::Server::TCP );
use POE::API::Peek;
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

# Include the directory this script is in
use lib $FindBin::Bin;

use ELSA;
use ELSA::Node::Manager;
use ELSA::Node::Web;
use ELSA::Exceptions;
use ELSA::Indexer;

my %opts;
getopt('Dc:', \%opts);
my $config_file = -f '/etc/elsa.conf' ? '/etc/elsa.conf' : '/usr/local/elsa/etc/elsa.conf';
if ($opts{c}){
	$config_file = $opts{c};
}
die("No such file $config_file") unless -f $config_file;

my $api;

eval {
	
	my %inline_states = (
		_child => sub { 1 },
		_stop => sub { 1 },
	);
	
	my $config = new Config::JSON($config_file);
	
	unless (exists $opts{D}){
		print "Daemonizing...\n";
		daemonize($config->get('manager/user'), $config->get('manager/group'), $config->get('manager/pidfile'));
		#TODO Disable printing to screen so STDOUT/STDERR isn't cluttered
	}
	
	open(FH, "> /tmp/parent_pid");
	print FH $$;
	close(FH);
	
	# Create the manager
	my $manager = new ELSA::Node::Manager($config_file);
	
	$api = POE::API::Peek->new();
	
	POE::Component::Server::TCP->new(
		Alias => "socket",
		Port  => $manager->conf->get('manager/listen_port'),
		ClientInput => sub {
			my $msg = $_[ARG0];
#			$manager->log->debug('ClientInput got msg: ' . Dumper($msg));
#			$manager->log->debug('Session_id: ' . $_[SESSION]->ID());
			unless ($msg and ref($msg) and $msg->can('header')){
				$manager->log->error('Invalid msg: ' . Dumper($msg));
				$_[HEAP]->{client}->put({error => 'Invalid msg'});
				return;
			} 
		
			if ($msg->getMode() eq 'call'){
				$msg->addRouteBack('post', undef, 'sync_respond' );
				#$manager->log->trace('ClientInput sync routing msg: ' . Dumper($msg));
			}
			$msg->route();
		},
		ClientFilter => $POE::Event::Message::Filter,
		InlineStates => {
			sync_respond => sub {
				if ($_[ARG1] and ref($_[ARG1]) eq 'ARRAY' and $_[ARG1]->[0]){
					my $response = $_[ARG1]->[0];
					#$manager->log->trace('sync_response: ' . Dumper($response));
					$_[HEAP]->{client}->put($response);
				}
				else {
					my $response = POE::Event::Message->new(undef, undef);
					$response->setErr(-1, 'Invalid args for response received');
					$_[HEAP]->{client}->put($response);
				}
			}
		}
	);
	
	# Create the manager session
	POE::Session->create(
		options => { trace => 1, debug => 1, default => 1},
		inline_states => { %inline_states },
		object_states => [
			$manager => [ 
				@ELSA::Node::Manager::Object_states,
			],
		],
		heap => { 
			tasks => {},
			wheels => {},
			rsvps => {},
		},
		args => [ 'manager', \@ELSA::Node::Manager::Published_states ],
	);
	
	# Create the web listener
	my $web = new ELSA::Node::Web($config_file);
	POE::Session->create(
		options => { trace => 1, debug => 1, default => 1},
		inline_states => { %ELSA::Node::Web::Inline_states },
		object_states => [
			$web => [ 
				@ELSA::Node::Web::Object_states,	
			 ],
		],
		args => [ 'web' ],
	);
	
	POE::Session->create(
		options => { trace => 1, debug => 1, default => 1 },
		heap => { query_results => {}, query_sessions => {} },
		inline_states => { 
			_start => sub {
				$_[KERNEL]->alias_set('agent');
				$_[KERNEL]->sig('DIE', '_handle_exception');
				$_[HEAP]->{manager} = $manager;
			},
			_stop => sub { 1 },
			_handle_exception => \&_handle_exception,
			execute => \&execute,
		},
	);
	
	# Do initial directory validation
	if ($manager->conf->get('validate_directory')){
		$manager->log->info("Validating directory entries...");
		my $indexer = new ELSA::Indexer($manager->{_CONFIG_FILE});
		$indexer->initial_validate_directory();
		$indexer->db->disconnect();
	}

	POE::Kernel->run();
        
};
if ($@){
		print $@ . "\n";
        ELSA::log_error($@);
}

sub _handle_exception {
	my ($kernel, $heap, $sig, $ex) = @_[KERNEL,HEAP,ARG0..ARG1];
	$kernel->sig_handled();
					
	my $e = new ELSA::Exception(
		error => sprintf('%s from event %s at file %s line %d', 
			$ex->{error_str}, $ex->{event}, $ex->{file}, $ex->{line})
	);
	
	if ($e and ref($e) and $e->can('as_string')){
		$heap->{manager}->log->error("Got generic exception: " . $e->as_string());
	}
	else {
		$heap->{manager}->log->error("Got generic exception: " . Dumper($e));
	}
	
	if( $ex->{source_session} ne $_[SESSION] ) {
		$kernel->signal( $ex->{source_session}, 'DIE', $ex );
	}
}

sub execute {
	my ($kernel,$heap,$postback_args,$sent_args) = @_[KERNEL,HEAP,ARG0..ARG1];
	my ($method_session, $method) = @$postback_args;
	my ($msg) = @$sent_args;	
	$heap->{manager}->log->debug('Executing with postback_args: ' . Dumper($postback_args) . ' msg: ' . Dumper($msg) .
		", method_session: $method_session, method: $method");
	
	my $active_event = $api->active_event();
	$heap->{manager}->log->trace('active event: ' . $active_event);
		
	foreach my $event ($api->event_queue_dump()){
		my $str = 'Event: ';
		foreach my $item qw(index priority event type){
			$str .= ' ' . $item . ': ' . $event->{$item};
		}
		foreach my $item qw(source destination){
			$str .= ' ' . $item . ': ' . join(', ', $api->session_alias_list($event->{$item}));
		}
		$heap->{manager}->log->trace($str);
	}
	
	# We have to post to PreforkDispatch's new_request method from within the same session our method is
	#  so we have a wrapper called dispatch in each parent session which does this for us.
	$kernel->post($method_session, 'dispatch', $method, $msg);
}

__END__
