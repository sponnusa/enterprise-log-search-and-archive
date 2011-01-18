package Request;
use strict;
use warnings;
use Data::Dumper;
use JSON -convert_blessed_universally;

use Apache2::Request;
use Apache2::Const qw(:http);
use APR::Request::Param;
use Apache2::Const -compile => qw(:satisfy);

use Exporter;
our @ISA = qw(Exporter);
our @EXPORT = qw(init_request conf);

use CGI::Application::Plugin::Session;
use CGI::Application::Plugin::Apache2::Request;
use CGI::Session::Driver::file;
use CGI qw(header);
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

use YUI;
use Janus;

sub new {
	my $class = shift;
	my $self = { _ERROR => '' };
	return bless $self, $class;
}

sub conf {
	my $self = shift;
	return $self->{_CONF};
}

sub log {
	my $self = shift;
	return $self->{_LOGGER};
}

sub json {
	my $self = shift;
	return $self->{_JSON};
}

sub init_request {
	my $self = shift;
	my $no_auth = shift;
	my $r = Apache2::Request->new( $self->param('r') );
	#TODO don't set this per-request
	$self->{_CONF} ||= new Config::JSON( $r->dir_config('LOGZILLA_CONFIG_FILE') )
		or die("Could not open config file " . $r->dir_config('LOGZILLA_CONFIG_FILE') );
	$YUI::Yui_version = $self->conf->get('yui/version');
	$YUI::Yui_modifier = $self->conf->get('yui/modifier');
	  	  
	# The logger obj should only get instantiated at the first run, it should persist thereafter	
	Log::Log4perl::init_once( $self->conf->get('log4perl.conf') ) or die("Unable to init logger");
	$self->{_LOGGER} ||= Log::Log4perl::get_logger("Apache") or die("Unable to init logger");
	$self->log->level( $self->conf->get('debug_level') );
	
	#$self->log->debug('self->query: ' . Dumper($self->query()));
	my @raw_params = $self->query()->param();
	$self->{_QUERY_PARAMS} = {};
	$self->log->debug('tied_params:' . Dumper(\@raw_params));
	foreach my $param (@raw_params){
		$self->log->debug("key: $param, length of val: " . length($self->query()->param($param)) . ", val: " . $self->query()->param($param));
		$self->{_QUERY_PARAMS}->{$param} = $self->query()->param($param);
	}
	
	unless ($self->{_JSON}){
		if ($self->conf->get('debug_level') eq 'DEBUG'){
			$self->{_JSON} = JSON->new->pretty->allow_nonref->allow_blessed;	
		}
		else {
			$self->{_JSON} = JSON->new->allow_nonref->allow_blessed;
		}
	}
	
	$self->log->debug('some auth required; ' . $r->some_auth_required());
	$self->log->debug('auth name: ' . $r->auth_name());
	$self->log->debug('r: ' . $r->as_string());
	$self->log->debug('remote user: ' . $r->get_remote_logname());
	$self->log->debug('requires: ' . Dumper($r->requires));
	$self->log->debug('satisfies: ' . $r->satisfies());
	if ($no_auth){
		$self->log->debug('auth_type: ' . $r->auth_type());
		$r->auth_type('None');
		$self->log->debug('dir_config: ' . Dumper($r->dir_config()));
		$r->dir_config('AuthType', 'None');
		$self->log->debug('dir_config: ' . Dumper($r->dir_config()));
	}
	else {
		# Add on our auth data
		$self->log->trace("session: " . Dumper( $self->session ) );
		unless ($self->session->param('user_info')){
			my $ret = $self->_get_user_info($r);
			unless ($ret){
				$self->query->header(-status => HTTP_UNAUTHORIZED);
				return 'Unauthorized';
			}
			$self->session->param('user_info', $ret);	
		}
		print $self->session->header();
	}
	
	print header(-expires => 'now');
	
	return $r;
}

sub rpc {
	my $self = shift;
	my $method = shift;
	my $params = shift;
	
	#$self->log->debug('params: ' . Dumper($params));
	my $timeout = $self->conf->get('Janus/timeout');
	if ($params and ref($params) eq 'HASH' and defined $params->{timeout}){
		$timeout = sprintf('%d', $params->{timeout});
		$self->log->debug('Set timeout ' . $timeout);
	}
	
	my $msg = POE::Event::Message->package($params);
	$msg->param('_user', $self->session->param('user_info'));
	$msg->addRouteTo('post',  $self->conf->get('Janus/session'), $method);
	$msg->addRemoteRouteTo($self->conf->get('Janus/server'), $self->conf->get('Janus/port'), 'sync');
	$msg->setMode('call');
	$self->log->debug('routing: ' . Dumper($msg));
	my $ret;
	eval {
		local $SIG{ALRM} = sub { die 'alarm'; };
		alarm $timeout;
		($ret) = $msg->route();
		alarm 0;
	};
	if ($@){
		my $errmsg = 'Janus connection timed out after ' . $timeout . ' seconds';
		$self->{_ERROR} = $errmsg;
		$self->log->error($self->{_ERROR});
		return 0;
	}
	
	$self->log->debug( "got ServerInput: " . Dumper($ret) );
	
	if ($ret and ref($ret) eq 'POE::Event::Message' and $ret->can('status')){	
		my ($status, $errmsg) = $ret->status();
		if ($status == Janus::ERROR){
			$self->{_ERROR} = $errmsg;
			$self->log->error($self->{_ERROR});
			return 0;
		}
		elsif ($status == Janus::REVALIDATE){
			# client needs to revalidate
			my $r = Apache2::Request->new( $self->param('r') );
			$self->log->warn('Revalidating user : ' . Dumper($r->user()));
			my $info = $self->_get_user_info($r);
			unless ($info){
				$self->{_ERROR} = 'Error during client revalidation';
				$self->log->error($self->{_ERROR});
				return 0;
			}
			$self->session->param('user_info', $info);
			# retry
			$ret = $self->rpc($method, $params);
			unless ($ret){
				$self->log->error('recursive failure during query, method: ' . $method . ', params: ' . Dumper($params) . ', ret: ' . Dumper($ret));
				$self->{_ERROR} = 'recursive failure during query';
				return 0;
			}
		}
		else {
			$ret = $ret->body();
		}
		return $ret;
	}
	else {
		$self->{_ERROR} = 'No value returned.';
		return 0;
	}
	
}

sub _get_user_info {
	my $self = shift;
	my $r = shift;
	unless ($r){
		$self->log->error('Did not receive r');
		return 0;
	}
	my $ret = $self->rpc('get_user_info', $r->user());
	if ($ret and ref($ret) eq 'HASH' and $ret->{permissions}){
		return $ret;
	}
	else {
		$self->log->error('Unable to get user info, got: ' . Dumper($ret));
		return 0;
	}
}


1