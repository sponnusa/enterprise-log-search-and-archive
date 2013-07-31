package Web::Query;
use Moose;
extends 'Web';
use Data::Dumper;
use Plack::Request;
use Plack::Session;
use Encode;
use Scalar::Util;
use Try::Tiny;
use Ouch qw(:trytiny);;

use Utils;

sub call {
	my ($self, $env) = @_;
    $self->session(Plack::Session->new($env));
	my $req = Plack::Request->new($env);
	my $res = $req->new_response(200); # new Plack::Response
	$res->content_type('text/plain');
	$res->header('Access-Control-Allow-Origin' => '*');
	
	$self->api->clear_warnings;
	
	Log::Log4perl::MDC->put('client_ip_address', $req->address);
	
	my $method = $self->_extract_method($req->request_uri);
	$self->api->log->debug('method: ' . $method);
	
	# Make sure private methods can't be run from the web
	if ($method =~ /^\_/){
		$res->status(404);
		$res->body('not found');
		return $res->finalize();
	}
	
	my $args = $req->parameters->as_hashref;
	$self->api->log->debug('args: ' . Dumper($args));
	if ($self->session->get('user')){
		$args->{user} = $self->api->get_stored_user($self->session->get('user'));
	}
	else {
		$args->{user} = $self->api->get_user($req->user);
	}
	unless ($self->api->can($method)){
		$res->status(404);
		$res->body('not found');
		return $res->finalize();
	}
	my ($ret, $e);
	try {
		$self->api->freshen_db;
		$ret = $self->api->$method($args);
		unless ($ret){
			$ret = { error => $self->api->last_error };
		}
	}
	catch {
		$e = catch_any(shift);
	};
#	if ($@){
#		my $e = $@;
#		$self->api->log->error($e);
#		$res->body([encode_utf8($self->api->json->encode({error => $e}))]);
#	}
	if ($e){
		$self->api->log->error($e->trace);
		$res->status($e->code);
		$res->body([encode_utf8($self->api->json->encode($e))]);
	}
	elsif (ref($ret) and ref($ret) eq 'HASH' and $ret->{mime_type}){
		$res->content_type($ret->{mime_type});
		if (ref($ret->{ret}) and ref($ret->{ret}) eq 'HASH'){
			if ($self->api->has_warnings){
				$ret->{ret}->{warnings} = $self->api->warnings;
			}
		}
		elsif (ref($ret->{ret}) and blessed($ret->{ret}) and $ret->{ret}->can('add_warning') and $self->api->has_warnings){
			foreach my $warning ($self->api->all_warnings){
				push @{ $ret->{ret}->warnings }, $warning;
			}
		}
		$res->body($ret->{ret});
		if ($ret->{filename}){
			$res->header('Content-disposition', 'attachment; filename=' . $ret->{filename});
		}
	}
	else {
		if (ref($ret) and ref($ret) eq 'HASH'){
			if ($self->api->has_warnings){
				$ret->{warnings} = $self->api->warnings;
			}
		}
		elsif (ref($ret) and blessed($ret) and $ret->can('add_warning') and $self->api->has_warnings){
			foreach my $warning ($self->api->all_warnings){
				push @{ $ret->warnings }, $warning;
			}
		}
		$res->body([encode_utf8($self->api->json->encode($ret))]);
	}
	$res->finalize();
}

1;