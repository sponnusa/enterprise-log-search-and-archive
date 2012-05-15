package Web::Query;
use Moose;
extends 'Web';
use Data::Dumper;
use Plack::Request;
use Plack::Session;
use Encode;
use Scalar::Util;

sub call {
	my ($self, $env) = @_;
    $self->session(Plack::Session->new($env));
	my $req = Plack::Request->new($env);
	my $res = $req->new_response(200); # new Plack::Response
	$res->content_type('text/plain');
	$res->header('Access-Control-Allow-Origin' => '*');
	
	$self->api->clear_warnings;
	
	my $method = $self->_extract_method($req->request_uri);
	$self->api->log->debug('method: ' . $method);
	my $args = $req->parameters->as_hashref;
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
	my $ret;
	eval {
		$self->api->freshen_db;
		$ret = $self->api->$method($args);
		unless ($ret){
			$ret = { error => $self->api->last_error };
		}
	};
	if ($@){
		my $e = $@;
		$self->api->log->error($e);
		$res->body([encode_utf8($self->api->json->encode({error => $e}))]);
	}
	elsif (ref($ret) and ref($ret) eq 'HASH' and $ret->{mime_type}){
		$res->content_type($ret->{mime_type});
		if (ref($ret->{ret}) and ref($ret->{ret}) eq 'HASH'){
			if ($self->api->has_warnings){
				$ret->{ret}->{warnings} = $self->api->warnings;
			}
		}
		elsif (ref($ret->{ret}) and blessed($ret->{ret}) and $ret->{ret}->can('add_warning')){
			$ret->warnings($self->api->warnings);
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
		elsif (ref($ret) and blessed($ret) and $ret->can('add_warning')){
			$ret->warnings($self->api->warnings);
		}
		$res->body([encode_utf8($self->api->json->encode($ret))]);
	}
	$res->finalize();
}

1;