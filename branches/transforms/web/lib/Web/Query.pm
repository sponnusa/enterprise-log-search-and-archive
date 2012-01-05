package Web::Query;
use Moose;
extends 'Web';
use Data::Dumper;
use Plack::Request;
use Plack::Session;
use Encode;

sub call {
	my ($self, $env) = @_;
    $self->session(Plack::Session->new($env));
	my $req = Plack::Request->new($env);
	$self->{_USERNAME} = $req->user ? $req->user : undef;
	my $res = $req->new_response(200); # new Plack::Response
	$res->content_type('text/plain');
	$res->header('Access-Control-Allow-Origin' => '*');
	
	my $method = $self->_extract_method($req->request_uri);
	$self->api->log->debug('method: ' . $method);
	my $args = $req->parameters->as_hashref;
	$args->{user_info} = $self->session->get('user_info');
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
	elsif (ref($ret) and $ret->{mime_type}){
		$res->content_type($ret->{mime_type});
		$res->body($ret->{ret});
		if ($ret->{filename}){
			$res->header('Content-disposition', 'attachment; filename=' . $ret->{filename});
		}
	}
	else {
		$res->body([encode_utf8($self->api->json->encode($ret))]);
	}
	$res->finalize();
}

1;