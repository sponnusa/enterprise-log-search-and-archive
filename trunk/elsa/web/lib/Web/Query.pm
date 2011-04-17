package Web::Query;
use Moose;
use base 'Web';
use Data::Dumper;
use Plack::Request;
use Plack::Session;

sub call {
	my ($self, $env) = @_;
    $self->session(Plack::Session->new($env));
	my $req = Plack::Request->new($env);
	$self->{_USERNAME} = $req->user ? $req->user : undef;
	my $res = $req->new_response(200); # new Plack::Response
	$res->content_type('text/plain');
	$res->header('Access-Control-Allow-Origin' => '*');
	
	my $method = $self->_extract_method($req->request_uri);
	$self->log->debug('method: ' . $method);
	my $body = $self->rpc($method, $req->query_parameters->as_hashref);
	$res->body($self->json->encode($body));
	$res->finalize();
}

1;