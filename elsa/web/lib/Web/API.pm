package Web::API;
use Moose;
extends 'Web';
use Data::Dumper;
use Plack::Request;
use Encode;
use Scalar::Util;
use Digest::SHA qw(sha512_hex);

sub call {
	my ($self, $env) = @_;
    my $req = Plack::Request->new($env);
	my $res = $req->new_response(200); # new Plack::Response
	$res->content_type('text/plain');
	$res->header('Access-Control-Allow-Origin' => '*');
	
	$self->api->clear_warnings;
	
	my $method = $self->_extract_method($req->request_uri);
	$self->api->log->debug('method: ' . $method);
	
	# Make sure private methods can't be run from the web
	if ($method =~ /^\_/){
		$res->status(404);
		$res->body('not found');
		return $res->finalize();
	}
	
	my $args = $req->parameters->as_hashref;
	$args->{from_peer} = $req->address;
	$self->api->log->debug('args: ' . Dumper($args));
	
	# Authenticate via apikey
	unless ($self->api->_check_auth_header($req)){
		$res->status(401);
		$res->body('unauthorized');
		$res->header('WWW-Authenticate', 'ApiKey');
		return $res->finalize();
	}
	
	unless ($self->api->can($method)){
		$res->status(404);
		$res->body('not found');
		return $res->finalize();
	}
	my $ret;
	eval {
		$self->api->freshen_db;
		if ($req->upload and $req->uploads->{filename}){
			$args->{upload} = $req->uploads->{filename};
		}
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
	elsif (ref($ret) and ref($ret) eq 'ARRAY'){
		# API function returned Plack-compatible response
		return $ret;
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

__PACKAGE__->meta->make_immutable;
1;