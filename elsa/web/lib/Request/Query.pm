package Request::Query;
use strict;
use warnings;
use Data::Dumper;
use base qw( Request );
use JSON -convert_blessed_universally;
use Apache2::Request;
use CGI::Application::Plugin::Apache;
use Apache2::Const qw(:common :http);
use APR::Request::Param;

use lib qw(../);
use Janus;

sub run {
	my $self = shift;
	my $r = $self->init_request();
	
	die("No session id") unless ( $self->session->id );

	my $method = 'error';
	$method = $self->param('method') if $self->param('method');
	
	# Do the query
	my $ret = $self->rpc($method, $self->{_QUERY_PARAMS});
	unless ($ret){
		$self->header_add( -type => 'application/javascript' );
		print $self->json->encode({error => 'Unknown error'});
		return HTTP_NOT_FOUND;
	}
	
	if (ref($ret) eq 'HASH' and $ret->{error}){
		$self->header_add( -type => 'application/javascript' );
		print $self->json->encode({error => $ret->{error}});
		return HTTP_NOT_FOUND;
	}
	elsif ( ref($ret) eq 'HASH' and $ret->{mime_type} and $ret->{ret} ) {
		my %header_options = (-type => $ret->{mime_type}, -expires => 'now');
		if ($ret->{filename}){
			$header_options{-attachment} = $ret->{filename};
		}
		$self->header_add( %header_options );
		if ( $ret->{mime_type} eq 'application/javascript' ) {
			print $self->json->encode( $ret->{ret} );
		}
		else {
			print $ret->{ret};
		}
		return HTTP_OK;
	}
	elsif ($ret) {
		$self->header_add( -type => 'application/javascript' );
		print $self->json->encode($ret);
		return HTTP_OK;
	}
	else {
		$self->header_add( -type => 'application/javascript' );
		print $self->json->encode( { error => 'No value returned.' } );
		return HTTP_INTERNAL_SERVER_ERROR;
	}
}


1;

__END__

