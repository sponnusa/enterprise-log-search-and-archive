package Connector::SIRT;
use Moose;
use Data::Dumper;
use AnyEvent::HTTP;
use MIME::Base64;
extends 'Connector';

sub BUILD {
	my $self = shift;
	
	$self->api->log->trace('posting to url ' . $self->api->conf->get('connectors/sirt/url'));
	my $post_str = 'data=' . encode_base64($self->api->json->encode( $self->data ));
	$post_str .= '&username=' . $self->user_info->{username};
	#$self->api->log->trace('post_str: ' . $post_str);
	my $cv = AnyEvent->condvar;
	http_post(
		$self->api->conf->get('connectors/sirt/url'), 
		$post_str, 
		headers => { 'Content-type' => 'application/x-www-form-urlencoded' }, 
		sub {
			my ($body, $hdr) = @_;
			$self->api->log->trace('body: ' . Dumper($body));
			$cv->send;
		}
	);
	$cv->recv;
	return 1;
}


1