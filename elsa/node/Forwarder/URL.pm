package Forwarder::URL;
use Moose;
use Data::Dumper;
use LWP::UserAgent;
use HTTP::Request::Common;
extends 'Forwarder';

has 'url' => (is => 'rw', isa => 'Str', required => 1);
has 'ua' => (is => 'rw', isa => 'LWP::UserAgent', required => 1);

sub BUILDARGS {
	my ($class, %params) = @_;
	
	$params{ua} = new LWP::UserAgent(agent => 'ELSA Log Relay/0.1', timeout => 10);
	if (exists $params{ca_file}){
		$params{ua}->ssl_opts(
			SSL_ca_file => delete $params{ca_file},
			SSL_cert_file => delete $params{cert_file},
			SSL_key_file => delete $params{key_file},
		);
	}
	
	return \%params;
}

sub forward {
	my $self = shift;
	
	foreach (@_){
		my $file = $_;
		my $req = HTTP::Request::Common::POST($self->url,
			[
				'filename' => [ $file ]
			],
			'Content_Type' => 'form-data');
		
		my $res = $self->ua->request($req);
		if ($res->is_success){
			my $ret = $res->content();
			$self->log->debug('got ret: ' . Dumper($ret));
		}
		else {
			$self->log->error('Failed to upload logs via url ' . $self->url . ': ' . $res->status_line);
		}
	}
	
	return 1;					
}

__PACKAGE__->meta->make_immutable;

1;