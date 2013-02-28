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
	
	if (exists $params{verify_mode} and not $params{verify_mode}){
		local $ENV{PERL_LWP_SSL_VERIFY_HOSTNAME} = 0;
	}
	
	$params{ua} = new LWP::UserAgent(agent => 'ELSA Log Relay/0.1', timeout => 10);
	my %ssl_opts;
	foreach (qw(ca_file cert_file key_file verify_mode)){
		if (exists $params{$_}){
			$ssl_opts{'SSL_' . $_} = $params{$_};
		}
	}
	if (keys %ssl_opts){
		$params{ua}->ssl_opts(%ssl_opts);
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