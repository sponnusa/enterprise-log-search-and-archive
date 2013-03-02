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
		$ENV{PERL_LWP_SSL_VERIFY_HOSTNAME} = 0;
	}
	else {
		$ENV{PERL_LWP_SSL_VERIFY_HOSTNAME} = 1;
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
	my $args = shift;
	
	my $req = HTTP::Request::Common::POST($self->url,
		[
			md5 => $args->{md5},
			count => $args->{batch_counter},
			size => $args->{file_size},
			start => $args->{start},
			end => $args->{end},
			compressed => $args->{compressed} ? 1 : 0,
			batch_time => $args->{batch_time},
			total_errors => $args->{total_errors},
			filename => [ $args->{file} ]
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
	
	return 1;					
}

__PACKAGE__->meta->make_immutable;

1;