package Info;
use Moose;
# Base class for Info plugins
has 'conf' => (is => 'rw', isa => 'Object', required => 1);
has 'data' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'urls' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });
has 'plugins' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });
has 'summary' => (is => 'rw', isa => 'Str', required => 1, default => '');

sub BUILD {
	my $self = shift;
	if ($self->conf->get('pcap_url')){
		push @{ $self->plugins }, 'getPcap';
	}
	if ($self->conf->get('block_url')){
		push @{ $self->plugins }, 'blockIp';
	}
	return $self;
}

1;