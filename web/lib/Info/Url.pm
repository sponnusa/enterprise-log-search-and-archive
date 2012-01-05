package Info::Url;
use Moose;
use Data::Dumper;
extends 'Info';
has 'plugins' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [qw(getPcap)] });

sub BUILD {
	my $self = shift;
	if ($self->conf->get('info/url/url_templates')){
		foreach my $template (@{ $self->conf->get('info/url/url_templates') } ){
			push @{ $self->urls }, sprintf($template, $self->data->{site});
		}
	}
}

1;