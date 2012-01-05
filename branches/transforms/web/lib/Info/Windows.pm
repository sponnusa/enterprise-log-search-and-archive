package Info::Windows;
use Moose;
use Data::Dumper;
extends 'Info';
has 'eventid' => (is => 'rw', isa => 'Int', required => 1);

sub BUILDARGS {
	my ($class, %args) = @_;
	
	$args{eventid} = $args{data}->{eventid};
	
	return \%args;
}

sub BUILD {
	my $self = shift;
	if ($self->conf->get('info/windows/url_templates')){
		foreach my $template (@{ $self->conf->get('info/windows/url_templates') }){
			push @{ $self->urls }, sprintf($template, $self->eventid);
		}
	}
}

1;