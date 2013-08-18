package Info::Url;
use Moose;
use Data::Dumper;
extends 'Info';

sub BUILD {
	my $self = shift;
	if ($self->conf->get('info/url/url_templates')){
		foreach my $template (@{ $self->conf->get('info/url/url_templates') } ){
			push @{ $self->urls }, sprintf($template, $self->data->{site});
		}
	}
}

1;