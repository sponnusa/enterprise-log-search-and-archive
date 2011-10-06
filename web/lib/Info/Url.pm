package Info::Url;
use strict;
use Data::Dumper;
use base qw( Info );

sub new {
	my $class = shift;
	my $self = $class->SUPER::new(@_);
	die('No site given') unless $self->data and $self->data->{site};
	bless($self, $class);

	my $urls = [ 'http://whois.domaintools.com/' . $self->{data}->{site} ];
	$self->summary('No summary for URL');
	
	$self->urls($urls);
	
	$self->plugins([qw(getPcap)]);
	
	return $self;
}

1;