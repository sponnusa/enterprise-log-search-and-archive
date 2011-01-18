package Info::Windows;
use strict;
use Data::Dumper;
use base qw( Info );

sub new {
	my $class = shift;
	my $self = $class->SUPER::new(@_);
	die('No eventid given') unless $self->data and $self->data->{eventid};
	bless($self, $class);

	my $description = $self->_get_eventid_desc();
	$self->{_DESCRIPTION} = $description;
	$self->summary($description);
	
	# Get URL's from config
	my $urls = [];
	if ($self->conf->get('info/windows/link_templates')){
		foreach my $template (@{ $self->conf->get('info/windows/link_templates') }){
			push @$urls, sprintf($template, $self->data->{eventid});
		}
	}
	
	$self->urls($urls);
	
	$self->plugins([]);
	
	return $self;
}

sub _get_eventid_desc {
	my $self = shift;
		
	die('Invalid eventid csv file given: ' . $self->conf->get('info/windows/eventids.csv')) 
		unless -f $self->conf->get('info/windows/eventids.csv');
	open(FH, $self->conf->get('info/windows/eventids.csv'));
	while (<FH>){
		chomp;
		my ($eventid, $desc) = split(/\,/, $_);
		if ($eventid == $self->data->{eventid}){
			return $desc;
		}
	}
	close(FH);
	return 'Unknown event ID';
}

1;