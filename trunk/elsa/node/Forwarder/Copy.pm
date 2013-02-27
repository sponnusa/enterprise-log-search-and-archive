package Forwarder::Copy;
use Moose;
use Data::Dumper;
use File::Copy;
extends 'Forwarder';

has 'dir' => (is => 'rw', isa => 'Str', required => 1);

sub forward {
	my $self = shift;
	
	foreach (@_){
		$self->log->trace('Copying file ' . $_);
		move($_, $self->dir . '/') or ($self->log->error('Error copying ' . $_. ' to dir ' . $self->dir . ': ' . $!)
			and return 0);
	}
	
	return 1;					
}

__PACKAGE__->meta->make_immutable;

1;