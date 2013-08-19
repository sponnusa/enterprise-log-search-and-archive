package Warnings;
use Moose::Role;

has 'warnings' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { 'has_warnings' => 'count', 'clear_warnings' => 'clear', 'all_warnings' => 'elements' });

sub add_warning {
	my $self = shift;
	my $code = shift;
	my $errstr = shift;
	my $data = shift;
	
	push @{ $self->warnings }, new Ouch($code, $errstr, $data);
}

1;