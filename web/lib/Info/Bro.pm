package Info::Bro;
use Moose;
extends 'Info';
has 'plugins' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [qw(getPcap)] });

1;