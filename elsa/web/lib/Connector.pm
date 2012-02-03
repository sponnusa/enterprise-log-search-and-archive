package Connector;
use Moose;

# Base class for Transform plugins
has 'api' => (is => 'rw', isa => 'Object', required => 1);
has 'user_info' => (is => 'rw', isa => 'HashRef', required => 1);
has 'data' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });

1;