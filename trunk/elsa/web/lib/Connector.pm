package Connector;
use Moose;
use MooseX::ClassAttribute;

# Base class for Transform plugins
has 'api' => (is => 'rw', isa => 'Object', required => 1);
has 'user_info' => (is => 'rw', isa => 'HashRef', required => 1);
has 'data' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { [] });
has 'query' => (is => 'rw', isa => 'HashRef');
#has 'description' => (is => 'rw', isa => 'Str', required => 1);


1;