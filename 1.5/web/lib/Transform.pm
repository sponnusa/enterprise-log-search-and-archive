package Transform;
use Moose;
use Log::Log4perl;

# Base class for Transform plugins
has 'conf' => (is => 'rw', isa => 'Object', required => 1);
has 'log' => (is => 'rw', isa => 'Object', required => 1);
has 'data' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });
has 'user' => (is => 'rw', isa => 'User');
# A transform may be a "meta" tranform which refers to other transforms
has 'transforms' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => '');
has 'groupby' => (is => 'rw', isa => 'Str', required => 1, default => '');
has 'args' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });

1;