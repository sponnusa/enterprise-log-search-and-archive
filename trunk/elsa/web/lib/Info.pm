package Info;
use strict;
use Data::Dumper;
use base qw(Class::Accessor);

__PACKAGE__->mk_accessors(qw(data conf summary urls plugins));

# Base class for Info plugins

1;