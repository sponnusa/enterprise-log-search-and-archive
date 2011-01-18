package ELSA::Exceptions;
use strict;
use warnings;

our @ISA = qw(Exporter);
our @EXPORT = qw( throw_e throw_params throw_sql throw_parse );

use Exception::Class (
	'ELSA::Exception' => {
		isa => 'Exception::Class::Base',
		description => 'Generic ELSA exception',
		fields => 'error',
		alias => 'throw_e',
	},
	'ELSA::Exception::Param' => {
		isa => 'Exception::Class::Base',
		description => 'Invalid method param given',
		fields => ['param', 'value' ],
		alias => 'throw_params',
	},
	'ELSA::Exception::SQL' => {
		isa => 'Exception::Class::Base',
		description => 'SQL error',
		fields => [ 'sql_error', 'query', 'args' ],
		alias => 'throw_sql',
	},
	'ELSA::Exception::Parse' => {
		isa => 'Exception::Class::Base',
		description => 'Parsing error',
		fields => [ 'parse_error', 'text' ],
		alias => 'throw_parse',
	},
);

1;