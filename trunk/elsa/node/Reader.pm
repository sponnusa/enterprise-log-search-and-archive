package Reader;
use Moose;
use Data::Dumper;
use DBI;
use Socket qw(inet_aton);
use Log::Log4perl;
use String::CRC32;

use constant FIELD_TIMESTAMP => 0;
use constant FIELD_HOST => 1;
use constant FIELD_PROGRAM => 2;
use constant FIELD_CLASS_ID => 3;
use constant FIELD_MSG => 4;
use constant FIELD_I0 => 5;
use constant FIELD_I1 => 6;
use constant FIELD_I2 => 7;
use constant FIELD_I3 => 8;
use constant FIELD_I4 => 9;
use constant FIELD_I5 => 10;
use constant FIELD_S0 => 11;
use constant FIELD_S1 => 12;
use constant FIELD_S2 => 13;
use constant FIELD_S3 => 14;
use constant FIELD_S4 => 15;
use constant FIELD_S5 => 16;

my $Missing_field_tolerance = 1;
our $Default_class_id = 1;
our $Proto_map = {
	'ICMP' => 1,
	'icmp' => 1,
	'TCP' => 6,
	'tcp' => 6,
	'UDP' => 17,
	'udp' => 17,
};
our $Log_parse_errors = 1;

has 'class_info' => (is => 'rw', isa => 'HashRef', required => 1);
has 'log' => ( is => 'ro', isa => 'Log::Log4perl::Logger', required => 1 );
has 'conf' => ( is => 'ro', isa => 'Config::JSON', required => 1 );
has 'db' => (is => 'rw', isa => 'Object', required => 1);
has 'cache' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'to_add' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });
has 'offline_processing' => (is => 'rw', isa => 'Bool', required => 1, default => 0);
has 'offline_processing_times' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { { start => CORE::time(), end => 0 } });

sub BUILDARGS {
	my $class = shift;
	my %params = @_;
	
	if ($params{config_file}){
		$params{conf} = Config::JSON->new($params{config_file});
	}
	
	if ($params{conf}){ # wrap this in a condition so that the right error message will be thrown if no conf
		unless ($params{log}){
			my $logdir = $params{conf}->get('logdir');
			my $debug_level = $params{conf}->get('debug_level');
			my $l4pconf = qq(
				log4perl.category.ELSA       = $debug_level, File
				log4perl.appender.File			 = Log::Log4perl::Appender::File
				log4perl.appender.File.filename  = $logdir/node.log
				log4perl.appender.File.syswrite = 1
				log4perl.appender.File.recreate = 1
				log4perl.appender.File.layout = Log::Log4perl::Layout::PatternLayout
				log4perl.appender.File.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %m%n
				log4perl.filter.ScreenLevel               = Log::Log4perl::Filter::LevelRange
				log4perl.filter.ScreenLevel.LevelMin  = $debug_level
				log4perl.filter.ScreenLevel.LevelMax  = ERROR
				log4perl.filter.ScreenLevel.AcceptOnMatch = true
				log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
				log4perl.appender.Screen.Filter = ScreenLevel 
				log4perl.appender.Screen.stderr  = 1
				log4perl.appender.Screen.layout = Log::Log4perl::Layout::PatternLayout
				log4perl.appender.Screen.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %m%n
			);
			Log::Log4perl::init( \$l4pconf ) or die("Unable to init logger\n");
			$params{log} = Log::Log4perl::get_logger("ELSA") or die("Unable to init logger\n");
		}
		
		my $dbh = DBI->connect(($params{conf}->get('database/dsn') or 'dbi:mysql:database=syslog;'), 
			$params{conf}->get('database/username'), 
			$params{conf}->get('database/password'), 
			{
				RaiseError => 1, 
				mysql_auto_reconnect => 1,
				mysql_local_infile => 1, # Needed by some MySQL implementations
			}
		) or die 'connection failed ' . $! . ' ' . $DBI::errstr;
		$params{db} = $dbh;
		
		unless ($params{class_info}){
			$params{class_info} = get_class_info($dbh);
		}
		
		$Log_parse_errors = $params{conf}->get('log_parse_errors') if defined $params{conf}->get('log_parse_errors');
	}
	
	return \%params;
}

sub get_class_info {
	my $dbh = shift;
	my $ret = { classes => {}, classes_by_id => {}, fields => [], field_conversions => {}, };
	my ($query, $sth);	
	# Get classes
	$query = "SELECT id, class FROM classes";
	$sth = $dbh->prepare($query);
	$sth->execute;
	while (my $row = $sth->fetchrow_hashref){
		$ret->{classes_by_id}->{ $row->{id} } = $row->{class};
	}
		
	# Get fields
	$query = "SELECT DISTINCT field, class, field_type, input_validation, field_id, class_id, field_order,\n" .
		"IF(class!=\"\", CONCAT(class, \".\", field), field) AS fqdn_field, pattern_type\n" .
		"FROM fields\n" .
		"JOIN fields_classes_map t2 ON (fields.id=t2.field_id)\n" .
		"JOIN classes t3 ON (t2.class_id=t3.id)\n";
	$sth = $dbh->prepare($query);
	$sth->execute;
	while (my $row = $sth->fetchrow_hashref){
		push @{ $ret->{fields} }, {
			field => $row->{field},
			fqdn_field => $row->{fqdn_field},
			class => $row->{class}, 
			value => $row->{field}, 
			text => uc($row->{field}),
			field_id => $row->{field_id},
			class_id => $row->{class_id},
			field_order => $row->{field_order},
			field_type => $row->{field_type},
			input_validation => $row->{input_validation},
			pattern_type => $row->{pattern_type},
		};
	}
	
	# Find unique classes;
	foreach my $class_id (keys %{ $ret->{classes_by_id} }){
		$ret->{classes}->{ $ret->{classes_by_id}->{$class_id} } = $class_id;
	}
	
	# Find unique field conversions
	foreach my $field_hash (@{ $ret->{fields} }){
		$ret->{field_conversions}->{ $field_hash->{class_id} } ||= {};
		if ($field_hash->{pattern_type} eq 'IPv4'){
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{IPv4} ||= {};
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{IPv4}->{ $field_hash->{field_order} } = $field_hash->{field};
		}
		elsif ($field_hash->{field} eq 'proto' and $field_hash->{pattern_type} eq 'QSTRING'){
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{PROTO} ||= {};
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{PROTO}->{ $field_hash->{field_order} } = $field_hash->{field};
		}
		elsif ($field_hash->{field} eq 'country_code' and $field_hash->{pattern_type} eq 'QSTRING'){
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{COUNTRY_CODE} ||= {};
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{COUNTRY_CODE}->{ $field_hash->{field_order} } = $field_hash->{field};
		}
	}
			
	# Find fields by arranged by order
	$ret->{fields_by_order} = {};
	foreach my $field_hash (@{ $ret->{fields} }){
		$ret->{fields_by_order}->{ $field_hash->{class_id} } ||= {};
		$ret->{fields_by_order}->{ $field_hash->{class_id} }->{ $field_hash->{field_order} } = $field_hash;
	}
	
	# Find fields by arranged by short field name
	$ret->{fields_by_name} = {};
	foreach my $field_hash (@{ $ret->{fields} }){
		$ret->{fields_by_name}->{ $field_hash->{field} } ||= [];
		push @{ $ret->{fields_by_name}->{ $field_hash->{field} } }, $field_hash;
	}
	
	return $ret;
}

sub parse {
	my $self = shift;
	my $input = shift;
	
	if (ref($input)){
		if (ref($input) eq 'HASH'){
			return $self->parse_hash($input);
		}
	}
	else {
		return $self->parse_line($input);
	}
}

sub parse_line {
	my $self = shift;
	my $raw_line = shift;
	
	chomp($raw_line);
	
	# Escape any backslashes
	$raw_line =~ s/\\/\\\\/g;
		
	my @line = split(/\t/, $raw_line);
	
	# Fix class_id for "unknown"
    if ($line[FIELD_CLASS_ID] eq 'unknown'){
    	$line[FIELD_CLASS_ID] = $Default_class_id;
    }
    		        
	# If we're configured to do so, we'll tolerate missing a missing field and make up a default
	if ($Missing_field_tolerance){
		my $missing_fields = 0;
		# Make sure that we've got the basics--things we don't want to fake
		unless ($line[FIELD_HOST] and $line[FIELD_MSG]){
			die "Unable to parse log line: $raw_line.  Only parsed into:\n" . Dumper(\@line);
		}
		unless ($line[FIELD_TIMESTAMP]){
			$line[FIELD_TIMESTAMP] = time();
			$self->log->warn('Missing required field timestamp') if $Log_parse_errors;
			$missing_fields++;
		}
		unless ($line[FIELD_PROGRAM]){
			# Check to see if this is a dumb situation in which Cisco put program in msg
			$line[FIELD_PROGRAM] = 'unknown';
			$self->log->warn('Missing required field program') if $Log_parse_errors;
			$missing_fields++;
		}
		unless ($line[FIELD_CLASS_ID]){
			$line[FIELD_CLASS_ID] = $Default_class_id;
			$self->log->warn('Missing required field class id') if $Log_parse_errors;
			$missing_fields++;
		}
					
		if ($missing_fields > $Missing_field_tolerance){
			die "Unable to parse log line $raw_line: not enough fields.  Only parsed into:\n" . Dumper(\@line);
		}
	}
	else {
		# No tolerance for any missing fields
		unless ($line[FIELD_TIMESTAMP] and $line[FIELD_CLASS_ID] and $line[FIELD_HOST] and
			$line[FIELD_PROGRAM] and $line[FIELD_MSG]){
			die "Unable to parse log line $raw_line: no tolerance for missing fields.  Only parsed into:\n" . Dumper(\@line);
		}
	}
    
    unless ($self->class_info->{classes_by_id}->{ $line[FIELD_CLASS_ID] }){
		die "Unable to parse valid class id from log line $raw_line.  Only parsed into:\n" . Dumper(\@line);
	}
	
	# Fix weird programs that may be wrong
	if ($line[FIELD_PROGRAM] =~ /^\d+$/){
#		$self->log->debug("ALL NUMBER PROG: " . $line[FIELD_PROGRAM] . ", raw_line: $raw_line");
		$line[FIELD_PROGRAM] = 'unknown';
	}
	
	# Normalize program name to be all lowercase
	$line[FIELD_PROGRAM] = lc($line[FIELD_PROGRAM]);
	
	# Normalize program name to swap any weird chars with underscores
	#$line[FIELD_PROGRAM] =~ s/[^a-zA-Z0-9\_\-]/\_/g;
	
	# Host gets the int version of itself
	$line[FIELD_HOST] = unpack('N*', inet_aton($line[FIELD_HOST]));
	
	# Perform a crc32 conversion of the program and store it in the cache for later recording
	if ($self->cache->{ $line[FIELD_PROGRAM] }){
		$line[FIELD_PROGRAM] = $self->cache->{ $line[FIELD_PROGRAM] };
	}
	else {
		my $program = $line[FIELD_PROGRAM];
		$line[FIELD_PROGRAM] = crc32( $program );
		$self->to_add->{ $program } = { id => $line[FIELD_PROGRAM], class_id => $line[FIELD_CLASS_ID] };
		$self->cache->{ $program } = $line[FIELD_PROGRAM];
	}
	
	if ($line[FIELD_CLASS_ID] ne 1){ #skip default since there aren't any fields
		# Convert any IP fields as necessary
		foreach my $field_order (keys %{ $self->class_info->{field_conversions}->{ $line[FIELD_CLASS_ID] }->{'IPv4'} }){
			$line[$field_order] = unpack('N', inet_aton($line[$field_order]));
		}
		
		# Convert any proto fields as necessary
		foreach my $field_order (keys %{ $self->class_info->{field_conversions}->{ $line[FIELD_CLASS_ID] }->{PROTO} }){
			$line[$field_order] = $Proto_map->{ $line[$field_order] };
		}
	}
	
	# Update start/end times if necessary
	if ($self->offline_processing){
		if ($line[FIELD_TIMESTAMP] < $self->offline_processing_times->{start}){
			$self->offline_processing_times->{start} = $line[FIELD_TIMESTAMP];
		}
		if ($line[FIELD_TIMESTAMP] > $self->offline_processing_times->{end}){
			$self->offline_processing_times->{end} = $line[FIELD_TIMESTAMP];
		}
	}
	
	# Write nulls explicitly
	for (my $i = FIELD_I0; $i <= FIELD_S5; $i++){
		$line[$i] ||= undef;
	}
		
	# Push our auto-inc dummy val on
	#unshift(@line, '0');
	
	return \@line;
}

sub parse_hash {
	my $self = shift;
	my $hash = shift;
		
	# Fix class_id for "unknown"
    if ($hash->{class_id} eq 'unknown'){
    	$hash->{class_id} = $Default_class_id;
    }
    		        
	# If we're configured to do so, we'll tolerate missing a missing field and make up a default
	if ($Missing_field_tolerance){
		my $missing_fields = 0;
		# Make sure that we've got the basics--things we don't want to fake
		unless ($hash->{host} and $hash->{msg}){
			die "Unable to parse log line.  Only parsed into:\n" . Dumper($hash);
		}
		unless ($hash->{timestamp}){
			$hash->{timestamp} = time();
			$self->log->warn('Missing required field timestamp') if $Log_parse_errors;
			$missing_fields++;
		}
		unless ($hash->{program}){
			# Check to see if this is a dumb situation in which Cisco put program in msg
			$hash->{program} = 'unknown';
			$self->log->warn('Missing required field program') if $Log_parse_errors;
			$missing_fields++;
		}
		unless ($hash->{class_id}){
			$hash->{class_id} = $Default_class_id;
			$self->log->warn('Missing required field class id') if $Log_parse_errors;
			$missing_fields++;
		}
					
		if ($missing_fields > $Missing_field_tolerance){
			die "Unable to parse log line. Not enough fields.  Only parsed into:\n" . Dumper($hash);
		}
	}
	else {
		# No tolerance for any missing fields
		unless ($hash->{timestamp} and $hash->{class_id} and $hash->{host} and
			$hash->{program} and $hash->{msg}){
			die "Unable to parse log line. No tolerance for missing fields.  Only parsed into:\n" . Dumper($hash);
		}
	}
    
    unless ($self->class_info->{classes_by_id}->{ $hash->{class_id} }){
		die "Unable to parse valid class id from log line.  Only parsed into:\n" . Dumper($hash);
	}
	
	# Fix weird programs that may be wrong
	if ($hash->{program} =~ /^\d+$/){
#		$self->log->debug("ALL NUMBER PROG: " . $line[FIELD_PROGRAM] . ", raw_line: $raw_line");
		$hash->{program} = 'unknown';
	}
	
	# Normalize program name to be all lowercase
	$hash->{program} = lc($hash->{program});
	
	# Host gets the int version of itself
	$hash->{host} = unpack('N*', inet_aton($hash->{host}));
	
	# Perform a crc32 conversion of the program and store it in the cache for later recording
	if ($self->cache->{ $hash->{program} }){
		$hash->{program} = $self->cache->{ $hash->{program} };
	}
	else {
		my $program = $hash->{program};
		$hash->{program} = crc32( $program );
		$self->to_add->{ $program } = { id => $hash->{program}, class_id => $hash->{class_id} };
		$self->cache->{ $program } = $hash->{program};
	}
	
	my @line = ($hash->{timestamp}, $hash->{host}, $hash->{program}, $hash->{class_id}, $hash->{msg});
	
	if ($hash->{class_id} ne 1){ #skip default since there aren't any fields
		#foreach my $field_order (sort { $a cmp $b } keys %{ $self->class_info->{fields_by_order}->{ $hash->{class_id} } }){
		for (my $i = FIELD_I0; $i <= FIELD_S5; $i++){
			my $value = $self->class_info->{fields_by_order}->{ $hash->{class_id} }->{$i} ? $hash->{ $self->class_info->{fields_by_order}->{ $hash->{class_id} }->{$i}->{field} } : undef;
			# Convert any IP fields as necessary
			if ($self->class_info->{field_conversions}->{ $hash->{class_id} }->{IPv4}->{$i}){
				$value = unpack('N', inet_aton($value));
			}
			# Convert any proto fields as necessary
			elsif ($self->class_info->{field_conversions}->{ $hash->{class_id} }->{PROTO}->{$i}){
				$value = $Proto_map->{$value};
			}
			if (not defined $value){
				$line[$i] = $i <= FIELD_I5 ? 0 : '';
			}
			else {
				$line[$i] = $value;
			}
		}
	}
	
	# Push our auto-inc dummy val on
	#unshift(@line, '0');
	
	# Update start/end times if necessary
	if ($self->offline_processing){
		if ($hash->{timestamp} < $self->offline_processing_times->{start}){
			$self->offline_processing_times->{start} = $hash->{timestamp};
		}
		if ($hash->{timestamp} > $self->offline_processing_times->{end}){
			$self->offline_processing_times->{end} = $hash->{timestamp};
		}
	}
			
	return \@line;
}


__PACKAGE__->meta->make_immutable;
1;