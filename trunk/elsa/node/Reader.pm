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
	'HOPOPT' => 0,
	'hopopt' => 0,
	'ICMP' => 1,
	'icmp' => 1,
	'IGMP' => 2,
	'igmp' => 2,
	'GGP' => 3,
	'ggp' => 3,
	'IPv4' => 4,
	'ipv4' => 4,
	'ST' => 5,
	'st' => 5,
	'TCP' => 6,
	'tcp' => 6,
	'CBT' => 7,
	'cbt' => 7,
	'EGP' => 8,
	'egp' => 8,
	'IGP' => 9,
	'igp' => 9,
	'BBN-RCC-MON' => 10,
	'bbn-rcc-mon' => 10,
	'NVP-II' => 11,
	'nvp-ii' => 11,
	'PUP' => 12,
	'pup' => 12,
	'ARGUS' => 13,
	'argus' => 13,
	'EMCON' => 14,
	'emcon' => 14,
	'XNET' => 15,
	'xnet' => 15,
	'CHAOS' => 16,
	'chaos' => 16,
	'UDP' => 17,
	'udp' => 17,
	'MUX' => 18,
	'mux' => 18,
	'DCN-MEAS' => 19,
	'dcn-meas' => 19,
	'HMP' => 20,
	'hmp' => 20,
	'PRM' => 21,
	'prm' => 21,
	'XNS-IDP' => 22,
	'xns-idp' => 22,
	'TRUNK-1' => 23,
	'trunk-1' => 23,
	'TRUNK-2' => 24,
	'trunk-2' => 24,
	'LEAF-1' => 25,
	'leaf-1' => 25,
	'LEAF-2' => 26,
	'leaf-2' => 26,
	'RDP' => 27,
	'rdp' => 27,
	'IRTP' => 28,
	'irtp' => 28,
	'ISO-TP4' => 29,
	'iso-tp4' => 29,
	'NETBLT' => 30,
	'netblt' => 30,
	'MFE-NSP' => 31,
	'mfe-nsp' => 31,
	'MERIT-INP' => 32,
	'merit-inp' => 32,
	'DCCP' => 33,
	'dccp' => 33,
	'3PC' => 34,
	'3pc' => 34,
	'IDPR' => 35,
	'idpr' => 35,
	'XTP' => 36,
	'xtp' => 36,
	'DDP' => 37,
	'ddp' => 37,
	'IDPR-CMTP' => 38,
	'idpr-cmtp' => 38,
	'TP++' => 39,
	'tp++' => 39,
	'IL' => 40,
	'il' => 40,
	'IPv6' => 41,
	'ipv6' => 41,
	'SDRP' => 42,
	'sdrp' => 42,
	'IPv6-Route' => 43,
	'ipv6-route' => 43,
	'IPv6-Frag' => 44,
	'ipv6-frag' => 44,
	'IDRP' => 45,
	'idrp' => 45,
	'RSVP' => 46,
	'rsvp' => 46,
	'GRE' => 47,
	'gre' => 47,
	'DSR' => 48,
	'dsr' => 48,
	'BNA' => 49,
	'bna' => 49,
	'ESP' => 50,
	'esp' => 50,
	'AH' => 51,
	'ah' => 51,
	'I-NLSP' => 52,
	'i-nlsp' => 52,
	'SWIPE' => 53,
	'swipe' => 53,
	'NARP' => 54,
	'narp' => 54,
	'MOBILE' => 55,
	'mobile' => 55,
	'TLSP' => 56,
	'tlsp' => 56,
	'SKIP' => 57,
	'skip' => 57,
	'IPv6-ICMP' => 58,
	'ipv6-icmp' => 58,
	'IPv6-NoNxt' => 59,
	'ipv6-nonxt' => 59,
	'IPv6-Opts' => 60,
	'ipv6-opts' => 60,
	'CFTP' => 62,
	'cftp' => 62,
	'SAT-EXPAK' => 64,
	'sat-expak' => 64,
	'KRYPTOLAN' => 65,
	'kryptolan' => 65,
	'RVD' => 66,
	'rvd' => 66,
	'IPPC' => 67,
	'ippc' => 67,
	'SAT-MON' => 69,
	'sat-mon' => 69,
	'VISA' => 70,
	'visa' => 70,
	'IPCV' => 71,
	'ipcv' => 71,
	'CPNX' => 72,
	'cpnx' => 72,
	'CPHB' => 73,
	'cphb' => 73,
	'WSN' => 74,
	'wsn' => 74,
	'PVP' => 75,
	'pvp' => 75,
	'BR-SAT-MON' => 76,
	'br-sat-mon' => 76,
	'SUN-ND' => 77,
	'sun-nd' => 77,
	'WB-MON' => 78,
	'wb-mon' => 78,
	'WB-EXPAK' => 79,
	'wb-expak' => 79,
	'ISO-IP' => 80,
	'iso-ip' => 80,
	'VMTP' => 81,
	'vmtp' => 81,
	'SECURE-VMTP' => 82,
	'secure-vmtp' => 82,
	'VINES' => 83,
	'vines' => 83,
	'TTP' => 84,
	'ttp' => 84,
	'IPTM' => 84,
	'iptm' => 84,
	'NSFNET-IGP' => 85,
	'nsfnet-igp' => 85,
	'DGP' => 86,
	'dgp' => 86,
	'TCF' => 87,
	'tcf' => 87,
	'EIGRP' => 88,
	'eigrp' => 88,
	'OSPFIGP' => 89,
	'ospfigp' => 89,
	'Sprite-RPC' => 90,
	'sprite-rpc' => 90,
	'LARP' => 91,
	'larp' => 91,
	'MTP' => 92,
	'mtp' => 92,
	'AX.25' => 93,
	'ax.25' => 93,
	'IPIP' => 94,
	'ipip' => 94,
	'MICP' => 95,
	'micp' => 95,
	'SCC-SP' => 96,
	'scc-sp' => 96,
	'ETHERIP' => 97,
	'etherip' => 97,
	'ENCAP' => 98,
	'encap' => 98,
	'GMTP' => 100,
	'gmtp' => 100,
	'IFMP' => 101,
	'ifmp' => 101,
	'PNNI' => 102,
	'pnni' => 102,
	'PIM' => 103,
	'pim' => 103,
	'ARIS' => 104,
	'aris' => 104,
	'SCPS' => 105,
	'scps' => 105,
	'QNX' => 106,
	'qnx' => 106,
	'A/N' => 107,
	'a/n' => 107,
	'IPComp' => 108,
	'ipcomp' => 108,
	'SNP' => 109,
	'snp' => 109,
	'Compaq-Peer' => 110,
	'compaq-peer' => 110,
	'IPX-in-IP' => 111,
	'ipx-in-ip' => 111,
	'VRRP' => 112,
	'vrrp' => 112,
	'PGM' => 113,
	'pgm' => 113,
	'L2TP' => 115,
	'l2tp' => 115,
	'DDX' => 116,
	'ddx' => 116,
	'IATP' => 117,
	'iatp' => 117,
	'STP' => 118,
	'stp' => 118,
	'SRP' => 119,
	'srp' => 119,
	'UTI' => 120,
	'uti' => 120,
	'SMP' => 121,
	'smp' => 121,
	'SM' => 122,
	'sm' => 122,
	'PTP' => 123,
	'ptp' => 123,
	'ISIS over IPv4' => 124,
	'isis over ipv4' => 124,
	'FIRE' => 125,
	'fire' => 125,
	'CRTP' => 126,
	'crtp' => 126,
	'CRUDP' => 127,
	'crudp' => 127,
	'SSCOPMCE' => 128,
	'sscopmce' => 128,
	'IPLT' => 129,
	'iplt' => 129,
	'SPS' => 130,
	'sps' => 130,
	'PIPE' => 131,
	'pipe' => 131,
	'SCTP' => 132,
	'sctp' => 132,
	'FC' => 133,
	'fc' => 133,
	'RSVP-E2E-IGNORE' => 134,
	'rsvp-e2e-ignore' => 134,
	'Mobility Header' => 135,
	'mobility header' => 135,
	'UDPLite' => 136,
	'udplite' => 136,
	'MPLS-in-IP' => 137,
	'mpls-in-ip' => 137,
	'manet' => 138,
	'manet' => 138,
	'HIP' => 139,
	'hip' => 139,
	'Shim6' => 140,
	'shim6' => 140,
	'WESP' => 141,
	'wesp' => 141,
	'ROHC' => 142,
	'rohc' => 142,
	'Reserved' => 255,
	'reserved' => 255,
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
    		        
	# If we're configured to do so, we'll tolerate missing a field and make up a default
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
			$line[$field_order] = exists $Proto_map->{ $line[$field_order] } ? $Proto_map->{ $line[$field_order] } : $line[$field_order];
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
				$value = exists $Proto_map->{$value} ? $Proto_map->{$value} : $value;
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