package Utils;
use Data::Dumper;
use Moose::Role;
with 'MooseX::Log::Log4perl';
use Config::JSON;
use DBI;
use JSON;
use IO::Handle;
use IO::File;
use Digest::HMAC_SHA1;
use Socket;

our $Db_timeout = 3;
our $Bulk_dir = '/tmp';

has 'log' => ( is => 'ro', isa => 'Log::Log4perl::Logger', required => 1 );
has 'conf' => (is => 'rw', isa => 'Object', required => 1);
has 'db' => (is => 'rw', isa => 'Object', required => 1);
has 'json' => (is => 'ro', isa => 'JSON', required => 1);
#has 'bulk_dir' => (is => 'rw', isa => 'Str', required => 1, default => $Bulk_dir);
has 'db_timeout' => (is => 'rw', isa => 'Int', required => 1, default => $Db_timeout);

around BUILDARGS => sub {
	my $orig = shift;
	my $class = shift;
	my %params = @_;
		
	if ($params{config_file}){
		$params{conf} = new Config::JSON ( $params{config_file} ) or die("Unable to open config file");
	}		
	
	my $log_level = 'DEBUG';
	if ($ENV{DEBUG_LEVEL}){
		$log_level = $ENV{DEBUG_LEVEL};
	}
	elsif ($params{conf}->get('debug_level')){
		$log_level = $params{conf}->get('debug_level');
	}
	my $logdir = $params{conf}->get('logdir');
	my $logfile = 'web.log';
	if ($params{conf}->get('logfile')){
		$logfile = $params{conf}->get('logfile');
	}
	
	my $log_conf = qq(
		log4perl.category.App       = $log_level, File
		log4perl.appender.File			 = Log::Log4perl::Appender::File
		log4perl.appender.File.filename  = $logdir/$logfile 
		log4perl.appender.File.layout = Log::Log4perl::Layout::PatternLayout
		log4perl.appender.File.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %x%n%m%n
		log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
		log4perl.appender.Screen.stderr  = 1
		log4perl.appender.Screen.layout = Log::Log4perl::Layout::PatternLayout
		log4perl.appender.Screen.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %x%n%m%n
		log4perl.appender.Syncer            = Log::Log4perl::Appender::Synchronized
		log4perl.appender.Syncer.appender   = File
	);
		
	Log::Log4perl::init( \$log_conf ) or die("Unable to init logger\n");
	$params{log} = Log::Log4perl::get_logger('App')
	  or die("Unable to init logger\n");
	
	if ($params{conf}->get('db/timeout')){
		$Db_timeout = $params{conf}->get('db/timeout');
	}
	
	$params{db} = DBI->connect_cached(
		$params{conf}->get('meta_db/dsn'),
		$params{conf}->get('meta_db/username'),
		$params{conf}->get('meta_db/password'),
		{ 
			PrintError => 0,
			HandleError => \&_dbh_error_handler,
			AutoCommit => 1,
			mysql_connect_timeout => $Db_timeout,
			mysql_auto_reconnect => 1, # we will auto-reconnect on disconnect
		}
	) or die($DBI::errstr);
	
	if ($params{conf}->get('debug_level') eq 'DEBUG' or $params{conf}->get('debug_level') eq 'TRACE'){
		$params{json} = JSON->new->pretty->allow_nonref->allow_blessed->convert_blessed;	
	}
	else {
		$params{json} = JSON->new->allow_nonref->allow_blessed->convert_blessed;
	}
	
	if ($params{conf}->get('bulk_dir')){
		$Bulk_dir = $params{conf}->get('bulk_dir');
	}
	
	return $class->$orig(%params);
};

sub _dbh_error_handler {
	my $errstr = shift;
	my $dbh    = shift;
	my $query  = $dbh->{Statement};

	$errstr .= " QUERY: $query";
	Log::Log4perl::get_logger('App')->error($errstr);
	foreach my $sth (grep { defined } @{$dbh->{ChildHandles}}){
		$sth->rollback; # in case there was an active transaction
	}
	
	confess($errstr);
}

sub freshen_db {
	my $self = shift;
	$self->db(
		DBI->connect_cached(
			$self->conf->get('meta_db/dsn'),
			$self->conf->get('meta_db/username'),
			$self->conf->get('meta_db/password'),
			{ 
				PrintError => 0,
				HandleError => \&_dbh_error_handler,
				#RaiseError => 1,
				AutoCommit => 1,
				mysql_connect_timeout => $Db_timeout,
				mysql_auto_reconnect => 1, # we will auto-reconnect on disconnect
			})
	);
}

sub epoch2iso {
	my $epochdate = shift;
	my $use_gm_time = shift;
	
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst);
	if ($use_gm_time){
		($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime($epochdate);
	}
	else {
		($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($epochdate);
	}
	my $date = sprintf("%04d-%02d-%02d %02d:%02d:%02d", 
		$year + 1900, $mon + 1, $mday, $hour, $min, $sec);
	return $date;
}

sub get_hash {
	my ($self, $data) = shift;
	my $digest = new Digest::HMAC_SHA1($self->conf->get('link_key'));
	$digest->add($data);
	return $digest->hexdigest();
}

sub _get_node_info {
	my $self = shift;
	my $user = shift;
	my ($query, $sth);
	
	my $nodes = $self->_get_nodes($user);
	$self->log->trace('got nodes: ' . Dumper($nodes));
	
	unless (scalar keys %$nodes){
		die('No nodes available');
	}
		
	my $ret = { nodes => {} };
	
	# Get indexes from all nodes in parallel
	my $cv = AnyEvent->condvar;
	$cv->begin(sub {
		$cv->send;
	});
	
	foreach my $node (keys %$nodes){
		if (exists $nodes->{$node}->{error}){
			$self->add_warning('node ' . $node . ' had error ' . $nodes->{$node}->{error});
			delete $ret->{nodes}->{$node};
			next;
		}
		$ret->{nodes}->{$node} = {
			db => $nodes->{$node}->{db},
			dbh => $nodes->{$node}->{dbh},
		};
				
		# Get indexes
		$query = sprintf('SELECT CONCAT(SUBSTR(type, 1, 4), "_", id) AS name, start, 
		UNIX_TIMESTAMP(start) AS start_int, end, UNIX_TIMESTAMP(end) AS end_int, type, records 
		FROM %s.v_indexes WHERE type="temporary" OR (type="permanent" AND ISNULL(locked_by)) OR type="realtime" ORDER BY start', 
			$nodes->{$node}->{db});
		$cv->begin;
		$self->log->trace($query);
		$nodes->{$node}->{dbh}->query($query, sub {
				my ($dbh, $rows, $rv) = @_;
				
				if ($rv and $rows){
					#$self->log->trace('node returned rv: ' . $rv);
					$ret->{nodes}->{$node}->{indexes} = {
						indexes => $rows,
						min => $rows->[0]->{start_int},
						max => $rows->[$#$rows]->{end_int},
						start_max => $rows->[$#$rows]->{start_int},
					};
				}
				else {
					$self->log->error('No indexes for node ' . $node . ', rv: ' . $rv);
					$ret->{nodes}->{$node}->{error} = 'No indexes for node ' . $node;
				}
				$cv->end;
			});
		
		# Get tables
		$query = sprintf('SELECT table_name, start, UNIX_TIMESTAMP(start) AS start_int, end, ' .
			'UNIX_TIMESTAMP(end) AS end_int, table_type, min_id, max_id, max_id - min_id AS records ' .
			'FROM %s.tables t1 JOIN table_types t2 ON (t1.table_type_id=t2.id) ORDER BY start', 
			$nodes->{$node}->{db});
		$cv->begin;
		$self->log->trace($query);
		$nodes->{$node}->{dbh}->query($query, sub {
				my ($dbh, $rows, $rv) = @_;
				
				if ($rv and $rows){
					#$self->log->trace('node returned rv: ' . $rv);
					$ret->{nodes}->{$node}->{tables} = {
						tables => $rows,
						min => $rows->[0]->{start_int},
						max => $rows->[$#$rows]->{end_int},
						start_max => $rows->[$#$rows]->{start_int},
					};
				}
				else {
					$self->log->error('No tables for node ' . $node);
					$ret->{nodes}->{$node}->{error} = 'No tables for node ' . $node;
				}
				$cv->end;
			});
		
		# Get classes
		$query = "SELECT id, class FROM classes";
		$cv->begin;
		$self->log->trace($query);
		$nodes->{$node}->{dbh}->query($query, sub {
				my ($dbh, $rows, $rv) = @_;
				
				if ($rv and $rows){
					$ret->{nodes}->{$node}->{classes} = {};
					foreach my $row (@$rows){
						$ret->{nodes}->{$node}->{classes}->{ $row->{id} } = $row->{class};
					}
				}
				else {
					$self->log->error('No classes for node ' . $node);
					$ret->{nodes}->{$node}->{error} = 'No classes for node ' . $node;
				}
				$cv->end;
			});
		
		# Get fields
		$query = sprintf("SELECT DISTINCT field, class, field_type, input_validation, field_id, class_id, field_order,\n" .
			"IF(class!=\"\", CONCAT(class, \".\", field), field) AS fqdn_field, pattern_type\n" .
			"FROM %s.fields\n" .
			"JOIN %1\$s.fields_classes_map t2 ON (fields.id=t2.field_id)\n" .
			"JOIN %1\$s.classes t3 ON (t2.class_id=t3.id)\n", $nodes->{$node}->{db});
		$cv->begin;
		$self->log->trace($query);
		$nodes->{$node}->{dbh}->query($query, sub {
				my ($dbh, $rows, $rv) = @_;
				
				if ($rv and $rows){
					$ret->{nodes}->{$node}->{fields} = [];
					foreach my $row (@$rows){
						push @{ $ret->{nodes}->{$node}->{fields} }, {
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
				}
				else {
					$self->log->error('No fields for node ' . $node);
					$ret->{nodes}->{$node}->{error} = 'No fields for node ' . $node;
				}
				$cv->end;
			});
	}
	$cv->end;
	
	$self->log->debug('Blocking');
	$cv->recv;
	
	my $time_ranges = { indexes => {}, archive => {} };
	$ret->{totals} = {};
	foreach my $type (qw(indexes archive)){
		my $key = $type;
		if ($type eq 'archive'){
			$key = 'tables';
		}
		# Find min/max indexes
		my $min = 2**32;
		my $max = 0;
		my $start_max = 0;
		foreach my $node (keys %{ $ret->{nodes} }){
			if (defined $ret->{nodes}->{$node}->{$key}->{min} and $ret->{nodes}->{$node}->{$key}->{min} < $min){
				$min = $ret->{nodes}->{$node}->{$key}->{min};
			}
			if (defined $ret->{nodes}->{$node}->{$key}->{max} and $ret->{nodes}->{$node}->{$key}->{max} > $max){
				$max = $ret->{nodes}->{$node}->{$key}->{max};
				$start_max = $ret->{nodes}->{$node}->{$key}->{start_max};
			}
			foreach my $hash (@{ $ret->{nodes}->{$node}->{$key}->{$key} }){
				$ret->{totals}->{$type} += $hash->{records};
			}
		}
		if ($min == 2**32 and $max == 0){
			$self->log->trace('No min/max found for type ' . $type);
		}
		else {
			$ret->{$type . '_min'} = $min;
			$ret->{$type . '_max'} = $max;
			$ret->{$type . '_start_max'} = $start_max;
			$self->log->trace('Found min ' . $min . ', max ' . $max . ' for type ' . $type);
		}
	}
	
	# Resolve class names into class_id's for excluded classes
	my $given_excluded_classes = $self->conf->get('excluded_classes') ? $self->conf->get('excluded_classes') : {};
	my $excluded_classes = {};
	foreach my $node (keys %{ $ret->{nodes} }){
		foreach my $class_id (keys %{ $ret->{nodes}->{$node}->{classes} }){
			if ($given_excluded_classes->{ lc($ret->{nodes}->{$node}->{classes}->{$class_id}) } or
				$given_excluded_classes->{ uc($ret->{nodes}->{$node}->{classes}->{$class_id}) }){
				$excluded_classes->{$class_id} = 1;
			}
		}
	}
	
	# Find unique classes;
	$ret->{classes} = {};
	$ret->{classes_by_id} = {};
	foreach my $node (keys %{ $ret->{nodes} }){
		foreach my $class_id (keys %{ $ret->{nodes}->{$node}->{classes} }){
			next if $excluded_classes->{$class_id};
			$ret->{classes_by_id}->{$class_id} = $ret->{nodes}->{$node}->{classes}->{$class_id};
			$ret->{classes}->{ $ret->{nodes}->{$node}->{classes}->{$class_id} } = $class_id;
		}
	}
	
	# Find unique fields
	foreach my $node (keys %{ $ret->{nodes} }){
		FIELD_LOOP: foreach my $field_hash (@{ $ret->{nodes}->{$node}->{fields} }){
			next if $excluded_classes->{ $field_hash->{class_id} };
			foreach my $already_have_hash (@{ $ret->{fields} }){
				if ($field_hash->{fqdn_field} eq $already_have_hash->{fqdn_field}){
					next FIELD_LOOP;
				}
			}
			push @{ $ret->{fields} }, $field_hash;
		}
	}
	
	# Find unique field conversions
	$ret->{field_conversions} = {
		0 => {
			TIME => {
				0 => 'timestamp',
				100 => 'minute',
				101 => 'hour',
				102 => 'day',
			},
		},
	};
	foreach my $field_hash (@{ $ret->{fields} }){
		next if $excluded_classes->{ $field_hash->{class_id} };
		$ret->{field_conversions}->{ $field_hash->{class_id} } ||= {};
		if ($field_hash->{pattern_type} eq 'IPv4'){
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{IPv4} ||= {};
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{IPv4}->{ $field_hash->{field_order} } = $field_hash->{value};
		}
		elsif ($field_hash->{value} eq 'proto' and $field_hash->{pattern_type} eq 'QSTRING'){
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{PROTO} ||= {};
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{PROTO}->{ $field_hash->{field_order} } = $field_hash->{value};
		}
		elsif ($field_hash->{value} eq 'country_code' and $field_hash->{pattern_type} eq 'NUMBER'){
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{COUNTRY_CODE} ||= {};
			$ret->{field_conversions}->{ $field_hash->{class_id} }->{COUNTRY_CODE}->{ $field_hash->{field_order} } = $field_hash->{value};
		}
	}
			
	# Find fields by arranged by order
	$ret->{fields_by_order} = {};
	foreach my $field_hash (@{ $ret->{fields} }){
		next if $excluded_classes->{ $field_hash->{class_id} };
		$ret->{fields_by_order}->{ $field_hash->{class_id} } ||= {};
		$ret->{fields_by_order}->{ $field_hash->{class_id} }->{ $field_hash->{field_order} } = $field_hash;
	}
	
	# Find fields by arranged by short field name
	$ret->{fields_by_name} = {};
	foreach my $field_hash (@{ $ret->{fields} }){
		next if $excluded_classes->{ $field_hash->{class_id} };
		$ret->{fields_by_name}->{ $field_hash->{value} } ||= [];
		push @{ $ret->{fields_by_name}->{ $field_hash->{value} } }, $field_hash;
	}
	
	# Find fields by type
	$ret->{fields_by_type} = {};
	foreach my $field_hash (@{ $ret->{fields} }){
		next if $excluded_classes->{ $field_hash->{class_id} };
		$ret->{fields_by_type}->{ $field_hash->{field_type} } ||= {};
		$ret->{fields_by_type}->{ $field_hash->{field_type} }->{ $field_hash->{value} } ||= [];
		push @{ $ret->{fields_by_type}->{ $field_hash->{field_type} }->{ $field_hash->{value} } }, $field_hash;
	}
	
	$ret->{updated_at} = time();
	$ret->{updated_for_admin} = $user->is_admin;
	
	
	foreach my $node (keys %{ $ret->{nodes} }){
		
	}
	
	return $ret;
}

sub _get_nodes {
	my $self = shift;
	my $user = shift;
	my %nodes;
	my $node_conf = $self->conf->get('nodes');
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub { shift->send });
	my $start = time();
	foreach my $node (keys %$node_conf){
		my $db_name = 'syslog';
		if ($node_conf->{$node}->{db}){
			$db_name = $node_conf->{$node}->{db};
		}
		
		my $mysql_port = 3306;
		if ($node_conf->{$node}->{port}){
			$mysql_port = $node_conf->{$node}->{port};
		}

		eval {
			$nodes{$node} = { db => $db_name };
			
			$cv->begin;
			my $node_start = time();	
			$nodes{$node}->{dbh} = AsyncDB->new(log => $self->log, db_args => [
				'dbi:mysql:database=' . $db_name . ';host=' . $node . ';port=' . $mysql_port, 
				$node_conf->{$node}->{username}, 
				$node_conf->{$node}->{password}, 
				{
					mysql_connect_timeout => $self->db_timeout,
					PrintError => 0,
					mysql_multi_statements => 1,
				}
			], cb => sub {
				$self->log->trace('connected to ' . $node . ' on ' . $mysql_port . ' in ' . (time() - $node_start));
				$cv->end;
			});
			
		};
		if ($@){
			$self->add_warning($@);
			delete $nodes{$node};
		}		
	}
	$cv->end;
	$cv->recv;
	$self->log->trace('All connected in ' . (time() - $start) . ' seconds');
		
	return \%nodes;
}

sub old_get_nodes {
	my $self = shift;
	my $user = shift;
	my %nodes;
	my $node_conf = $self->conf->get('nodes');
	
	my $mysql_port = 3306;
	my $db_name = 'syslog';
	foreach my $node (keys %$node_conf){
		next unless $user->is_permitted('node_id', unpack('N*', inet_aton($node)));
		if ($node_conf->{$node}->{port}){
			$mysql_port = $node_conf->{$node}->{port};
		}
		
		if ($node_conf->{$node}->{db}){
			$db_name = $node_conf->{$node}->{db};
		}
		eval {
			$nodes{$node} = { db => $db_name };
			$nodes{$node}->{dbh} = AsyncMysql->new(log => $self->log, db_args => [
				'dbi:mysql:database=' . $db_name . ';host=' . $node . ';port=' . $mysql_port,  
				$node_conf->{$node}->{username}, 
				$node_conf->{$node}->{password}, 
				{
					mysql_connect_timeout => $self->db_timeout,
					PrintError => 0,
					mysql_multi_statements => 1,
				}
			]);
		};
		if ($@){
			$self->log->error($@);
			$self->add_warning($@);
			delete $nodes{$node};
		}
	}
		
	return \%nodes;
}



1;
