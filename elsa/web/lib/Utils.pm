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
use Time::HiRes qw(time);
use Hash::Merge::Simple qw(merge);
use AnyEvent::HTTP;
use URI::Escape qw(uri_escape);
use Time::HiRes qw(time);
use Digest::SHA qw(sha512_hex);
use Sys::Hostname;
use Ouch qw(:traditional);
use Exporter qw(import);

our @EXPORT = qw(catch_any);

use CustomLog;
use Results;

our $Db_timeout = 3;
our $Bulk_dir = '/tmp';
our $Auth_timestamp_grace_period = 86400;



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
	my $logfile = 'web';
	if ($params{conf}->get('logfile')){
		$logfile = $params{conf}->get('logfile');
	}
	my $tmpdir = $logdir . '/../tmp';
	
	my $log_format = 'File, RFC5424';
	if ($params{conf}->get('log_format')){
		$log_format = $params{conf}->get('log_format');
	}
	
	my $hostname = hostname;
	
	my $log_conf = qq'
		log4perl.category.App       = $log_level, $log_format
		log4perl.appender.File			 = Log::Log4perl::Appender::File
		log4perl.appender.File.filename  = $logdir/$logfile.log 
		log4perl.appender.File.layout = Log::Log4perl::Layout::PatternLayout
		log4perl.appender.File.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %x%n%m%n
		log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
		log4perl.appender.Screen.stderr  = 1
		log4perl.appender.Screen.layout = Log::Log4perl::Layout::PatternLayout
		log4perl.appender.Screen.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %x%n%m%n
		log4perl.appender.Syncer            = Log::Log4perl::Appender::Synchronized
		log4perl.appender.Syncer.appender   = File
		log4perl.appender.Dat			 = Log::Log4perl::Appender::File
		log4perl.appender.Dat.filename  = $logdir/elsa.dat
		log4perl.appender.Dat.layout = Log::Log4perl::Layout::PatternLayout
		log4perl.appender.Dat.layout.ConversionPattern = %d{e.SSSSSS}\0%p\0%M\0%F\0%L\0%P\0%m%n\1
		log4perl.appender.SyncerDat            = Log::Log4perl::Appender::Synchronized
		log4perl.appender.SyncerDat.appender   = Dat
		log4perl.appender.RFC5424         = Log::Log4perl::Appender::Socket::UNIX
        log4perl.appender.RFC5424.Socket = $tmpdir/ops
        #log4perl.appender.RFC5424.layout = Log::Log4perl::Layout::PatternLayout::Multiline
        log4perl.appender.RFC5424.layout = CustomLog
        log4perl.appender.RFC5424.layout.ConversionPattern = 1 %d{yyyy-MM-ddTHH:mm:ss.000}Z 127.0.0.1 elsa - 99 [elsa\@32473 priority="%p" method="%M" file="%F{2}" line_number="%L" pid="%P" client="%X{client_ip_address}" qid="%X{qid}" hostname="$hostname"] %m%n
	';
	
	if (not Log::Log4perl->initialized()){
		Log::Log4perl::init( \$log_conf ) or die("Unable to init logger");
	}
	$params{log} = Log::Log4perl::get_logger('App')
	  or die("Unable to init logger");
	
	if ($params{conf}->get('db/timeout')){
		$Db_timeout = $params{conf}->get('db/timeout');
	}
	
	$params{db} = DBI->connect(
		$params{conf}->get('meta_db/dsn'),
		$params{conf}->get('meta_db/username'),
		$params{conf}->get('meta_db/password'),
		{ 
			PrintError => 0,
			HandleError => \&_dbh_error_handler,
			AutoCommit => 1,
			mysql_connect_timeout => $Db_timeout,
			mysql_auto_reconnect => 1, # we will auto-reconnect on disconnect
			mysql_local_infile => 1, # allow LOAD DATA LOCAL
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
#	foreach my $sth (grep { defined } @{$dbh->{ChildHandles}}){
#		$sth->rollback; # in case there was an active transaction
#	}
	
	throw(500, 'Internal error', { mysql => $query });
}

sub freshen_db {
	my $self = shift;
	$self->db(
		DBI->connect(
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
				mysql_local_infile => 1, # allow LOAD DATA LOCAL
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

sub _get_hash {
	my ($self, $data) = shift;
	my $digest = new Digest::HMAC_SHA1($self->conf->get('link_key'));
	$digest->add($data);
	return $digest->hexdigest();
}

sub _get_node_info {
	my $self = shift;
	my $is_lite = shift;
	my ($query, $sth);
	
	my $overall_start = time();
	my $nodes = $self->_get_nodes();
	#$self->log->trace('got nodes: ' . Dumper($nodes));
	
	my $ret = { nodes => {} };
	
	unless (scalar keys %$nodes){
		return $ret;
	}
	
	# Get indexes from all nodes in parallel
	my $cv = AnyEvent->condvar;
	$cv->begin(sub {
		$cv->send;
	});
	
	foreach my $node (keys %$nodes){
		if (exists $nodes->{$node}->{error}){
			$self->add_warning(502, 'node ' . $node . ' had error ' . $nodes->{$node}->{error}, { mysql => $node });
			delete $ret->{nodes}->{$node};
			next;
		}
		$ret->{nodes}->{$node} = {
			db => $nodes->{$node}->{db},
			#dbh => $nodes->{$node}->{dbh},
		};
		
		if ($is_lite){
			# Just get min/max times for indexes, count
			$query = sprintf('SELECT UNIX_TIMESTAMP(MIN(start)) AS start_int, UNIX_TIMESTAMP(MAX(end)) AS end_int, ' .
				'UNIX_TIMESTAMP(MAX(start)) AS start_max, SUM(records) AS records, type FROM %s.v_indexes ' .
				'WHERE type="temporary" OR (type="permanent" AND ISNULL(locked_by)) OR type="realtime"', $nodes->{$node}->{db});
			$cv->begin;
			$self->log->trace($query);
			$nodes->{$node}->{dbh}->query($query, sub {
				my ($dbh, $rows, $rv) = @_;
				
				if ($rv and $rows){
					#$self->log->trace('node returned rv: ' . $rv);
					$ret->{nodes}->{$node}->{indexes} = {
						min => $rows->[0]->{start_int} < $overall_start ? $rows->[0]->{start_int} : 0,
						max => $rows->[0]->{end_int} < $overall_start ? $rows->[0]->{end_int} : $overall_start,
						start_max => $rows->[0]->{start_max} < $overall_start ? $rows->[0]->{start_max} : 0,
						records => $rows->[0]->{records},
					};
				}
				else {
					$self->log->error('No indexes for node ' . $node . ', rv: ' . $rv);
					$ret->{nodes}->{$node}->{error} = 'No indexes for node ' . $node;
				}
				$cv->end;
			});
		}
		else {		
			# Get indexes
			$query = sprintf('SELECT CONCAT(SUBSTR(type, 1, 4), "_", id) AS name, start AS start_int, FROM_UNIXTIME(start) AS start,
			end AS end_int, FROM_UNIXTIME(end) AS end, type, last_id-first_id AS records, index_schema
			FROM %s.indexes WHERE type="temporary" OR (type="permanent" AND ISNULL(locked_by)) OR type="realtime" ORDER BY start', 
				$nodes->{$node}->{db});
			$cv->begin;
			$self->log->trace($query);
			$nodes->{$node}->{dbh}->query($query, sub {
				my ($dbh, $rows, $rv) = @_;
				
				if ($rv and $rows){
					#$self->log->trace('node returned rv: ' . $rv);
					foreach my $row (@$rows){
						$row->{schema} = decode_json(delete $row->{index_schema}) if $row->{index_schema};
					}
					$ret->{nodes}->{$node}->{indexes} = {
						indexes => $rows,
						min => $rows->[0]->{start_int} < $overall_start ? $rows->[0]->{start_int} : 0,
						max => $rows->[$#$rows]->{end_int} < $overall_start ? $rows->[0]->{end_int} : $overall_start,
						start_max => $rows->[$#$rows]->{start_int} < $overall_start ? $rows->[0]->{start_int} : 0,
					};
				}
				else {
					$self->log->error('No indexes for node ' . $node . ', rv: ' . $rv);
					$ret->{nodes}->{$node}->{error} = 'No indexes for node ' . $node;
				}
				$cv->end;
			});
		}
		
		if ($is_lite){
			# Just get min/max times, count
			$query = sprintf('SELECT UNIX_TIMESTAMP(MIN(start)) AS start_int, ' .
				'UNIX_TIMESTAMP(MIN(end)) AS end_int, SUM(max_id - min_id) AS records ' .
				'FROM %s.tables t1 JOIN table_types t2 ON (t1.table_type_id=t2.id) WHERE t2.table_type="archive"', 
				$nodes->{$node}->{db});
			$cv->begin;
			$self->log->trace($query);
			$nodes->{$node}->{dbh}->query($query, sub {
				my ($dbh, $rows, $rv) = @_;
				
				if ($rv and $rows){
					#$self->log->trace('node returned rv: ' . $rv);
					$ret->{nodes}->{$node}->{tables} = {
						min => $rows->[0]->{start_int},
						max => $rows->[0]->{end_int},
						start_max => $rows->[0]->{start_int},
						records => $rows->[0]->{records},
					};
				}
				else {
					$self->log->error('No tables for node ' . $node);
					$ret->{nodes}->{$node}->{error} = 'No tables for node ' . $node;
				}
				$cv->end;
			});
		}
		else {
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
		}
		
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
			if ($is_lite){
				$ret->{totals}->{$type} += $ret->{nodes}->{$node}->{$key}->{records};
			}
			else {
				foreach my $hash (@{ $ret->{nodes}->{$node}->{$key}->{$key} }){
					$ret->{totals}->{$type} += $hash->{records};
				}
			}
		}
		if ($min == 2**32){
			$self->log->trace('No min/max found for type min');
			$min = 0;
		}
		if ($max == 0){
			$self->log->trace('No min/max found for type max');
			$max = $overall_start;
		}
		$ret->{$type . '_min'} = $min;
		$ret->{$type . '_max'} = $max;
		$ret->{$type . '_start_max'} = $start_max;
		$self->log->trace('Found min ' . $min . ', max ' . $max . ' for type ' . $type);
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
	#$ret->{updated_for_admin} = $user->is_admin;
	$ret->{took} = (time() - $overall_start);
	
	if ($is_lite){
		foreach my $node (keys %{ $ret->{nodes} }){
			$ret->{nodes}->{$node} = {};
		}
	}
	
	if ($self->conf->get('version')){
		$ret->{version} = $self->conf->get('version');
	}
	
	$self->log->trace('get_node_info finished in ' . $ret->{took});
	
	return $ret;
}

sub _get_nodes {
	my $self = shift;
	my %nodes;
	my $node_conf = $self->conf->get('nodes');
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub { shift->send });
	my $start = Time::HiRes::time();
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
			$nodes{$node}->{dbh} = SyncMysql->new(log => $self->log, db_args => [
				'dbi:mysql:database=' . $db_name . ';host=' . $node . ';port=' . $mysql_port,  
				$node_conf->{$node}->{username}, 
				$node_conf->{$node}->{password}, 
				{
					mysql_connect_timeout => $self->db_timeout,
					PrintError => 0,
					mysql_multi_statements => 1,
				}
			]);
#			$cv->begin;
#			my $node_start = Time::HiRes::time();	
#			$nodes{$node}->{dbh} = AsyncDB->new(log => $self->log, db_args => [
#				'dbi:mysql:database=' . $db_name . ';host=' . $node . ';port=' . $mysql_port, 
#				$node_conf->{$node}->{username}, 
#				$node_conf->{$node}->{password}, 
#				{
#					mysql_connect_timeout => $self->db_timeout,
#					PrintError => 0,
#					mysql_multi_statements => 1,
#				}
#			], cb => sub {
#				$self->log->trace('connected to ' . $node . ' on ' . $mysql_port . ' in ' . (Time::HiRes::time() - $node_start));
#				$cv->end;
#			});
			
		};
		if ($@){
			$self->add_warning(502, $@, { mysql => $node });
			delete $nodes{$node};
		}		
	}
	$cv->end;
	$cv->recv;
	$self->log->trace('All connected in ' . (Time::HiRes::time() - $start) . ' seconds');
		
	return \%nodes;
}

#sub old_get_nodes {
#	my $self = shift;
#	my $user = shift;
#	my %nodes;
#	my $node_conf = $self->conf->get('nodes');
#	
#	my $mysql_port = 3306;
#	my $db_name = 'syslog';
#	foreach my $node (keys %$node_conf){
#		next unless $user->is_permitted('node_id', unpack('N*', inet_aton($node)));
#		if ($node_conf->{$node}->{port}){
#			$mysql_port = $node_conf->{$node}->{port};
#		}
#		
#		if ($node_conf->{$node}->{db}){
#			$db_name = $node_conf->{$node}->{db};
#		}
#		eval {
#			$nodes{$node} = { db => $db_name };
#			$nodes{$node}->{dbh} = SyncMysql->new(log => $self->log, db_args => [
#				'dbi:mysql:database=' . $db_name . ';host=' . $node . ';port=' . $mysql_port,  
#				$node_conf->{$node}->{username}, 
#				$node_conf->{$node}->{password}, 
#				{
#					mysql_connect_timeout => $self->db_timeout,
#					PrintError => 0,
#					mysql_multi_statements => 1,
#				}
#			]);
#		};
#		if ($@){
#			$self->log->error($@);
#			$self->add_warning($@);
#			delete $nodes{$node};
#		}
#	}
#		
#	return \%nodes;
#}

sub info {
	my $self = shift;
	
	my ($query, $sth);
	my $overall_start = time();
	
	# Execute search on every peer
	my @peers;
	foreach my $peer (keys %{ $self->conf->get('peers') }){
		push @peers, $peer;
	}
	$self->log->trace('Executing global node_info on peers ' . join(', ', @peers));
	
	my $cv = AnyEvent->condvar;
	$cv->begin;
	my %stats;
	my %results;
	foreach my $peer (@peers){
		$cv->begin;
		my $peer_conf = $self->conf->get('peers/' . $peer);
		my $url = $peer_conf->{url} . 'API/';
		$url .= ($peer eq '127.0.0.1' or $peer eq 'localhost') ? 'local_info' : 'info';
		$self->log->trace('Sending request to URL ' . $url);
		my $start = time();
		my $headers = { 
			Authorization => $self->_get_auth_header($peer),
		};
		$results{$peer} = http_get $url, headers => $headers, sub {
			my ($body, $hdr) = @_;
			eval {
				my $raw_results = $self->json->decode($body);
				$stats{$peer}->{total_request_time} = (time() - $start);
				$results{$peer} = { %$raw_results }; #undef's the guard
			};
			if ($@){
				$self->log->error($@ . "\nHeader: " . Dumper($hdr) . "\nbody: " . Dumper($body));
				$self->add_warning(502, 'peer ' . $peer . ': ' . $@, { http => $peer });
				delete $results{$peer};
			}
			$cv->end;
		};
	}
	$cv->end;
	$cv->recv;
	$stats{overall} = (time() - $overall_start);
	$self->log->debug('stats: ' . Dumper(\%stats));
	
	my $overall_final = $self->_merge_node_info(\%results);
	
	return $overall_final;
}

sub _merge_node_info {
	my ($self, $results) = @_;
	#$self->log->debug('merging: ' . Dumper($results));
	
	# Merge these results
	my $overall_final = merge values %$results;
	
	# Merge the times and counts
	my %final = (nodes => {});
	foreach my $peer (keys %$results){
		next unless $results->{$peer} and ref($results->{$peer}) eq 'HASH';
		if ($results->{$peer}->{nodes}){
			foreach my $node (keys %{ $results->{$peer}->{nodes} }){
				if ($node eq '127.0.0.1' or $node eq 'localhost'){
					$final{nodes}->{$peer} ||= $results->{$peer}->{nodes};
				}
				else {
					$final{nodes}->{$node} ||= $results->{$peer}->{nodes};
				}
			}
		}
		foreach my $key (qw(archive_min indexes_min)){
			if (not $final{$key} or $results->{$peer}->{$key} < $final{$key}){
				$final{$key} = $results->{$peer}->{$key};
			}
		}
		foreach my $key (qw(archive indexes)){
			$final{totals} ||= {};
			$final{totals}->{$key} += $results->{$peer}->{totals}->{$key};
		}
		foreach my $key (qw(archive_max indexes_max indexes_start_max archive_start_max)){
			if (not $final{$key} or $results->{$peer}->{$key} > $final{$key}){
				$final{$key} = $results->{$peer}->{$key};
			}
		}
	}
	$self->log->debug('final: ' . Dumper(\%final));
	foreach my $key (keys %final){
		$overall_final->{$key} = $final{$key};
	}
	
	return $overall_final;
}

sub _peer_query {
	my ($self, $q) = @_;
	my ($query, $sth);
	
	# Execute search on every peer
	my @peers;
	foreach my $peer (keys %{ $self->conf->get('peers') }){
		if (scalar keys %{ $q->nodes->{given} }){
			if ($q->nodes->{given}->{$peer}){
				# Normal case, fall through
			}
			elsif ($q->nodes->{given}->{ $q->peer_label }){
				# Translate the peer label to localhost
				push @peers, '127.0.0.1';
				next;
			}
			else {
				# Node not explicitly given, skipping
				next;
			}
		}
		elsif (scalar keys %{ $q->nodes->{excluded} }){
			next if $q->nodes->{excluded}->{$peer};
		}
		push @peers, $peer;
	}
	
	$self->log->trace('Executing global query on peers ' . join(', ', @peers));
	
	my $cv = AnyEvent->condvar;
	$cv->begin;
	my $headers = { 'Content-type' => 'application/x-www-form-urlencoded', 'User-Agent' => $self->user_agent_name };
	my %batches;
	foreach my $peer (@peers){
		my $peer_label = $peer;
		if (($peer eq '127.0.0.1' or $peer eq 'localhost') and $q->peer_label){
			$peer_label = $q->peer_label;
		}
		$cv->begin;
		my $peer_conf = $self->conf->get('peers/' . $peer);
		my $url = $peer_conf->{url} . 'API/';
		$url .= ($peer eq '127.0.0.1' or $peer eq 'localhost') ? 'local_query' : 'query';
		my $request_body = 'permissions=' . uri_escape($self->json->encode($q->user->permissions))
			. '&q=' . uri_escape($self->json->encode({ query_string => $q->query_string, query_meta_params => $q->meta_params }))
			. '&peer_label=' . $peer_label;
		$self->log->trace('Sending request to URL ' . $url . ' with body ' . $request_body);
		my $start = time();
		
		if ($peer_conf->{headers}){
			foreach my $header_name (keys %{ $peer_conf->{headers} }){
				$headers->{$header_name} = $peer_conf->{headers}->{$header_name};
			}
		}
		$headers->{Authorization} = $self->_get_auth_header($peer);
		$q->peer_requests->{$peer} = http_post $url, $request_body, headers => $headers, sub {
			my ($body, $hdr) = @_;
			eval {
				my $raw_results = $self->json->decode($body);
				if ($raw_results and ref($raw_results) and $raw_results->{error}){
					$self->log->error('Peer ' . $peer_label . ' got error: ' . $raw_results->{error});
					$q->add_warning(502, 'Peer ' . $peer_label . ' encountered an error.', { http => $peer });
					return;
				}
				#$self->log->debug('raw_results: ' . Dumper($raw_results));
				#my $is_groupby = ($q->has_groupby or $raw_results->{groupby});
				my $is_groupby = $raw_results->{groupby} ? 1 : 0;
				my $results_package = $is_groupby ? 'Results::Groupby' : 'Results';
				if ($q->has_groupby and ref($raw_results->{results}) ne 'HASH'){
					$self->log->error('Wrong: ' . Dumper($q->TO_JSON) . "\n" . Dumper($raw_results));
				}
				#my $results_package = ref($raw_results->{results}) eq 'ARRAY' ? 'Results' : 'Results::Groupby';
				my $results_obj = $results_package->new(results => $raw_results->{results}, 
					total_records => $raw_results->{totalRecords}, is_approximate => $raw_results->{approximate});
				if ($results_obj->records_returned and not $q->results->records_returned){
					$q->results($results_obj);
				}
				elsif ($results_obj->records_returned){
					$self->log->debug('query returned ' . $results_obj->records_returned . ' records, merging ' . Dumper($q->results) . ' with ' . Dumper($results_obj));
					$q->results->merge($results_obj, $q);
				}
				elsif ($raw_results->{batch}){
					my $current_message = $q->batch_message;
					$current_message .= $peer . ': ' . $raw_results->{batch_message};
					$q->batch_message($current_message);
					$q->batch(1);
					$batches{$peer} = $raw_results->{qid};
				}
				
				# Mark approximate if our peer results were
				if ($results_obj->is_approximate and not $q->results->is_approximate){
					$q->results->is_approximate($results_obj->is_approximate);
				}
				
				if ($raw_results->{warnings} and ref($raw_results->{warnings}) eq 'ARRAY'){
					foreach my $warning (@{ $raw_results->{warnings} }){ 
						push @{ $q->warnings }, $warning;
					}
				}
				#$q->groupby($raw_results->{groupby}) if $raw_results->{groupby};
				if ($is_groupby){
					$q->groupby($raw_results->{groupby});
				}
				else {
					$q->groupby([]);
				}
				my $stats = $raw_results->{stats};
				$stats ||= {};
				$stats->{total_request_time} = (time() - $start);
				$q->stats->{peers} ||= {};
				$q->stats->{peers}->{$peer} = { %$stats };
			};
			if ($@){
				$self->log->error($@ . 'url: ' . $url . "\nbody: " . $request_body);
				$q->add_warning(502, 'Invalid results back from peer ' . $peer_label, { http => $peer });
			}	
			delete $q->peer_requests->{$peer};
			$cv->end;
		};
	}
	$cv->end;
	$cv->recv;
	$self->log->debug('stats: ' . Dumper($q->stats));
	
	$self->log->info(sprintf("Query " . $q->qid . " returned %d rows", $q->results->records_returned));
	
	$q->time_taken(int((Time::HiRes::time() - $q->start_time) * 1000)) unless $q->livetail;
	
	if (scalar keys %batches){
		$query = 'INSERT INTO foreign_queries (qid, peer, foreign_qid) VALUES (?,?,?)';
		$sth = $self->db->prepare($query);
		foreach my $peer (sort keys %batches){
			$sth->execute($q->qid, $peer, $batches{$peer});
		}
		$self->log->trace('Updated query to have foreign_qids ' . Dumper(\%batches));
	}
	
	return $q;
}

sub _get_auth_header {
	my $self = shift;
	my $peer = shift;
	
	my $timestamp = CORE::time();
	
	my $peer_conf = $self->conf->get('peers/' . $peer);
	die('no apikey or user found for peer ' . $peer) unless $peer_conf->{username} and $peer_conf->{apikey};
	return 'ApiKey ' . $peer_conf->{username} . ':' . $timestamp . ':' . sha512_hex($timestamp . $peer_conf->{apikey});
}

sub _check_auth_header {
	my $self = shift;
	my $req = shift;
	
	my ($username, $timestamp, $apikey);
	if ($req->header('Authorization')){
		($username, $timestamp, $apikey) = $req->header('Authorization') =~ /ApiKey ([^\:]+)\:([^\:]+)\:([^\s]+)/;
	}
	
	# Authenticate via apikey
	unless ($username and $timestamp and $apikey){
		$self->log->error('No apikey given');
		return 0;
	}
	unless ($timestamp > (CORE::time() - $Auth_timestamp_grace_period)
		and $timestamp <= (CORE::time() + $Auth_timestamp_grace_period)){
		$self->log->error('timestamp is out of date');
		return 0;
	}
	if ($self->conf->get('apikeys')->{$username} 
		and sha512_hex($timestamp . $self->conf->get('apikeys')->{$username}) eq $apikey){
		$self->log->trace('Authenticated ' . $username);
		return 1;
	}
	else {
		$self->log->error('Invalid apikey: '  . $username . ':' . $timestamp . ':' . $apikey);
		return 0;
	}
}

# Helper function to convert $@ into an Ouch exception if it isn't one already
sub catch_any {
	if ($@){
		return ref($@) ? $@ : new Ouch(500, $@);
	}
}

1;
