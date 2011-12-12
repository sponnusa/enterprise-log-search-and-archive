package API;
use Moose;
with 'MooseX::Traits';
use Data::Dumper;
use Log::Log4perl;
use Config::JSON;
use JSON -convert_blessed_universally;
use Date::Manip;
use AnyEvent;
use DBI;
use Digest::HMAC_SHA1;
use MIME::Base64;
use URI::Escape;
use Socket qw(inet_aton inet_ntoa);
use Storable qw(dclone);
use Search::QueryParser;
use Net::DNS;
use Sys::Hostname::FQDN;
use String::CRC32;
use CHI;
use Time::HiRes qw(time);
use Module::Pluggable require => 1, search_path => [ qw( Export Info ) ];
use URI::Escape qw(uri_unescape);
use Mail::Internet;
use Email::LocalDelivery;
use Carp;

use AsyncMysql;

our $Default_limit = 100;
our $Max_limit = 1000;
our $Implicit_plus = 0;
our $Db_timeout = 3;

our $Field_order_to_attr = {
	0 => 'timestamp',
	100 => 'minute',
	101 => 'hour',
	102 => 'day',
	1 => 'host_id',
	2 => 'program_id',
	3 => 'class_id',
	4 => 'msg',
	5 => 'attr_i0',
	6 => 'attr_i1',
	7 => 'attr_i2',
	8 => 'attr_i3',
	9 => 'attr_i4',
	10 => 'attr_i5',
	11 => 'attr_s0',
	12 => 'attr_s1',
	13 => 'attr_s2',
	14 => 'attr_s3',
	15 => 'attr_s4',
	16 => 'attr_s5',
};

our $Field_order_to_meta_attr = {
	0 => 'timestamp',
	100 => 'minute',
	101 => 'hour',
	102 => 'day',
	1 => 'host_id',
	2 => 'program_id',
	3 => 'class_id',
	4 => 'msg',
};

our $Field_order_to_field = {
	1 => 'host',
	4 => 'msg',
	5 => 'i0',
	6 => 'i1',
	7 => 'i2',
	8 => 'i3',
	9 => 'i4',
	10 => 'i5',
	11 => 's0',
	12 => 's1',
	13 => 's2',
	14 => 's3',
	15 => 's4',
	16 => 's5',
};

our $Field_to_order = {
	'timestamp' => 0,
	'minute' => 100,
	'hour' => 101,
	'day' => 102,
	'host' => 1,
	'program' => 2,
	'class' => 3,
	'msg' => 4,
	'i0' => 5,
	'i1' => 6,
	'i2' => 7,
	'i3' => 8,
	'i4' => 9,
	'i5' => 10,
	's0' => 11,
	's1' => 12,
	's2' => 13,
	's3' => 14,
	's4' => 15,
	's5' => 16,
};

our $Proto_map = {
	'ICMP' => 1,
	'icmp' => 1,
	'TCP' => 6,
	'tcp' => 6,
	'UDP' => 17,
	'udp' => 17,
};

our $Inverse_proto_map = {
	1 => 'ICMP',
	6 => 'TCP',
	17 => 'UDP',
};

our $Time_values = {
	timestamp => 1,
	minute => 60,
	hour => 3600,
	day => 86400,
};

has 'log' => ( is => 'ro', isa => 'Log::Log4perl::Logger', required => 1 );
has 'conf' => ( is => 'ro', isa => 'Config::JSON', required => 1 );
has 'json' => (is => 'ro', isa => 'JSON', required => 1);
has 'ldap' => (is => 'rw', isa => 'Object', required => 0);
has 'db' => (is => 'rw', isa => 'Object', required => 1);
has 'last_error' => (is => 'rw', isa => 'Str', required => 1, default => '');
has 'cache' => (is => 'rw', isa => 'Object', required => 1, default => sub { return CHI->new( driver => 'RawMemory', global => 1) });
has 'warnings' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { 'add_warning' => 'push', 'clear_warnings' => 'clear' });

sub BUILDARGS {
	my ($class, %params) = @_;

	# Optionally init everything here from just a given config file
	if ($params{config_file}){
		$params{conf} = new Config::JSON ( $params{config_file} ) or die("Unable to open config file");
		my $log_level = 'DEBUG';
		if ($params{conf}->get('debug_level')){
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
		
		if ($log_level eq 'DEBUG' or $log_level eq 'TRACE'){
			$params{json} = JSON->new->pretty->allow_nonref->allow_blessed;	
		}
		else {
			$params{json} = JSON->new->allow_nonref->allow_blessed;
		}
			
		$params{db} = DBI->connect(
			$params{conf}->get('meta_db/dsn'),
			$params{conf}->get('meta_db/username'),
			$params{conf}->get('meta_db/password'),
			{ 
				PrintError => 0,
				HandleError => \&_dbh_error_handler,
				#RaiseError => 1,
				AutoCommit => 1,
				mysql_connect_timeout => $Db_timeout,
				mysql_auto_reconnect => 1, # we will auto-reconnect on disconnect
			}
		) or die($DBI::errstr);
	}
		
	return \%params;
}

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

sub BUILD {
	my $self = shift;
	
	if ( $self->conf->get('auth/method') eq 'LDAP' ) {
		require Net::LDAP::Express;
		require Net::LDAP::FilterBuilder;
		$self->ldap($self->_get_ldap());
	}
	
	if ($self->conf->get('db/timeout')){
		$Db_timeout = $self->conf->get('db/timeout');
	}
	
	# init plugins
	$self->plugins();
	
	return $self;
}

sub _get_ldap {
	my $self = shift;
	my $ldap = new Net::LDAP::Express(
		host        => $self->conf->get('ldap/host'),
		bindDN      => $self->conf->get('ldap/bindDN'),
		bindpw      => $self->conf->get('ldap/bindpw'),
		base        => $self->conf->get('ldap/base'),
		searchattrs => [ $self->conf->get('ldap/searchattrs') ],
	);
	unless ($ldap) {
		$self->log->error('Unable to connect to LDAP server');
		return;
	}
	return $ldap;
}


sub get_user_info {
	my $self = shift;
	my $username = shift;
	if ($self->conf->get('auth/method') eq 'none'){
		return {
			username => 'user',
			uid => 2,
			is_admin => 1,
			permissions => {
				class_id => {
					0 => 1,
				},
				host_id => {
					0 => 1,
				},
				program_id => {
					0 => 1,
				},
				node_id => {
					0 => 1,
				},
			},
			filter => '',
			email => $self->conf->get('email/to') ? $self->conf->get('email/to') : 'root@localhost',
		};
	}
	else {
		unless ($username){
			$self->log->error('Did not receive username');
			return 0;
		}
	}
	
	my $user_info = {
		'username'    => $username,
		'extra_attrs' => {},
		'groups' => [$username],   # all users belong to a group with their name
	};
	if ($username eq 'system'){
		$user_info->{uid} = 1;
		$user_info->{is_admin} = 1;
		$user_info->{qids} = {};
		$user_info->{permissions} = {
			class_id => {
				0 => 1,
			},
			host_id => {
				0 => 1,
			},
			program_id => {
				0 => 1,
			},
			node_id => {
				0 => 1,
			},
			filter => '',
		};
		return $user_info;
	}

	# Get the groups this user is a part of
	if ( $self->conf->get('auth/method') eq 'LDAP' ) {
		$self->ldap($self->_get_ldap());
		unless ($self->ldap) {
			$self->log->error('Unable to connect to LDAP server');
			return;
		}
		my $filter = sprintf( '(&(%s=%s))',
			$self->conf->get('ldap/searchattrs'), $username );
		my $result = $self->ldap->search( filter => $filter );
		my @entries = $result->entries();
		if ( scalar @entries > 1 ) {
			$self->log->error('Ambiguous response from LDAP server');
			return;
		}
		elsif ( scalar @entries < 1 ) {
			$self->log->error(
				'User ' . $username . ' not found in LDAP server' );
			return;
		}

		my $entry       = $entries[0];
		my $attr_map    = $self->conf->get('ldap/attr_map');
		my $extra_attrs = $self->conf->get('ldap/extra_attrs');
		ATTR_LOOP: foreach my $attr ( $entry->attributes() ) {
			$self->log->debug('checking attr: ' . $attr . ', val: ' . $entry->get_value($attr));
			foreach my $normalized_attr ( keys %{$attr_map} ) {
				if ( $attr eq $attr_map->{$normalized_attr} ) {
					$user_info->{$normalized_attr} = $entry->get_value($attr);
					next ATTR_LOOP;
				}
			}
			foreach my $normalized_attr ( keys %{$extra_attrs} ) {
				if ( $attr eq $extra_attrs->{$normalized_attr} ) {
					$user_info->{extra_attrs}->{$normalized_attr} =
					  $entry->get_value($attr);
					next ATTR_LOOP;
				}
			}
			if ( $attr eq $self->conf->get('ldap/groups_attr') ) {
				push @{ $user_info->{groups} }, $entry->get_value($attr);

				# Is the group this user is a member of a designated admin group?
				foreach my $group (@{ $user_info->{groups} }){
					if ( $self->conf->get('ldap/admin_groups')->{ $group } ){
						$self->log->debug( 'user ' . $user_info->{username} . ' is a member of admin group ' . $group );
						$user_info->{is_admin} = 1;
						next ATTR_LOOP;
					}
				}
			}
		}
	}
	elsif ($self->conf->get('auth/method') eq 'local'){
		my %in;
		while (my @arr = getgrent()){
			my @members = split(/\s+/, $arr[3]);
			if (grep { $username } @members){
				$in{$arr[0]} = 1;
			}
		}
		$self->log->debug('groups before: ' . Dumper($user_info->{groups}));
		$user_info->{groups} = [ keys %in, $username ];
		$self->log->debug('groups after: ' . Dumper($user_info->{groups}));
		# Is the group this user is a member of a designated admin group?
		foreach my $group (@{ $user_info->{groups} }){
			my @admin_groups = qw(root admin);
			if ($self->conf->get('admin_groups')){
				@admin_groups = @{ $self->conf->get('admin_groups') };
			}
			if ( grep { $user_info->{username} } @admin_groups ){
				$self->log->debug( 'user ' . $user_info->{username} . ' is an admin');
				$user_info->{is_admin} = 1;
			}
		}
		$user_info->{email} = $username . '@localhost';
	}
	elsif ($self->conf->get('auth/method') eq 'db'){
		die('No admin groups listed in admin_groups') unless $self->conf->get('admin_groups');
		my ($query, $sth);
		$query = 'SELECT groupname FROM groups t1 JOIN users_groups_map t2 ON (t1.uid=t2.uid) JOIN users t3 ON (t2.uid=t3.uid) WHERE t3.username=?';
		$sth = $self->db->prepare($query);
		$sth->execute($username);
		while (my $row = $sth->fetchrow_hashref){
			push @{ $user_info->{groups} }, $row->{groupname};
		}
		# Is the group this user is a member of a designated admin group?
		foreach my $group (@{ $user_info->{groups} }){
			if ( $self->conf->get('admin_groups')->{ $group } ){
				$self->log->debug( 'user ' . $username . ' is a member of admin group ' . $group );
				$user_info->{is_admin} = 1;
			}
		}
		$user_info->{email} = $username . '@localhost'; #TODO allow putting in an email somewhere for db auth
	}
	else {
		$self->log->error('No auth_method');
		return;
	}

	# Get the uid
	my ( $query, $sth );
	$query = 'SELECT uid FROM users WHERE username=?';
	$sth   = $self->db->prepare($query);
	$sth->execute( $user_info->{username} );
	my $row = $sth->fetchrow_hashref;
	if ($row) {
		$user_info->{uid} = $row->{uid};
	}
	else {
		# UID not found, so this is a new user and the corresponding user group,
		$self->log->debug('Creating user from : ' . Dumper($user_info));
		$user_info = $self->_create_user($user_info);
	}
	
	unless ($user_info){
		$self->log->error('Undefined user');
		return;
	}
	$self->log->debug('User info thus far: ' . Dumper($user_info));

	$user_info->{permissions} = $self->_get_permissions($user_info->{groups}, $user_info->{is_admin})
		or ($self->log->error('Unable to get permissions') and return 0);
	$self->log->debug('got permissions: ' . Dumper($user_info->{permissions}));

	# Tack on a place to store queries
	$user_info->{qids} = {};

	# Record when the session started for timeout purposes
	$user_info->{session_start_time} = time();

	return $user_info;
}


sub _get_permissions {
	my ($self, $groups, $is_admin) = @_;
	return {} unless $groups and ref($groups) eq 'ARRAY' and scalar @$groups;
	my ($query, $sth);
	
	# Find group permissions
	my %permissions;
	ATTR_LOOP: foreach my $attr qw(class_id host_id program_id node_id){
		if ($is_admin){
			$permissions{$attr} = { 0 => 1 };
			next ATTR_LOOP;
		}
		else {
			$query =
			  'SELECT DISTINCT attr_id' . "\n" .
			  'FROM groups t1' . "\n" .
			  'LEFT JOIN permissions t2 ON (t1.gid=t2.gid)' . "\n" .
			  'WHERE attr=? AND t1.groupname IN (';
			my @values = ( $attr );
			my @placeholders;
			foreach my $group ( @{ $groups } ) {
				push @placeholders ,       '?';
				push @values, $group;
			}
			$query .= join( ', ', @placeholders ) . ')';
			$sth = $self->db->prepare($query);
			$sth->execute(@values);
			my @arr;
			while (my $row = $sth->fetchrow_hashref){
				# If at any point we get a zero, that means that all are allowed, no exceptions, so bug out to the next attr loop iter
				if ($row->{attr_id} eq '0' or $row->{attr_id} eq 0){
					$permissions{$attr} = { 0 => 1 };
					next ATTR_LOOP;
				}
				push @arr, $row->{attr_id};
			}
			# Special case for program/node which defaults to allow
			foreach my $allow_attr qw(program_id node_id){
				if (scalar @arr == 0 and $attr eq $allow_attr){
					$permissions{$attr} = { 0 => 1 };
					next ATTR_LOOP;
				}
			}
			$permissions{$attr} = { map { $_ => 1 } @arr };
		}
	}
	
	# Get filters using the values/placeholders found above
	my @values;
	my @placeholders;
	foreach my $group ( @{ $groups } ) {
		push @values,       '?';
		push @placeholders, $group;
	}
	$query =
	    'SELECT filter FROM filters ' . "\n"
	  . 'JOIN groups ON (filters.gid=groups.gid) ' . "\n"
	  . 'WHERE groupname IN (';
	$query .= join( ', ', @values ) . ')';
	$sth = $self->db->prepare($query);
	$sth->execute(@placeholders);
	$permissions{filter} = '';
	while ( my $row = $sth->fetchrow_hashref ) {
		$permissions{filter} .= ' ' . $row->{filter};
	}
	
	$self->log->debug('permissions: ' . Dumper(\%permissions));
	
	return \%permissions;
	
}


sub _create_user {
	my $self = shift;
	my $user_info = shift;

	$self->log->info("Creating user $user_info->{username}");
	my ( $query, $sth );
	eval {
		$self->db->begin_work;
		$query = 'INSERT INTO users (username) VALUES (?)';
		$sth   = $self->db->prepare($query);
		$sth->execute( $user_info->{username} );
		$query = 'INSERT INTO groups (groupname) VALUES (?)';
		$sth   = $self->db->prepare($query);
		$sth->execute( $user_info->{username} );
		$query =
		    'INSERT INTO users_groups_map (uid, gid) SELECT ' . "\n"
		  . '(SELECT uid FROM users WHERE username=?),' . "\n"
		  . '(SELECT gid FROM groups WHERE groupname=?)';
		$sth = $self->db->prepare($query);
		$sth->execute( $user_info->{username}, $user_info->{username} );

		# TODO optimize this
		my $select  = 'SELECT groupname FROM groups WHERE groupname=?';
		my $sel_sth = $self->db->prepare($select);
		$query = 'INSERT INTO groups (groupname) VALUES (?)';
		$sth   = $self->db->prepare($query);
		foreach my $group ( @{ $user_info->{groups} } ) {
			$sel_sth->execute($group);
			my $row = $sel_sth->fetchrow_hashref;

			# Only do the insert if a previous entry did not exist
			unless ($row) {
				$sth->execute($group);
			}
		}

		$query = 'SELECT uid FROM users WHERE username=?';
		$sth   = $self->db->prepare($query);
		$sth->execute( $user_info->{username} );
		my $row = $sth->fetchrow_hashref;
		if ($row) {
			$user_info->{uid} = $row->{uid};
		}
		else {
			$self->log->error(
				'Unable to find uid for user ' . $user_info->{username} );
			$self->db->rollback;
			return;
		}
		
		$self->db->commit;
	};
	if ($@) {
		$self->log->error( 'Database error: ' . $@ );
		return;
	}
	return $user_info;
}

sub get_saved_result {
	my ($self, $args) = @_;
	
	unless ($args and ref($args) eq 'HASH' and $args->{qid}){
		$self->log->error('Invalid args: ' . Dumper($args));
		return;
	}
	
	# Authenticate the hash if given (so that the uid doesn't have to match)
	if ($args->{hash} and $args->{hash} ne $self->_get_hash($args->{qid}) ){
		$self->_error(q{You are not authorized to view another user's saved queries});
		return;
	}
	
	my @values = ($args->{qid});
	
	my ($query, $sth);
	$query = 'SELECT t2.uid, t2.query, milliseconds FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid)' . "\n" .
		'WHERE t1.qid=?';
	if (not $args->{hash}){
		$query .= ' AND uid=?';
		push @values, $args->{user_info}->{uid};
	}
	
	$sth = $self->db->prepare($query);
	$sth->execute(@values);
	my $row = $sth->fetchrow_hashref;
	unless ($row){
		$self->_error('No saved results for qid ' . $args->{qid} . ' found.');
		return;
	}
	my $results = {};
	$results->{totalTime} = $row->{milliseconds};
	my $saved_query = $self->json->decode($row->{query});
	foreach my $item qw(query_string query_meta_params){
		$results->{$item} = $saved_query->{$item};
	}
	
	$query = 'SELECT data FROM saved_results_data WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{qid});
	$row = $sth->fetchrow_hashref;
	$results->{results} = $self->json->decode($row->{data});
	
	return $results;
}

sub _get_hash {
	my ($self, $data) = @_;
	
	my $digest = new Digest::HMAC_SHA1($self->conf->get('link_key'));
	$digest->add($data);
	return $digest->hexdigest();
}

sub _error {
	my $self = shift;
	my $err = shift;
	$self->log->error($err);
	return $self->last_error($err);
}

sub get_permissions {
	my ($self, $args) = @_;
		
	my $form_params = $self->get_form_params();
	
	# Build programs hash
	my $programs = {};
	foreach my $class_id (keys %{ $form_params->{programs} }){
		foreach my $program_name (keys %{ $form_params->{programs}->{$class_id} }){
			$programs->{ $form_params->{programs}->{$class_id}->{$program_name} } = $program_name;
		}
	}
	
	my ($query, $sth);
	
	$query = 'SELECT t3.uid, username, t1.gid, groupname, COUNT(DISTINCT(t4.attr_id)) AS has_exceptions' . "\n" .
		'FROM groups t1' . "\n" .
		'LEFT JOIN users_groups_map t2 ON (t1.gid=t2.gid)' . "\n" .
		'LEFT JOIN users t3 ON (t2.uid=t3.uid)' . "\n" .
		'LEFT JOIN permissions t4 ON (t1.gid=t4.gid)' . "\n" .
		'GROUP BY t1.gid' . "\n" .
		'ORDER BY t1.gid ASC';
	$sth = $self->db->prepare($query);
	$sth->execute();
	my @ldap_entries;
	while (my $row = $sth->fetchrow_hashref){
		push @ldap_entries, $row;
	}
	
	$query = 'SELECT t2.groupname, t1.gid, attr, attr_id' . "\n" .
		'FROM permissions t1' . "\n" .
		'JOIN groups t2 ON (t1.gid=t2.gid)' . "\n" .
		'WHERE t1.gid=?';
	$sth = $self->db->prepare($query);
	foreach my $ldap_entry (@ldap_entries){
		$sth->execute($ldap_entry->{gid});
		my %exceptions;
		while (my $row = $sth->fetchrow_hashref){
			$self->log->debug('got row: ' . Dumper($row));
			if ($row->{attr}){
				$exceptions{ $row->{attr} } ||= {};
				if ($row->{attr} eq 'class_id'){
					$row->{attr_value} = $form_params->{classes_by_id}->{ $row->{attr_id} };
					if ($row->{attr_value}){
						$exceptions{ $row->{attr} }->{ $row->{attr_value} } = $row->{attr_id};
					}
				}
				elsif ($row->{attr} eq 'program_id'){
					$row->{attr_value} = $programs->{ $row->{attr_id} };
					if ($row->{attr_value}){
						$exceptions{ $row->{attr} }->{ $row->{attr_value} } = $row->{attr_id};
					}
				}
				elsif ($row->{attr} eq 'host_id'){
					# Must be host_id == IP or IP range
					if ($row->{attr_id} =~ /^\d+$/){
						$row->{attr_value} = inet_ntoa(pack('N*', $row->{attr_id}));
					}
					elsif ($row->{attr_id} =~ /(\d+)\s*\-\s*(\d+)/){
						$row->{attr_value} = inet_ntoa(pack('N*', $1)) . '-' . inet_ntoa(pack('N*', $2));
					}
					else {
						$self->_error('bad host: ' . Dumper($args));
						return;
					}
					$exceptions{ $row->{attr} }->{ $row->{attr_value} } = $row->{attr_id};
				}
				elsif ($row->{attr} eq 'node_id'){
					# Must be node_id == IP or IP range
					if ($row->{attr_id} =~ /^\d+$/){
						$row->{attr_value} = inet_ntoa(pack('N*', $row->{attr_id}));
					}
					elsif ($row->{attr_id} =~ /(\d+)\s*\-\s*(\d+)/){
						$row->{attr_value} = inet_ntoa(pack('N*', $1)) . '-' . inet_ntoa(pack('N*', $2));
					}
					else {
						$self->_error('bad host: ' . Dumper($args));
						return;
					}
					$exceptions{ $row->{attr} }->{ $row->{attr_value} } = $row->{attr_id};
				}
				else {
					$self->_error('unknown attr: ' . Dumper($args));
					return;
				}
				$self->log->debug('attr=' . $row->{attr} . ', attr_id=' . $row->{attr_id} . ', attr_value=' . $row->{attr_value});
			}
		}
		$ldap_entry->{_exceptions} = { %exceptions };
	}
	
	my $permissions = {
		totalRecords => scalar @ldap_entries,
		records_returned => scalar @ldap_entries,
		results => [ @ldap_entries ],	
	};
	
	$permissions->{form_params} = $form_params;
	
	return $permissions;
}

sub get_exceptions {
	my ($self, $args) = @_;
		
	my $form_params = $self->get_form_params();
		
	# NO this does not scale
#	# Build programs hash
#	my $programs = {};
#	foreach my $class_id (keys %{ $form_params->{programs} }){
#		foreach my $program_name (keys %{ $form_params->{programs}->{$class_id} }){
#			$programs->{ $form_params->{programs}->{$class_id}->{$program_name} } = $program_name;
#		}
#	}
	
	my ($query, $sth);
	
	$query = 'SELECT t2.groupname, t1.gid, attr, attr_id' . "\n" .
		'FROM permissions t1' . "\n" .
		'JOIN groups t2 ON (t1.gid=t2.gid)' . "\n" .
		'WHERE t1.gid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{gid});
	my @rows;
	while (my $row = $sth->fetchrow_hashref){
		if ($row->{attr}){
			if ($row->{attr} eq 'class_id'){
				$self->log->debug('getting class from ' . Dumper($form_params) . ' with id ' . $row->{attr_id} );
				$row->{attr_value} = $form_params->{classes_by_id}->{ $row->{attr_id} };
			}
#			elsif ($row->{attr} eq 'program_id'){
#				$row->{attr_value} = $programs->{ $row->{attr_id} };
#			}
			elsif ($row->{attr} eq 'node_id' or $row->{attr} eq 'host_id'){
				if ($row->{attr_id} =~ /^\d+$/){
					$row->{attr_value} = inet_ntoa(pack('N*', $row->{attr_id}));
				}
				elsif ($row->{attr_id} =~ /(\d+)\s*\-\s*(\d+)/){
					$row->{attr_value} = inet_ntoa(pack('N*', $1)) . '-' . inet_ntoa(pack('N*', $2));
				}
				else {
					$self->_error('bad ' . $row->{attr} . ': ' . Dumper($args));
					return;
				}
			}
			else {
				$self->_error('unknown attr: ' . $row->{attr});
				return;
			}
		}
		push @rows, $row;
	}
	my $exceptions = {
		totalRecords => scalar @rows,
		records_returned => scalar @rows,
		results => [ @rows ],	
	};
	$exceptions->{form_params} = $form_params;
	return $exceptions;
}

sub set_permissions {
	my ($self, $args) = @_;
	
	unless ($args->{action} and ($args->{action} eq 'add' or $args->{action} eq 'delete')){
		$self->_error('No set permissions action given: ' . Dumper($args));
		return;
	}
	eval { $args->{permissions} = $self->json->decode( $args->{permissions} ); };
	$self->log->debug('args: ' . Dumper($args));
	if ($@) {
		$self->_error(
			'Error decoding permissions args: ' 
			  . $@ . ' : '
			  . Dumper($args));
		return;
	}
	unless ( $args->{permissions} and ref( $args->{permissions} ) eq 'ARRAY' ) {
		$self->_error('Invalid permissions args: ' . Dumper($args));
		return;
	}
	
	my ($query, $sth);
	if ($args->{action} eq 'add'){
		$query = 'INSERT INTO permissions (gid, attr, attr_id) VALUES (?,?,?)';
	}
	elsif ($args->{action} eq 'delete'){
		$query = 'DELETE FROM permissions WHERE gid=? AND attr=? AND attr_id=?';
	}
	my $rows_updated = 0;
	$sth = $self->db->prepare($query);
	foreach my $perm (@{ $args->{permissions} }){
		$self->log->info('Changing permissions: ' . join(', ', $args->{action}, $perm->{gid}, $perm->{attr}, $perm->{attr_id}));
		$sth->execute($perm->{gid}, $perm->{attr}, $perm->{attr_id});
		$rows_updated += $sth->rows;
		if ($sth->rows){
			$self->_revalidate_group($perm->{gid});	
		}
	}
	
	return {success => $rows_updated, groups_deleted => $rows_updated};	
}

sub _revalidate_group {
	my ( $self, $gid ) = @_;
	
	my $members = $self->_get_group_members($gid);
	unless ($members and ref($members) eq 'ARRAY' and scalar @$members){
		$self->log->error('No members found for gid ' . $gid);
		return;
	}
	my ($query, $sth);
	$query = 'SELECT uid FROM users WHERE username=?';
	$sth = $self->db->prepare($query);
	my %must_revalidate;
	foreach my $member (@$members){
		$sth->execute($member);
		my $row = $sth->fetchrow_hashref;
		if ($row){
			$must_revalidate{ $row->{uid} } = 1;
			$self->log->info('User ' . $member . ' must revalidate');
		}
	}
	#TODO find and expire these sessions
}

sub _get_group_members {
	my ( $self, $gid ) = @_;
	my ($query, $sth);
	$query = 'SELECT groupname FROM groups WHERE gid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($gid);
	my $row = $sth->fetchrow_hashref;
	unless ($row){
		$self->log->error('Unknown group for gid ' . $gid);
		return;
	}
	my $group_search = $row->{groupname};
	my @ret;
	
	if ( $self->conf->get('auth/method') eq 'LDAP' ) {
		# this will be a per-org implementation
		unless ($self->ldap) {
			$self->log->error('Unable to connect to LDAP server');
			return;
		}
	
		# Whittle the group name down to just the cn
		my @filter_parts = split(/[a-zA-Z0-9]{2}\=/, $group_search);
		$self->log->debug('filter_parts: ' . Dumper(\@filter_parts));
		my $cn = $filter_parts[1];
		chop($cn); # strip the trailing comma
		unless (scalar @filter_parts > 1){
			$self->log->error('Invalid filter: ' . $group_search);
			return;
		}
		my $filter = sprintf( '(&(objectclass=group)(cn=%s))', $cn );
		$self->log->debug('filter: ' . $filter);
		my $result = $self->ldap->search( sizelimit => 2, filter => $filter );
		my @entries = $result->entries();
		$self->log->debug('entries: ' . Dumper(\@entries));
		if ( scalar @entries < 1 ) {
			$self->log->error(
				'No entries found in LDAP server:' . $self->ldap->error() );
			return;
		}
	}
	elsif ($self->conf->get('auth/method') eq 'db'){
		$query = 'SELECT username FROM users t1 JOIN users_groups_map t2 ON (t1.uid=t2.uid) WHERE t2.gid=?';
		$sth = $self->db->prepare($query);
		$sth->execute($gid);
		while (my $row = $sth->fetchrow_hashref){
			push @ret, $row->{username};
		}
	}
	else {
		$self->log->error('No auth_method');
		return;
	}
	$self->log->debug('Found members: ' . Dumper(\@ret));
	return \@ret;
}


sub get_stats {
	my ($self, $args) = @_;
	my $user = $args->{user_info};
	$self->clear_warnings;
	
	my ($query, $sth);
	my $stats = {};
	my $days_ago = 7;
	my $limit = 20;
	
	# Queries per user
	$query = 'SELECT username, COUNT(*) AS count FROM query_log t1 JOIN users t2 ON (t1.uid=t2.uid)' . "\n" .
		'WHERE timestamp > DATE_SUB(NOW(), INTERVAL ? DAY)' . "\n" .
		'GROUP BY t1.uid ORDER BY count DESC LIMIT ?';
	$sth = $self->db->prepare($query);
	$sth->execute($days_ago, $limit);
	$stats->{queries_per_user} = { x => [], User => [] };
	while (my $row = $sth->fetchrow_hashref){
		push @{ $stats->{queries_per_user}->{x} }, $row->{username};
		push @{ $stats->{queries_per_user}->{User} }, $row->{count};
	}
	
	# General query stats
	$query = 'SELECT DATE_FORMAT(timestamp, "%Y-%m-%d") AS x, COUNT(*) AS Count, AVG(milliseconds) AS Avg_Time, ' . "\n" .
		'SUM(num_results) AS Results, AVG(num_results) AS Avg_Results' . "\n" .
		'FROM query_log WHERE timestamp > DATE_SUB(NOW(), INTERVAL ? DAY) GROUP BY x LIMIT ?';
	$sth = $self->db->prepare($query);
	$sth->execute($days_ago, $limit);
	$stats->{query_stats} = { x => [], Count => [], Avg_Time => [], Avg_Results => [] };
	while (my $row = $sth->fetchrow_hashref){
		foreach my $col (keys %{ $stats->{query_stats} }){
			push @{ $stats->{query_stats}->{$col} }, $row->{$col};
		}
	}
	
	$stats->{nodes} = $self->_get_nodes();
		
	my $intervals = 100;
	if ($args->{intervals}){
		$intervals = sprintf('%d', $args->{intervals});
	}
	
	#TODO figure out why a cv out here per-node does not work
	foreach my $node (keys %{ $stats->{nodes} }){
		next unless $stats->{nodes}->{$node}->{dbh};
		# Get load stats
		my $load_stats = {};
		
		my $cv = AnyEvent->condvar;
		$cv->begin(sub { shift->send });
		foreach my $item qw(load archive index){
			$load_stats->{$item} = {
				data => {
					x => [],
					LogsPerSec => [],
					KBytesPerSec => [],
				},
			};
			
			
						
			$query = 'SELECT MIN(bytes) AS min_bytes, AVG(bytes) AS avg_bytes, MAX(bytes) AS max_bytes,' . "\n" .
				'MIN(count) AS min_count, AVG(count) AS avg_count, MAX(count) AS max_count,' . "\n" .
				'UNIX_TIMESTAMP(MAX(timestamp))-UNIX_TIMESTAMP(MIN(timestamp)) AS total_time, UNIX_TIMESTAMP(MIN(timestamp)) AS earliest' . "\n" .
				'FROM stats WHERE type=? AND timestamp BETWEEN ? AND ?';
			
			$cv->begin;
			unless ($stats->{nodes}->{$node}->{dbh}){
				$self->log->warn('no dbh for node ' . $node . ':' . Dumper($stats->{nodes}->{$node}->{dbh}));
			}
			$stats->{nodes}->{$node}->{dbh}->query($query, sub {
					my ($dbh, $rows, $rv) = @_;
					$self->log->trace('got stat for node ' . $node . ': ' . Dumper($rows));
					$load_stats->{$item}->{summary} = $rows->[0];
					$cv->end;
				},
				$item, $args->{start}, $args->{end});
			
			$query = 'SELECT UNIX_TIMESTAMP(timestamp) AS ts, timestamp, bytes, count FROM stats WHERE type=? AND timestamp BETWEEN ? AND ?';
			$cv->begin;
			$stats->{nodes}->{$node}->{dbh}->query($query, sub {
					my ($dbh, $rows, $rv) = @_;
					return unless $intervals;
					# arrange in the number of buckets requested
					my $bucket_size = ($load_stats->{$item}->{summary}->{total_time} / $intervals);
					return unless $bucket_size;
					foreach my $row (@$rows){
						my $ts = $row->{ts} - $load_stats->{$item}->{summary}->{earliest};
						my $bucket = int(($ts - ($ts % $bucket_size)) / $bucket_size);
						# Sanity check the bucket because too large an index array can cause an OoM error
						if ($bucket > $intervals){
							die('Bucket ' . $bucket . ' with bucket_size ' . $bucket_size . ' and ts ' . $row->{ts} . ' was greater than intervals ' . $intervals);
						}
						unless ($load_stats->{$item}->{data}->{x}->[$bucket]){
							$load_stats->{$item}->{data}->{x}->[$bucket] = $row->{timestamp};
						}
						
						unless ($load_stats->{$item}->{data}->{LogsPerSec}->[$bucket]){
							$load_stats->{$item}->{data}->{LogsPerSec}->[$bucket] = 0;
						}
						$load_stats->{$item}->{data}->{LogsPerSec}->[$bucket] += ($row->{count} / $bucket_size);
						
						unless ($load_stats->{$item}->{data}->{KBytesPerSec}->[$bucket]){
							$load_stats->{$item}->{data}->{KBytesPerSec}->[$bucket] = 0;
						}
						$load_stats->{$item}->{data}->{KBytesPerSec}->[$bucket] += ($row->{bytes} / 1024 / $bucket_size);
					}
					$cv->end;
				},
				$item, $args->{start}, $args->{end});
			
			
		}
		$cv->end;
		$cv->recv;	
		$self->log->trace('here');
		$stats->{nodes}->{$node} = $load_stats;
	}
	
	$self->log->trace('received');
		
	# Combine the stats info for the nodes
	my $combined = {};
	$self->log->debug('got stats: ' . Dumper($stats->{nodes}));
	
	foreach my $stat qw(load index archive){
		$combined->{$stat} = { x => [], LogsPerSec => [], KBytesPerSec => [] };
		foreach my $node (keys %{ $stats->{nodes} }){
			if ($stats->{nodes}->{$node} and $stats->{nodes}->{$node}->{$stat}){ 
				my $load_data = $stats->{nodes}->{$node}->{$stat}->{data};
				next unless $load_data;
				for (my $i = 0; $i < (scalar @{ $load_data->{x} }); $i++){
					next unless $load_data->{x}->[$i];
					unless ($combined->{$stat}->{x}->[$i]){
						$combined->{$stat}->{x}->[$i] = $load_data->{x}->[$i];
					}
					$combined->{$stat}->{LogsPerSec}->[$i] += $load_data->{LogsPerSec}->[$i];
					$combined->{$stat}->{KBytesPerSec}->[$i] += $load_data->{KBytesPerSec}->[$i];
				}
			}	
		}
	}
	$stats->{combined_load_stats} = $combined;
		
	$self->log->debug('got stats: ' . Dumper($stats));
	return $stats;
}

sub _get_nodes {
	my $self = shift;
	my %nodes;
	my $node_conf = $self->conf->get('nodes');
	
	my $db_name = 'syslog';
	foreach my $node (keys %$node_conf){
		if ($node_conf->{$node}->{db}){
			$db_name = $node_conf->{$node}->{db};
		}
		$nodes{$node} = { db => $db_name };
		$nodes{$node}->{dbh} = AsyncMysql->new(log => $self->log, db_args => [
			'dbi:mysql:database=' . $db_name . ';host=' . $node, 
			$node_conf->{$node}->{username}, 
			$node_conf->{$node}->{password}, 
			{
				mysql_connect_timeout => $Db_timeout,
				PrintError => 0,
				mysql_multi_statements => 1,
			}
		]);
	}
		
	return \%nodes;
}

sub old_get_nodes {
	my $self = shift;
	my %nodes;
	my $node_conf = $self->conf->get('nodes');
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub { shift->send; });
	foreach my $node (keys %$node_conf){
		$cv->begin;
		my $db_name = 'syslog';
		if ($node_conf->{$node}->{db}){
			$db_name = $node_conf->{$node}->{db};
		}
		$nodes{$node} = { db => $db_name };
		$nodes{$node}->{dbh} = AnyEvent::DBI->new('dbi:mysql:database=' . $db_name . ';host=' . $node, 
			$node_conf->{$node}->{username}, $node_conf->{$node}->{password}, 
			mysql_connect_timeout => $Db_timeout,
			PrintError => 0,
			mysql_multi_statements => 1, 
			on_error => sub {
				my ($dbh, $filename, $line, $fatal) = @_;
				my $err = $@;
				chomp($err);
				$err .= ': ' . $filename . ' ' . $line;
				$self->log->error($err);
				$nodes{$node}->{error} = $err;
			},
			on_connect => sub {
				my ($dbh, $success) = @_;
				if ($success){
					$self->log->trace('connected to ' . $node);
					$cv->end;
				}
				else {
					my $err = 'unable to connect to ' . $node . ': ' . $@;
					$self->log->error($err);
					$nodes{$node}->{error} = $err;
					delete $nodes{$node}->{dbh};
					$cv->end;
				}
			});	
	}
	$cv->end;
	$cv->recv;
		
	return \%nodes;
}


sub _get_sphinx_nodes {
	my $self = shift;
	my $args = shift;
	my %nodes;
	my $node_conf = $self->conf->get('nodes');
	
	foreach my $node (keys %$node_conf){
		if ($args->{given_nodes}){
			next unless $args->{given_nodes}->{$node};
		}
		if ($args->{excluded_nodes}){
			next if $args->{excluded_nodes}->{$node};
		}
		my $db_name = 'syslog';
		if ($node_conf->{$node}->{db}){
			$db_name = $node_conf->{$node}->{db};
		}
		my $sphinx_port = 3307;
		if ($node_conf->{$node}->{sphinx_port}){
			$sphinx_port = $node_conf->{$node}->{sphinx_port};
		}
		$nodes{$node} = { db => $db_name };
								
		$nodes{$node}->{dbh} = AsyncMysql->new(log => $self->log, db_args => [
			'dbi:mysql:database=' . $db_name . ';host=' . $node, 
			$node_conf->{$node}->{username}, 
			$node_conf->{$node}->{password}, 
			{
				mysql_connect_timeout => $Db_timeout,
				PrintError => 0,
				mysql_multi_statements => 1,
			}
		]);
		
		$self->log->trace('connecting to sphinx on node ' . $node);
		
		$nodes{$node}->{sphinx} = AsyncMysql->new(log => $self->log, db_args => [
			'dbi:mysql:port=' . $sphinx_port .';host=' . $node, undef, undef,
			{
				mysql_connect_timeout => $Db_timeout,
				PrintError => 0,
				mysql_multi_statements => 1,
				mysql_bind_type_guessing => 1,
			}
		]);		
	}
	
	return \%nodes;
}

sub old_get_sphinx_nodes {
	my $self = shift;
	my $args = shift;
	my %nodes;
	my $node_conf = $self->conf->get('nodes');
	
	my $cv = AnyEvent->condvar;
	
	$cv->begin(sub { shift->send });
	foreach my $node (keys %$node_conf){
		if ($args->{given_nodes}){
			next unless $args->{given_nodes}->{$node};
		}
		if ($args->{excluded_nodes}){
			next if $args->{excluded_nodes}->{$node};
		}
		my $db_name = 'syslog';
		if ($node_conf->{$node}->{db}){
			$db_name = $node_conf->{$node}->{db};
		}
		my $sphinx_port = 3307;
		if ($node_conf->{$node}->{sphinx_port}){
			$sphinx_port = $node_conf->{$node}->{sphinx_port};
		}
		$nodes{$node} = { db => $db_name };
								
		$cv->begin;
		$nodes{$node}->{dbh} = AnyEvent::DBI->new('dbi:mysql:database=' . $db_name . ';host=' . $node, 
			$node_conf->{$node}->{username}, $node_conf->{$node}->{password}, 
			PrintError => 0, 
			mysql_connect_timeout => $Db_timeout,
			mysql_multi_statements => 1, 
			on_error => sub {
				my ($dbh, $filename, $line, $fatal) = @_;
				my $err = $@;
				chomp($err);
				$err .= ': ' . $filename . ' ' . $line;
				$self->log->error($err);
				$nodes{$node}->{error} = $err;
				$self->last_error($err);
			},
			on_connect => sub {
				my ($dbh, $success) = @_;
				if ($success){
					$self->log->trace('connected to ' . $node);
					$cv->end;
				}
				else {
					my $err = 'unable to connect to ' . $node . ': ' . $@;
					$self->log->error($err);
					$nodes{$node}->{error} = $err;
					delete $nodes{$node}->{dbh};
					$cv->end;
				}
		});
		
		$self->log->trace('connecting to sphinx on node ' . $node);
		
		$cv->begin;
		$nodes{$node}->{sphinx} = AnyEvent::DBI->new('dbi:mysql:port=' . $sphinx_port .';host=' . $node, undef, undef, 
			PrintError => 0, 
			mysql_connect_timeout => $Db_timeout,
			mysql_multi_statements => 1, 
			mysql_bind_type_guessing => 1, 
			on_error => sub {
				my ($dbh, $filename, $line, $fatal) = @_;
				my $err = $@;
				chomp($err);
				$err .= ': ' . $filename . ' ' . $line;
				$self->log->error($err);
				$nodes{$node}->{error} = $err;
				$self->last_error($err);
			},
			on_connect => sub {
				my ($dbh, $success) = @_;
				if ($success){
					$self->log->trace('connected to ' . $node);
					$cv->end;
				}
				else {
					my $err = 'unable to connect to ' . $node . ': ' . $@;
					$self->log->error($err);
					$nodes{$node}->{error} = $err;
					delete $nodes{$node}->{dbh};
					$cv->end;
				}
			});
		
	}
	$cv->end;
	
	$cv->recv;
	
	return \%nodes;
}


sub _get_node_info {
	my $self = shift;
	my ($query, $sth);
	
	my $nodes = $self->_get_nodes();
	$self->log->trace('got nodes: ' . Dumper($nodes));
		
	my $ret = { nodes => {} };
	
	# Get indexes from all nodes in parallel
	my $cv = AnyEvent->condvar;
	$cv->begin(sub {
		$cv->send;
	});
	
	foreach my $node (keys %$nodes){
#		if (exists $nodes->{$node}->{error}){
#			$self->add_warning('node ' . $node . ' had error ' . $nodes->{$node}->{error});
#			delete $ret->{nodes}->{$node};
#			#$ret->{nodes}->{$node}->{error} = $nodes->{$node}->{error};
#			next;
#		}
		$ret->{nodes}->{$node} = {
			db => $nodes->{$node}->{db},
			dbh => $nodes->{$node}->{dbh},
		};
		
		
		# Get indexes
		$query = sprintf('SELECT CONCAT(SUBSTR(type, 1, 4), "_", id) AS name, start, UNIX_TIMESTAMP(start) AS start_int, end, UNIX_TIMESTAMP(end) AS end_int, type, records FROM %s.v_indexes ORDER BY start', 
			$nodes->{$node}->{db});
		$cv->begin;
		$self->log->trace($query);
		$nodes->{$node}->{dbh}->query($query, sub {
				my ($dbh, $rows, $rv) = @_;
				
				if ($rv and $rows){
					$self->log->trace('node returned rv: ' . $rv);
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
			'UNIX_TIMESTAMP(end) AS end_int, table_type, min_id, max_id ' .
			'FROM %s.tables t1 JOIN table_types t2 ON (t1.table_type_id=t2.id) ORDER BY start', 
			$nodes->{$node}->{db});
		$cv->begin;
		$self->log->trace($query);
		$nodes->{$node}->{dbh}->query($query, sub {
				my ($dbh, $rows, $rv) = @_;
				
				if ($rv and $rows){
					$self->log->trace('node returned rv: ' . $rv);
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
	
	$cv->recv;
	
	# Find min/max indexes
	my $min = 2**32;
	my $max = 0;
	my $start_max = 0;
	foreach my $node (keys %{ $ret->{nodes} }){
		if ($ret->{nodes}->{$node}->{indexes}->{min} < $min){
			$min = $ret->{nodes}->{$node}->{indexes}->{min};
		}
		if ($ret->{nodes}->{$node}->{indexes}->{max} > $max){
			$max = $ret->{nodes}->{$node}->{indexes}->{max};
			$start_max = $ret->{nodes}->{$node}->{indexes}->{start_max};
		}
	}
	$ret->{min} = $min;
	$ret->{max} = $max;
	$ret->{start_max} = $start_max;
	$self->log->trace('Found min ' . $min . ', max ' . $max);
	
	# Find unique classes;
	$ret->{classes} = {};
	$ret->{classes_by_id} = {};
	foreach my $node (keys %{ $ret->{nodes} }){
		foreach my $class_id (keys %{ $ret->{nodes}->{$node}->{classes} }){
			$ret->{classes_by_id}->{$class_id} = $ret->{nodes}->{$node}->{classes}->{$class_id};
			$ret->{classes}->{ $ret->{nodes}->{$node}->{classes}->{$class_id} } = $class_id;
		}
	}
	
	# Find unique fields
	foreach my $node (keys %{ $ret->{nodes} }){
		FIELD_LOOP: foreach my $field_hash (@{ $ret->{nodes}->{$node}->{fields} }){
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
		$ret->{fields_by_order}->{ $field_hash->{class_id} } ||= {};
		$ret->{fields_by_order}->{ $field_hash->{class_id} }->{ $field_hash->{field_order} } = $field_hash;
	}
	
	# Find fields by arranged by short field name
	$ret->{fields_by_name} = {};
	foreach my $field_hash (@{ $ret->{fields} }){
		$ret->{fields_by_name}->{ $field_hash->{value} } ||= [];
		push @{ $ret->{fields_by_name}->{ $field_hash->{value} } }, $field_hash;
	}
	
	# Find fields by type
	$ret->{fields_by_type} = {};
	foreach my $field_hash (@{ $ret->{fields} }){
		$ret->{fields_by_type}->{ $field_hash->{field_type} } ||= {};
		$ret->{fields_by_type}->{ $field_hash->{field_type} }->{ $field_hash->{value} } ||= [];
		push @{ $ret->{fields_by_type}->{ $field_hash->{field_type} }->{ $field_hash->{value} } }, $field_hash;
	}
	
	return $ret;
}


sub get_form_params {
	my ( $self, $args) = @_;
	
	my $node_info = $self->_get_node_info();
	#$self->log->trace('got node_info: ' . Dumper($node_info));
	
	my $form_params = {
		start => _epoch2iso($node_info->{min}),
		start_int => $node_info->{min},
		display_start_int => $node_info->{min},
		end => _epoch2iso($node_info->{max}),
		end_int => $node_info->{max},
		classes => $node_info->{classes},
		classes_by_id => $node_info->{classes_by_id},
		fields => $node_info->{fields},
		nodes => [ keys %{ $node_info->{nodes} } ],
	};
	
	# You can change the default start time displayed to web users by changing this config setting
	if ($self->conf->get('default_start_time_offset')){
		$form_params->{display_start_time} = ($node_info->{max} - (86400 * $self->conf->get('default_start_time_offset')));
	}
	
	
	if ($args and $args->{permissions}){
		# this is for a user, restrict what gets sent back
		unless ($args->{permissions}->{class_id}->{0}){
			foreach my $class_id (keys %{ $form_params->{classes} }){
				unless ($args->{permissions}->{class_id}->{$class_id}){
					delete $form_params->{classes}->{$class_id};
				}
			}
		
			my $possible_fields = [ @{ $form_params->{fields} } ];
			$form_params->{fields} = [];
			for (my $i = 0; $i < scalar @$possible_fields; $i++){
				my $field_hash = $possible_fields->[$i];
				my $class_id = $field_hash->{class_id};
				if ($args->{permissions}->{class_id}->{$class_id}){
					push @{ $form_params->{fields} }, $field_hash;
				}
			}
		}
	}
	
	# Tack on the "ALL" and "NONE" special types
	unshift @{$form_params->{fields}}, 
		{'value' => 'ALL', 'text' => 'ALL', 'class_id' => 0 }, 
		{'value' => 'NONE', 'text' => 'NONE', 'class_id' => 1 };
	
	$form_params->{schedule_actions} = $self->get_schedule_actions();
	
	return $form_params;
}

sub get_schedule_actions {
	my ($self, $args) = @_;
	
	my ($query, $sth);
	$query = 'SELECT action_id, action FROM query_schedule_actions';
	$sth = $self->db->prepare($query);
	$sth->execute();
	my @ret;
	while (my $row = $sth->fetchrow_hashref){
		push @ret, $row;
	}
	return \@ret;
}

sub get_scheduled_queries {
	my ($self, $args) = @_;
	
	if ($args and ref($args) ne 'HASH'){
		$self->_error('Invalid args: ' . Dumper($args));
		return;
	}
	elsif (not $args){
		$args = {};
	}
	
	my $offset = 0;
	if ( $args->{startIndex} ){
		$offset = sprintf('%d', $args->{startIndex});
	}
	my $limit = 10;
	if ( $args->{results} ) {
		$limit = sprintf( "%d", $args->{results} );
	}
	
	my ($query, $sth);
	
	$query = 'SELECT COUNT(*) AS totalRecords FROM query_schedule' . "\n" .
		'WHERE uid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{user_info}->{uid});
	my $row = $sth->fetchrow_hashref;
	my $totalRecords = $row->{totalRecords};
	
	$query = 'SELECT t1.id, query, frequency, start, end, action, action_params, enabled, UNIX_TIMESTAMP(last_alert) As last_alert, alert_threshold' . "\n" .
		'FROM query_schedule t1' . "\n" .
		'JOIN query_schedule_actions t2 ON (t1.action_id=t2.action_id)' . "\n" .
		'WHERE uid=?' . "\n" .
		'ORDER BY t1.id DESC' . "\n" .
		'LIMIT ?,?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{user_info}->{uid}, $offset, $limit);
	my @rows;
	while (my $row = $sth->fetchrow_hashref){
		push @rows, $row;
	}
	my $ret = {
		'results' => [ @rows ],
		'totalRecords' => $totalRecords,
		'recordsReturned' => scalar @rows,
	};
	return $ret;
}

sub _epoch2iso {
	my $epochdate = shift;
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($epochdate);
	my $date = sprintf("%04d-%02d-%02d %02d:%02d:%02d", 
		$year + 1900, $mon + 1, $mday, $hour, $min, $sec);
	return $date;
}

sub schedule_query {
	my ($self, $args) = @_;
	
	foreach my $item qw(qid days time_unit action_id){	
		unless (defined $args->{$item}){
			$self->_error('Invalid args, missing arg: ' . $item);
			return;
		}
	}
	
	# Make sure these params are ints
	foreach my $item qw(qid days time_unit count action_id){
		next unless $args->{$item};
		$args->{$item} = sprintf('%d', $args->{$item});
	}
	$args->{uid} = sprintf('%d', $args->{user_info}->{uid});
	
	my %standard_vars = map { $_ => 1 } qw(uid qid days time_unit count action_id threshold_count threshold_time_unit);
	my $schedule_query_params = { action_params => {} };
	foreach my $item (keys %{$args}){
		if ($standard_vars{$item}){
			$schedule_query_params->{$item} = $args->{$item};
		}
		else {
			$schedule_query_params->{action_params}->{$item} = $args->{$item};
		}
	}
	$schedule_query_params->{action_params} = $self->json->encode($schedule_query_params->{action_params});
		
	my @frequency;
	for (my $i = 1; $i <= 7; $i++){
		if ($i eq $schedule_query_params->{time_unit}){
			$frequency[$i-1] = 1;
		}
		else {
			$frequency[$i-1] = 0;
		}
	}
	my $freq_str = join(':', @frequency);
	$self->log->debug('freq_str: ' . $freq_str);
	
	my ($query, $sth);
	$query = 'INSERT INTO query_schedule (uid, query, frequency, start, end, action_id, action_params, last_alert, alert_threshold) VALUES (?, ' . "\n" .
		'(SELECT query FROM query_log WHERE qid=?), ?, ?, ?, ?, ?, "1970-01-01 00:00:00", ?)';
	$sth = $self->db->prepare($query);
	my $days = $schedule_query_params->{days};
	unless ($days){
		$days = 2^32;
	}
	my $alert_threshold = 0;
	my $time_unit_map = {
		1 => (60 * 60 * 24 * 365),
		2 => (60 * 60 * 24 * 30),
		3 => (60 * 60 * 24 * 7),
		4 => (60 * 60 * 24),
		5 => (60 * 60),
		6 => (60),
	};
	if ($schedule_query_params->{threshold_count} and $schedule_query_params->{threshold_time_unit}){
		$alert_threshold = $time_unit_map->{ $schedule_query_params->{threshold_time_unit} } * $schedule_query_params->{threshold_count};
	}
	$sth->execute($schedule_query_params->{uid}, $schedule_query_params->{qid}, $freq_str, time(), (86400 * $days) + time(), 
		$schedule_query_params->{action_id}, $schedule_query_params->{action_params}, $alert_threshold);
	my $ok = $sth->rows;
	
	return $ok;
}

sub delete_saved_results {
	my ($self, $args) = @_;
	$self->log->debug('args: ' . Dumper($args));
	unless ($args->{qid}){
		$self->_error('Invalid args, no qid: ' . Dumper($args));
		return;
	}
	my ($query, $sth);
	# Verify this query belongs to the user
	$query = 'SELECT uid FROM query_log WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{qid});
	my $row = $sth->fetchrow_hashref;
	unless ($row){
		$self->_error('Invalid args, no results found for qid: ' . Dumper($args));
		return;
	}
	unless ($row->{uid} eq $args->{user_info}->{uid} or $args->{user_info}->{is_admin}){
		$self->_error('Unable to alter these saved results based on your authorization: ' . Dumper($args));
		return;
	}
	$query = 'DELETE FROM saved_results WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{qid});
	if ($sth->rows){
		return {deleted => $sth->rows};
	}
	else {
		$self->_error('Query ID ' . $args->{qid} . ' not found!');
	}
}

sub delete_scheduled_query {
	my ($self, $args) = @_;
	$self->log->debug('args: ' . Dumper($args));
	unless ($args->{id}){
		$self->_error('Invalid args, no id: ' . Dumper($args));
		return;
	}
	my ($query, $sth);
	$query = 'DELETE FROM query_schedule WHERE uid=? AND id=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{user_info}->{uid}, $args->{id});
	if ($sth->rows){
		return {deleted => $sth->rows}
	}
	else {
		$self->_error('Schedule ID ' . $args->{id} . ' not found!');
	}
}

sub update_scheduled_query {
	my ($self, $args) = @_;
	$self->log->debug('args: ' . Dumper($args));
	unless ($args->{id}){
		$self->_error('Invalid args, no id: ' . Dumper($args));
		return;
	}
	my $attr_map = {};
	foreach my $item qw(query frequency start end action action_params enabled alert_threshold){
		$attr_map->{$item} = $item;
	}
	my ($query, $sth);
	my $new_args = {};
	foreach my $given_arg (keys %{ $args }){
		next if $given_arg eq 'id' or $given_arg eq 'user_info';
		unless ($attr_map->{$given_arg}){
			$self->_error('Invalid arg: ' . $given_arg);
			return;
		}
		# Chop quotes
		$args->{$given_arg} =~ s/^['"](.+)['"]$/$1/;
		
		# Adjust timestamps if necessary
		if ($given_arg eq 'start' or $given_arg eq 'end'){
			$args->{$given_arg} = UnixDate($args->{$given_arg}, '%s');
		}
		
		# Convert action to action_id
		if ($given_arg eq 'action'){
			$given_arg = 'action_id';
			$args->{'action_id'} = delete $args->{'action'};
		}
		
		$self->log->debug('given_arg: ' . $given_arg . ': ' . $args->{$given_arg});
		$query = sprintf('UPDATE query_schedule SET %s=? WHERE id=?', $given_arg);
		$sth = $self->db->prepare($query);
		$sth->execute($args->{$given_arg}, $args->{id});
		$new_args->{$given_arg} = $args->{$given_arg};
	}
	
	return $new_args;
}

sub save_results {
	my ($self, $args) = @_;
	$self->log->debug(Dumper($args));
	my $comments = $args->{comments};
	eval {
		$args = $self->json->decode($args->{results});
	};
	if ($@){
		$self->_error($@);
		return;
	}
	unless ($args->{qid} and $args->{results} and ref($args->{results})){
		$self->_error('Invalid args: ' . Dumper($args));
		return;
	}
	
	$self->log->debug('got results to save: ' . Dumper($args));
		
	my $meta_info = { results => [] };
		
	unless (ref($args->{results})) {
		$self->log->info('No results for query');
		$self->_error('No results to save');
		return 0;
	}
	
	$meta_info->{results} = $args->{results};
	$meta_info->{comments} = $comments;
	$meta_info->{qid} = $args->{qid};

	$self->_save_results($meta_info);
}

sub _save_results {
	my ($self, $args) = @_;
	
	my ($query, $sth);
	
	$self->db->begin_work;
	$query = 'INSERT INTO saved_results (qid, comments) VALUES(?,?)';
	$sth = $self->db->prepare($query);
	
	$sth->execute($args->{qid}, $args->{comments});
	$query = 'INSERT INTO saved_results_data (qid, data) VALUES (?,?)';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{qid}, $self->json->encode($args->{results}));
		
	$self->db->commit;
	
	return 1;
}


sub get_saved_queries {
	my ($self, $args) = @_;
	
	if ($args and ref($args) ne 'HASH'){
		$self->_error('Invalid args: ' . Dumper($args));
		return;
	}
	elsif (not $args){
		$args = {};
	}
	
	my $offset = 0;
	if ( $args->{startIndex} ){
		$offset = sprintf('%d', $args->{startIndex});
	}
	my $limit = 10;
	if ( $args->{results} ) {
		$limit = sprintf( "%d", $args->{results} );
	}
	
	my $uid = $args->{user_info}->{uid};
	if ($args->{uid}){
		$uid = sprintf('%d', $args->{uid});
	}
	if ($uid ne $args->{user_info}->{uid} and not $args->{user_info}->{is_admin}){
		$self->_error(q{You are not authorized to view another user's saved queries});
		return;	
	}
	
	
	my $saved_queries;
	if ($args->{qid} and not ($args->{startIndex} or $args->{results})){
		# We're just getting one known query
		$saved_queries = $self->_get_saved_query(sprintf('%d', $args->{qid}));
	}
	else {
		$saved_queries = $self->_get_saved_queries($uid, $offset, $limit);
	}
	

	$self->log->debug( "saved_queries: " . Dumper($saved_queries) );
	return $saved_queries;
}

sub _get_saved_query {
	my ($self, $qid) = @_;
	
	my ( $query, $sth, $row );
	
	$query = 'SELECT t1.qid, t2.query, comments' . "\n" .
			'FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid)' . "\n" .
			'WHERE t2.qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($qid);
	
	return $sth->fetchrow_hashref or {error => 'QID ' . $qid . ' not found.'};
}

sub _get_saved_queries {
	my ( $self, $uid, $offset, $limit ) = @_;
	$limit = 100 unless $limit;

	my ( $query, $sth, $row );
	
	# First find total number
	$query =
	    'SELECT COUNT(*) AS totalRecords ' . "\n"
	  . 'FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid)' . "\n"
	  . 'WHERE uid=?'; #AND comments!=\'_alert\'';
	$sth = $self->db->prepare($query) or die( $self->db->errstr );
	$sth->execute( $uid );
	$row = $sth->fetchrow_hashref;
	my $totalRecords = $row->{totalRecords} ? $row->{totalRecords} : 0;

	# Find our type of database and use the appropriate query
	my $db_type = $self->db->get_info(17);    #17 == SQL_DBMS_NAME
	if ( $db_type =~ /Microsoft SQL Server/ ) {
		# In MS-SQL, we don't have the niceties of OFFSET, so we have to do this via subqueries
		my $outer_top = $offset + $limit;
		$query = 'SELECT * FROM ' . "\n" .
			'(SELECT TOP ? * FROM ' . "\n" .
			'(SELECT TOP ? t1.qid, t2.query, comments FROM ' . "\n" .
			'FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid) ' . "\n" .
		  	'WHERE uid=? ' . "\n" .
		  	'ORDER BY qid DESC) OverallTop ' . "\n" .
		  	'ORDER BY qid ASC) TopOfTop ' . "\n" .
		  	'ORDER BY qid DESC';
		$sth = $self->db->prepare($query) or die( $self->db->errstr );
		$sth->execute($limit, ($offset + $limit), $uid);  
	}
	else {
		$query =
		    'SELECT t1.qid, t2.query, comments, num_results, UNIX_TIMESTAMP(timestamp) AS timestamp ' . "\n"
		  . 'FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid) ' . "\n"
		  . 'WHERE uid=?' . "\n"
		  . 'ORDER BY qid DESC LIMIT ?,?';
		$sth = $self->db->prepare($query) or die( $self->db->errstr );
		$sth->execute( $uid, $offset, $limit );
	}

	my $queries = [];    # only save the latest unique query
	while ( my $row = $sth->fetchrow_hashref ) {
		# we have to decode this to make sure it doesn't end up as a string
		my $decode;
		eval { 
			$decode = $self->json->decode($row->{query}); 
		};
		
		my $query = $decode->{query_string};
		push @{$queries}, {
			qid => $row->{qid},
			timestamp => $row->{timestamp},
			query => $query, 
			num_results => $row->{num_results}, 
			comments => $row->{comments},
			hash => $self->_get_hash($row->{qid}),
		};
	}
	return { 
		totalRecords => $totalRecords,
		recordsReturned => scalar @$queries,
		results => [ @{$queries} ] 
	};
}

sub get_previous_queries {
	my ($self, $args) = @_;
	
	my $offset = 0;
	if ( $args->{startIndex} ){
		$offset = sprintf('%d', $args->{startIndex});
	}
	my $limit = $self->conf->get('previous_queries_limit');
	if ( $args->{results} ) {
		$limit = sprintf( "%d", $args->{results} );
	}
	my $dir = 'DESC';
	if ( $args->{dir} and $args->{dir} eq 'asc'){
		$dir = 'ASC';
	}
	my $uid = $args->{user_info}->{uid};
	
	my ( $query, $sth, $row );
	
	# First find total number
	$query =
	    'SELECT COUNT(*) AS totalRecords ' . "\n"
	  . 'FROM query_log ' . "\n"
	  . 'WHERE uid=?';
	$sth = $self->db->prepare($query) or die( $self->db->errstr );
	$sth->execute( $uid );
	$row = $sth->fetchrow_hashref;
	my $totalRecords = $row->{totalRecords} ? $row->{totalRecords} : 0;

	# Find our type of database and use the appropriate query
	my $db_type = $self->db->get_info(17);    #17 == SQL_DBMS_NAME
	if ( $db_type =~ /Microsoft SQL Server/ ) {
		# In MS-SQL, we don't have the niceties of OFFSET, so we have to do this via subqueries
		my $outer_top = $offset + $limit;
		$query = 'SELECT * FROM ' . "\n" .
			'(SELECT TOP ? qid, query, timestamp, num_results, milliseconds FROM ' . "\n" .
			'(SELECT TOP ? qid, query, timestamp, num_results, milliseconds FROM ' . "\n" .
			'FROM query_log ' . "\n" .
		  	'WHERE uid=?' . "\n" .
		  	'ORDER BY qid DESC) OverallTop ' . "\n" .
		  	'ORDER BY qid ASC) TopOfTop ' . "\n" .
		  	'ORDER BY qid DESC';
		$sth = $self->db->prepare($query) or die( $self->db->errstr );
		$sth->execute($limit, ($offset + $limit), $uid);
	}
	else {
		$query =
		    'SELECT qid, query, timestamp, num_results, milliseconds ' . "\n"
		  . 'FROM query_log ' . "\n"
		  . 'WHERE uid=? AND system=0' . "\n"
		  . 'ORDER BY qid ' . $dir . ' LIMIT ?,?';
		$sth = $self->db->prepare($query) or die( $self->db->errstr );
		$sth->execute( $uid, $offset, $limit );
	}

	my $queries = [];    # only save the latest unique query
	while ( my $row = $sth->fetchrow_hashref ) {
		if ( $row->{query} ) {

			# we have to decode this to make sure it doesn't end up as a string
			my $prev_query = $self->json->decode( $row->{query} );
			if (    $prev_query
				and ref($prev_query) eq 'HASH'
				and $prev_query->{query_string} )
			{
				push @{$queries},
				  {
					qid          => $row->{qid},
					query        => $prev_query->{query_string},
					query_obj    => $prev_query,
					timestamp    => $row->{timestamp},
					num_results  => $row->{num_results},
					milliseconds => $row->{milliseconds},
				  };

			}
		}
	}
	return { 
		totalRecords => $totalRecords,
		recordsReturned => scalar @$queries,
		results => [ @{$queries} ] 
	};
}

sub get_query_auto_complete {
	my ($self, $args) = @_;
	
	if ( not $args->{query} ) {
		$self->_error("No query specified");
		return;
	}

	my $limit = $self->conf->get('previous_queries_limit');
	if ( $args->{limit} ) {
		$limit = sprintf( "%d", $args->{limit} );
	}
	$limit = 100 unless $limit;
	
	
	my ( $query, $sth );
	my $like = q/%"query_string":"/ . $args->{query} . '%';
	
	# Find our type of database and use the appropriate query
	my $db_type = $self->db->get_info(17);    #17 == SQL_DBMS_NAME
	if ( $db_type =~ /Microsoft SQL Server/ ) {
		$query =
		    'SELECT TOP ' 
		  . $limit
		  . ' qid, query, timestamp, num_results, milliseconds ' . "\n"
		  . 'FROM query_log ' . "\n"
		  . 'WHERE uid=? AND query LIKE ?' . "\n"
		  . 'ORDER BY qid DESC';
		$sth = $self->db->prepare($query) or die( $self->db->errstr );
		$sth->execute( $args->{user_info}->{uid}, $like );
	}
	else {
		$query =
		    'SELECT qid, query, timestamp, num_results, milliseconds ' . "\n"
		  . 'FROM query_log ' . "\n"
		  . 'WHERE uid=? AND query LIKE ? ' . "\n"
		  . 'ORDER BY qid DESC LIMIT ?';
		$self->log->trace('like: ' . $like);
		$sth = $self->db->prepare($query) or die( $self->db->errstr );
		$sth->execute( $args->{user_info}->{uid}, $like, $limit );
	}

	my $queries = {};    # only save the latest unique query
	while ( my $row = $sth->fetchrow_hashref ) {
		if ( $row->{query} ) {
			my $prev_query = $self->json->decode( $row->{query} );
			if (    $prev_query
				and ref($prev_query) eq 'HASH'
				and $prev_query->{query_string} )
			{
				unless (
					    $queries->{ $prev_query->{query_string} }
					and $queries->{ $prev_query->{query_string} }->{timestamp}
					cmp    # stored date is older
					$row->{timestamp} < 0
				  )
				{
					$queries->{ $prev_query->{query_string} } = {
						qid          => $row->{qid},
						query        => $prev_query->{query_string},
						timestamp    => $row->{timestamp},
						num_results  => $row->{num_results},
						milliseconds => $row->{milliseconds},
					};
				}
			}
		}
	}
	return { results => [ values %{$queries} ] };
}


sub _get_host_info {
	my ( $self, $heap, $ip ) = @_;
	unless ($self->conf->get('inventory/dsn')){
		$self->log->error('No inventory db');
		return;
	}
	my $dbh = DBI->connect($self->conf->get('inventory/dsn'), 
		$self->conf->get('inventory/username'), 
		$self->conf->get('inventory/password'));
	unless ($dbh){
		$self->log->error('Invalid inventory db');
		return;
	}
	my ($query, $sth);
	$query = 'SELECT * FROM ' . $self->conf->get('inventory/table') 
		. ' WHERE ' . $self->conf->get('inventory/ip_column') .
		'=?';
	$self->log->debug('query: ' . $query);
	$sth = $dbh->prepare($query);
	$sth->execute($ip);
	return $sth->fetchrow_hashref;
}

sub set_permissions_exception {
	my ($self, $args) = @_;
	
	unless ($args->{action} and ($args->{action} eq 'add' or $args->{action} eq 'delete')){
		$self->_error('Invalid args, missing action: ' . Dumper($args));
		return;
	}
	
	eval { $args->{exception} = $self->json->decode( $args->{exception} ); };
	$self->log->debug('args: ' . Dumper($args));
	if ($@) {
		$self->_error(
			'Error decoding permissions args: ' 
			  . $@ . ' : '
			  . Dumper($args));
		return;
	}
	unless ( $args->{exception} and ref( $args->{exception} ) eq 'HASH' ) {
		$self->_error('Invalid permissions args: ' . Dumper($args));
		return;
	}
	
	my ($query, $sth);
	
	# we need to massage inbound hostnames
	if ($args->{exception}->{attr} eq 'host_id' or $args->{exception}->{attr} eq 'node_id'){
		if ($args->{exception}->{attr_id} =~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/){
			$args->{exception}->{attr_id} = unpack('N*', inet_aton($args->{exception}->{attr_id}));
		}
		elsif ($args->{exception}->{attr_id} =~ /(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s*\-\s*(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/){
			$args->{exception}->{attr_id} = unpack('N*', inet_aton($1)) . '-' . unpack('N*', inet_aton($2));
			$self->log->debug('attr_id: ' . $args->{exception}->{attr_id} . ', 1: ' . $1 . ', 2:' . $2);
		}
		else {
			$self->_error('Invalid permissions args, bad host: ' . Dumper($args));
			return;
		}
	}
	
	if ($args->{action} eq 'add'){
		$query = 'INSERT INTO permissions (gid, attr, attr_id) VALUES(?,?,?)';
		$sth = $self->db->prepare($query);
		$sth->execute(
			$args->{exception}->{gid}, 
			$args->{exception}->{attr}, 
			$args->{exception}->{attr_id});
	}
	elsif ($args->{action} eq 'delete') {
		$query = 'DELETE FROM permissions WHERE gid=? AND attr_id=?';
		$sth = $self->db->prepare($query);
		$sth->execute(
			$args->{exception}->{gid}, 
			$args->{exception}->{attr_id});
	}
	my $ret;
	if ($sth->rows){
		$ret = { success => $sth->rows };
		#TODO
		#$kernel->yield('_revalidate_group', $args->{exception}->{gid});		
	}
	else {
		return { error => 'Database was not altered with args ' . Dumper($args) };
	}
}


sub get_running_archive_query {
	my ($self, $args) = @_;
	
	my ($query, $sth);
	$query = 'SELECT qid, query FROM query_log WHERE uid=? AND archive=1 AND ISNULL(num_results)';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{user_info}->{uid});
	my $row = $sth->fetchrow_hashref;
	if ($row){
		my $query_params = $self->json->decode($row->{query});
		 return { qid => $row->{qid}, query => $query_params->{query_string} };
	}
	else {
		 return {qid => 0};
	}
}

sub query {
	my ($self, $args) = @_;
	$self->clear_warnings;
	unless ($args and ref($args) eq 'HASH'){
		die('Invalid query args');
	}
	$args->{start_time} = Time::HiRes::time();
	my ($query, $sth);
	
	if ($args->{q} ) {
		# JSON-encoded query from web
		my $decode = $self->json->decode($args->{q});
		# query_params should contain query_string and query_meta_params
		$self->log->debug( "Decoded as : " . Dumper($decode) );
		$args->{query_meta_params} = $decode->{query_meta_params};
		$args->{query_string} = $decode->{query_string};
		if ($args->{query_meta_params}->{groupby}){
			$args->{groupby} = $args->{query_meta_params}->{groupby}
		}
		if ($args->{query_meta_params}->{timeout}){
			$args->{timeout} = sprintf("%d", ($args->{query_meta_params}->{timeout} * 1000)); #time is in milleseconds
		}
		if ($args->{query_meta_params}->{archive_query}){
			$args->{archive_query} = $args->{query_meta_params}->{archive_query}
		}
	}
	$self->log->trace('args: ' . Dumper($args));
	
	my $ret = { query_string => $args->{query_string}, query_meta_params => $args->{query_meta_params} };	
	
	unless ($args->{query_string} ){
		$self->_error('Invalid args: ' . Dumper($args));
		return;
	}
		
	my $is_system = 0;
	# Is this a system-initiated query?
	if ($args->{query_schedule_id}){
		$is_system = 1;
	}
	elsif ($args->{user_info}->{username} eq 'system'){
		$is_system = 1;
	}
	
	my $is_archive = $args->{archive_query} ? 1 : 0;
	if ($is_archive){
		# Check to see if this user is already running an archive query
		$query = 'SELECT qid, uid FROM query_log WHERE archive=1 AND ISNULL(num_results)';
		$sth = $self->db->prepare($query);
		$sth->execute();
		my $counter = 0;
		while (my $row = $sth->fetchrow_hashref){
			if ($row->{uid} eq $args->{user_info}->{uid}){
				$self->_error('User ' . $args->{user_info}->{username} . ' already has an archive query running: ' . $row->{qid});
				return;
			}
			$counter++;
			if ($counter >= $self->conf->get('max_concurrent_archive_queries')){
				#TODO create a queuing mechanism for this
				$self->_error('There are already ' . $self->conf->get('max_concurrent_archive_queries') . ' queries running');
				return;
			}
		}
	}

	# Log the query
	$self->db->begin_work;
	$query = 'INSERT INTO query_log (uid, query, system, archive) VALUES (?, ?, ?, ?)';
	$sth   = $self->db->prepare($query);
	$sth->execute( $args->{user_info}->{uid}, 
		$args->{q} ? $args->{q} : $self->json->encode($ret), 
		$is_system, $is_archive );
	$query = 'SELECT MAX(qid) AS qid FROM query_log';
	$sth   = $self->db->prepare($query);
	$sth->execute();
	my $row = $sth->fetchrow_hashref;
	my $qid = $row->{qid};
	$self->db->commit;

	$self->log->debug( "Received query with qid $qid at " . time() );
	$ret->{qid} = $qid;
	
	# Set some sane defaults		
	$args->{limit} ||= $Default_limit;
	$args->{offset} ||= 0;
	$args->{timeout} ||= sprintf("%d", ($self->conf->get('query_timeout') * 1000));

	$self->log->trace("Using timeout of $args->{timeout}");
		
	if ($is_archive){
		# Cron job will pickup the query from the query log and execute it from here if it's an archive query.
		$ret->{batch_query} = $qid;
	}		
	else {
		# Actually perform the query
		
		# Parse our query
		$args->{node_info} = $self->cache->get('node_info');
		unless ($args->{node_info}){
			$args->{node_info} = $self->_get_node_info();
			$self->cache->set('node_info', $args->{node_info}, $self->conf->get('sphinx/index_interval'));
		}
		
		$self->_parse_query_string($args);
		
		# Find highlights to inform the web client
		my $highlights = {};	
		foreach my $boolean qw(and or){
			foreach my $class_id (keys %{ $args->{field_terms}->{$boolean} }){
				foreach my $field_name (keys %{ $args->{field_terms}->{$boolean}->{$class_id} }){
					foreach my $term (@{ $args->{field_terms}->{$boolean}->{$class_id}->{$field_name} }){
						my $regex = $term;
						$regex =~ s/^\s{2,}/\ /;
						$regex =~ s/\s{2,}$/\ /;
						$regex =~ s/\s/\./g;
						$highlights->{$regex} = 1;
					}
				}
			}
			foreach my $term (sort keys %{ $args->{any_field_terms}->{$boolean} }){
				my $regex = $term;
				$regex =~ s/^\s{2,}/\ /;
				$regex =~ s/\s{2,}$/\ /;
				$regex =~ s/\s/\./g;
				$highlights->{$regex} = 1;
			}
		}
		
		$ret->{highlights} = { %$highlights };
		
		# Execute search
		$self->_sphinx_query($args);
		$ret->{results} = $args->{results};
		
		$self->log->info(sprintf("Query $qid returned %d rows", $args->{recordsReturned}));
			
		$ret->{hash} = $self->_get_hash($qid); #tack on the hash for permalinking on the frontend
		$ret->{totalTime} = int(
			(Time::HiRes::time() - $args->{start_time}) * 1000
		);
		
		# Update the db to ack
		$query = 'UPDATE query_log SET num_results=?, milliseconds=? '
		  		. 'WHERE qid=?';
		$sth = $self->db->prepare($query);
		
		$ret->{totalRecords} = $args->{totalRecords};
		$ret->{recordsReturned} = $args->{recordsReturned};
		if ($args->{groupby}){
			$ret->{groupby} = $args->{groupby};
		}
		$sth->execute( $ret->{recordsReturned}, $ret->{totalTime}, $qid );
	}
	
	$ret->{errors} = $self->warnings;
	return $ret;
}

sub get_log_info {
	my ($self, $args) = @_;
	my $user = $args->{user_info};
	
	my $decode;
	eval {
		$decode = $self->json->decode(decode_base64($args->{q}));
	};
	if ($@){
		$self->_error('Invalid JSON args: ' . Dumper($args) . ': ' . $@);
		return;
	}
	
	unless ($decode and ref($decode) eq 'HASH'){
		$self->_error('Invalid args: ' . Dumper($decode));
		return;
	}
	$self->log->trace('decode: ' . Dumper($decode));
	
	my $data;
	
	unless ($decode->{class} and $self->conf->get('plugins/' . $decode->{class})){
		# Check to see if there is generic IP information for use with pcap
		if ($self->conf->get('pcap_url')){
			my %ip_fields = ( srcip => 1, dstip => 1, ip => 1);
			foreach my $field (keys %$decode){
				if ($ip_fields{$field}){
					my $plugin = Info::Pcap->new(conf => $self->conf, data => $decode);
					return  { summary => $plugin->summary, urls => $plugin->urls, plugins => $plugin->plugins };
				}
			}
		}
		$self->log->debug('no plugins for class ' . $decode->{class});
		$data =  { summary => 'No info.', urls => [], plugins => [] };
		return $data;
	}
	
	eval {
		my $plugin = $self->conf->get('plugins/' . $decode->{class})->new(conf => $self->conf, data => $decode);
		$data =  { summary => $plugin->summary, urls => $plugin->urls, plugins => $plugin->plugins };
	};
	if ($@){
		my $e = $@;
		$self->_error('Error creating plugin ' . $self->conf->get('plugins/' . $decode->{class}) . ': ' . $e);
		return;
	}
		
	unless ($data){
		$self->_error('Unable to find info from args: ' . Dumper($decode));
		return;
	}
		
	return $data;
}

sub _sphinx_query {
	my ($self, $args) = @_;
	
	$self->_build_sphinx_query($args);
	
	my $nodes = $self->_get_sphinx_nodes($args);
	my $ret = {};
	my $overall_start = time();
	foreach my $node (keys %{ $nodes }){
		if (exists $nodes->{$node}->{error}){
			my $err_str = 'not using node ' . $node . ' because ' . $nodes->{$node}->{error};
			$self->add_warning($err_str);
			$self->log->warn($err_str);
			delete $nodes->{$node};
		}
	}
	
	# Get indexes from all nodes in parallel
	my $cv = AnyEvent->condvar;
	$cv->begin(sub {
		$cv->send;
	});
	
	foreach my $node (keys %$nodes){
		next unless $self->_is_permitted($args, 'node_id', unpack('N*', inet_aton($node)));
		$ret->{$node} = {};
		my $node_info = $args->{node_info}->{nodes}->{$node};
		# Prune indexes
		my @index_arr;
		foreach my $index (@{ $node_info->{indexes}->{indexes} }){
			if ($args->{start_int} and $args->{end_int}){
				if (
					($args->{start_int} >= $index->{start_int} and $args->{start_int} <= $index->{end_int})
					or ($args->{end_int} >= $index->{start_int} and $args->{end_int} <= $index->{end_int})
					or ($args->{start_int} <= $index->{start_int} and $args->{end_int} >= $index->{end_int})
					or ($index->{start_int} <= $args->{start_int} and $index->{end_int} >= $args->{end_int})
				){
					push @index_arr, $index->{name};
				}
			}
			else {
				push @index_arr, $index->{name};
			}
		}	
		my $indexes = join(', ', @index_arr);
		unless ($indexes){
			$self->log->debug('no indexes for node ' . $node);
			next;
		}
		
		eval {
			my @multi_queries;
			my @multi_values;
			my $start = time();
			foreach my $query (@{ $args->{queries} }){
				my $search_query = 'SELECT *, ' . $query->{select} . ' FROM ' . $indexes . ' WHERE ' . $query->{where};
				if (exists $query->{groupby}){
					$search_query .= ' GROUP BY ' . $query->{groupby};
				}
				$search_query .= ' LIMIT ?,? OPTION ranker=none';
				push @multi_values, @{ $query->{values } }, $args->{offset}, $args->{limit};
				$self->log->debug('sphinx_query: ' . $search_query . ', values: ' . 
					Dumper($query->{values}));
				push @multi_queries, $search_query;
			}
			
			$self->log->trace('multiquery: ' . join(';', @multi_queries));
			$self->log->trace('values: ' . join(',', @multi_values));
			$cv->begin;
			$nodes->{$node}->{sphinx}->sphinx(join(';', @multi_queries) . ';SHOW META', sub { 
				$self->log->debug('Sphinx query for node ' . $node . ' finished in ' . (time() - $start));
				my ($dbh, $result, $rv) = @_;
				if (not $rv){
					my $e = 'node ' . $node . ' got error ' .  Dumper($result);
					$self->log->error($e);
					$self->add_warning($e);
					$cv->end;
					return;
				}
				my $rows = $result->{rows};
				$self->log->trace('node ' . $node . ' got sphinx result: ' . Dumper($result));
				$ret->{$node}->{sphinx_rows} = $rows;
				$ret->{$node}->{meta} = $result->{meta};
				
				$self->log->trace('$ret->{$node}->{meta}: ' . Dumper($ret->{$node}->{meta}));
				
				# Find what tables we need to query to resolve rows
				my %tables;
				ROW_LOOP: foreach my $row (@$rows){
					foreach my $table_hash (@{ $args->{node_info}->{nodes}->{$node}->{tables}->{tables} }){
						next unless $table_hash->{table_type} eq 'index';
						if ($table_hash->{min_id} >= $row->{id} or $row->{id} <= $table_hash->{max_id}){
							$tables{ $table_hash->{table_name} } ||= [];
							push @{ $tables{ $table_hash->{table_name} } }, $row->{id};
							next ROW_LOOP;
						}
					}
				}
				
				# Go get the actual rows from the dbh
				foreach my $table (sort keys %tables){
					my $placeholders = join(',', map { '?' } @{ $tables{$table} });
					my $table_query = sprintf("SELECT main.id,\n" .
						"DATE_FORMAT(FROM_UNIXTIME(timestamp), \"%%Y/%%m/%%d %%H:%%i:%%s\") AS timestamp,\n" .
						"INET_NTOA(host_id) AS host, program, class_id, class, msg,\n" .
						"i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5\n" .
						"FROM %1\$s main\n" .
						"LEFT JOIN %2\$s.programs ON main.program_id=programs.id\n" .
						"LEFT JOIN %2\$s.classes ON main.class_id=classes.id\n" .
						' WHERE main.id IN (' . $placeholders . ')',
						$table, $nodes->{$node}->{db});
					$self->log->trace('table query for node ' . $node . ': ' . $table_query 
						. ', placeholders: ' . join(',', @{ $tables{$table} }));
					$cv->begin;
					$nodes->{$node}->{dbh}->query($table_query, 
						sub { 
							my ($dbh, $rows, $rv) = @_;
							if (not $rv){
								my $errstr = 'node ' . $node . ' got error ' . $rows;
								$self->log->error($errstr);
								$self->add_warning($errstr);
								$cv->end;
								return;
							}
							$self->log->trace('node '. $node . ' got db rows: ' . (scalar @$rows));
							foreach my $row (@$rows){
								$ret->{$node}->{results} ||= {};
								$row->{node} = $node;
								$row->{node_id} = unpack('N*', inet_aton($node));
								$ret->{$node}->{results}->{ $row->{id} } = $row;
							}
							$cv->end;
						}, 
						@{ $tables{$table} });
				}
				$cv->end; #end sphinx query
			}, @multi_values);
		};
		if ($@){
			$ret->{$node}->{error} = 'sphinx query error: ' . $@;
			$self->log->error('sphinx query error: ' . $@);
			$cv->end;
		}
	}
	$cv->end; # bookend initial begin
	$cv->recv; # block until all of the above completes
	
	$args->{totalRecords} = 0;
	if (exists $args->{groupby}){
		$args->{results} = {};
		foreach my $groupby (@{ $args->{groupby} }){
			my %agg;
			foreach my $node (sort keys %$ret){
				# One-off for grouping by node
				if ($groupby eq 'node'){
					$agg{$node} = $ret->{$node}->{meta}->{total_found};
					next;
				}
				foreach my $sphinx_row (@{ $ret->{$node}->{sphinx_rows} }){
					# Resolve the @groupby col with the mysql col
					unless (exists $ret->{$node}->{results}->{ $sphinx_row->{id} }){
						$self->log->warn('mysql row for sphinx id ' . $sphinx_row->{id} . ' did not exist');
						next;
					}
					my $key;
					if (exists $Time_values->{ $groupby }){
						# We will resolve later
						$key = $sphinx_row->{'@groupby'};
					}
					elsif ($groupby eq 'program'){
						$key = $ret->{$node}->{results}->{ $sphinx_row->{id} }->{program};
					}
					elsif ($groupby eq 'class'){
						$key = $ret->{$node}->{results}->{ $sphinx_row->{id} }->{class};
					}
					elsif (exists $Field_to_order->{ $groupby }){
						# Resolve normally
						$key = $self->_resolve_value($args, $sphinx_row->{class_id}, 
							$sphinx_row->{'@groupby'}, $Field_to_order->{ $groupby });
					}
					else {
						# Resolve with the mysql row
						my $field_order = $self->_get_field($args, $groupby)->{ $sphinx_row->{class_id} }->{field_order};
						#$self->log->trace('resolving with row ' . Dumper($ret->{$node}->{results}->{ $sphinx_row->{id} }));
						$key = $ret->{$node}->{results}->{ $sphinx_row->{id} }->{ $Field_order_to_field->{$field_order} };
						$key = $self->_resolve_value($args, $sphinx_row->{class_id}, $key, $field_order);
						$self->log->trace('field_order: ' . $field_order . ' key ' . $key);
					}
					$agg{ $key } += $sphinx_row->{'@count'};	
				}
			}
			if (exists $Time_values->{ $groupby }){
				# Sort these in ascending label order
				my @tmp;
				foreach my $key (sort { $a <=> $b } keys %agg){
					$args->{totalRecords} += $agg{$key};
					my $unixtime = ($key * $Time_values->{ $groupby });
										
					$self->log->trace('key: ' . $key . ', tv: ' . $Time_values->{ $groupby } . 
						', unixtime: ' . $unixtime . ', localtime: ' . (scalar localtime($unixtime)));
					push @tmp, { 
						intval => $unixtime, 
						'@groupby' => $self->_resolve_value($args, 0, $key, $Field_to_order->{ $groupby }), 
						'@count' => $agg{$key}
					};
				}
				
				# Fill in zeroes for missing data so the graph looks right
				my @zero_filled;
				my $increment = $Time_values->{ $groupby };
				$self->log->trace('using increment ' . $increment . ' for time value ' . $groupby);
				OUTER: for (my $i = 0; $i < @tmp; $i++){
					push @zero_filled, $tmp[$i];
					if (exists $tmp[$i+1]){
						for (my $j = $tmp[$i]->{intval} + $increment; $j < $tmp[$i+1]->{intval}; $j += $increment){
							#$self->log->trace('i: ' . $tmp[$i]->{intval} . ', j: ' . ($tmp[$i]->{intval} + $increment) . ', next: ' . $tmp[$i+1]->{intval});
							push @zero_filled, { 
								'@groupby' => _epoch2iso($j), 
								intval => $j,
								'@count' => 0
							};
							last OUTER if scalar @zero_filled > $args->{limit};
						}
					}
				}
				$args->{results}->{$groupby} = [ @zero_filled ];
			}
			else { 
				# Sort these in descending value order
				my @tmp;
				foreach my $key (sort { $agg{$b} <=> $agg{$a} } keys %agg){
					$args->{totalRecords} += $agg{$key};
					push @tmp, { intval => $agg{$key}, '@groupby' => $key, '@count' => $agg{$key} };
					last if scalar @tmp > $args->{limit};
				}
				$args->{results}->{$groupby} = [ @tmp ];
			}
			$args->{recordsReturned} += scalar keys %agg;
		}	
	}
	else {
		my @tmp;
		foreach my $node (keys %$ret){
			$args->{totalRecords} += $ret->{$node}->{meta}->{total_found};
			foreach my $id (sort { $a <=> $b } keys %{ $ret->{$node}->{results} }){
				my $row = $ret->{$node}->{results}->{$id};
				$row->{_fields} = [
						{ field => 'host', value => $row->{host}, class => 'any' },
						{ field => 'program', value => $row->{program}, class => 'any' },
						{ field => 'class', value => $row->{class}, class => 'any' },
					];
				# Resolve column names for fields
				foreach my $col qw(i0 i1 i2 i3 i4 i5 s0 s1 s2 s3 s4 s5){
					my $value = delete $row->{$col};
					# Swap the generic name with the specific field name for this class
					my $field = $args->{node_info}->{fields_by_order}->{ $row->{class_id} }->{ $Field_to_order->{$col} }->{value};
					if (defined $value and $field){
						# See if we need to apply a conversion
						$value = $self->_resolve_value($args, $row->{class_id}, $value, $Field_to_order->{$col});
						push @{ $row->{_fields} }, { 'field' => $field, 'value' => $value, 'class' => $args->{node_info}->{classes_by_id}->{ $row->{class_id} } };
					}
				}
				push @tmp, $row;
			}
		}
		# Trim to just the limit asked for
		$args->{results} = [];
		foreach my $row (sort { $a->{timestamp} cmp $b->{timestamp} } @tmp){
			push @{ $args->{results} }, $row;
			last if scalar @{ $args->{results} } >= $args->{limit};
		}
		$args->{recordsReturned} = scalar @{ $args->{results} };
	}
	
	$self->log->debug('completed query in ' . (time() - $overall_start) . ' with ' . $args->{recordsReturned} . ' rows');
	
	return 1;
}

sub _get_field {
	my $self = shift;
	my $args = shift;
	my $raw_field = shift;
		
	# Account for FQDN fields which come with the class name
	my ($class, $field) = split(/\./, $raw_field);
	
	if ($field){
		# We were given an FQDN, so there is only one class this can be
		foreach my $field_hash (@{ $args->{node_info}->{fields} }){
			if ($field_hash->{fqdn_field} eq $raw_field){
				return { $args->{node_info}->{classes}->{$class} => $field_hash };
			}
		}
	} 
	
	# Was not FQDN
	$field = $raw_field;
	$class = 0;
	my %fields;
		
	# Could also be a meta-field/attribute
	if (defined $Field_to_order->{$field}){
		$fields{$class} = { 
			value => $field, 
			text => uc($field), 
			field_id => $Field_to_order->{$field},
			class_id => $class, 
			field_order => $Field_to_order->{$field}
		};
	}
		
	foreach my $row (@{ $args->{node_info}->{fields} }){
		if ($row->{value} eq $field){
			$fields{ $row->{class_id} } = $row;
		}
	}
	
	return \%fields;
}

sub _parse_query_string {
	my ($self, $args) = @_;
	#$self->log->trace('parsing query string from args: ' . Dumper($args));
	
	my $raw_query = $args->{query_string};
	
	my $stopwords = $self->conf->get('stopwords');
	$args->{given_classes} = {};
	$args->{excluded_classes} = {};
	$args->{distinct_classes} = {};
	$args->{permitted_classes} = {};
	
	# Attach the query filters for this user from permissions
	my $filtered_raw_query = $raw_query;
	if ($args->{user_info}->{permissions}->{filter}){
		$filtered_raw_query .= ' ' . $args->{user_info}->{permissions}->{filter};
	}
	
	# Check to see if the class was given in meta params
	if ($args->{query_meta_params}->{class}){
		$args->{given_classes}->{ sprintf("%d", $args->{node_info}->{classes}->{ uc($args->{query_meta_params}->{class}) }) } = 1;
	}
	
	# If no class was given anywhere, see if we can divine it from a groupby
	if (not scalar keys %{ $args->{given_classes} }){
		if (exists $args->{groupby}){
			foreach my $field (@{ $args->{groupby} }){
				# Special case for node
				next if $field eq 'node';
				my $field_infos = $self->_get_field($args, $field);
				foreach my $class_id (keys %{$field_infos}){
					$args->{given_classes}->{$class_id} = 1;
				}
			}
		}
	}
		
	# Check for meta limit
	if ($args->{query_meta_params}->{limit}){
		$args->{limit} = sprintf("%d", $args->{query_meta_params}->{limit});
		$self->log->debug("Set limit " . $args->{limit});
	}
	
	foreach my $type qw(field_terms attr_terms){
		foreach my $boolean qw(and or not){
			$args->{$type}->{$boolean} = {};
		}
	}
	foreach my $boolean qw(and or not){
		$args->{any_field_terms}->{$boolean} = [];
	}
		
	if ($raw_query =~ /\S/){ # could be meta_attr-only
		my $qp = new Search::QueryParser(rxTerm => qr/[^\s()]+/, rxField => qr/[\w,\.]+/);
		my $orig_parsed_query = $qp->parse($filtered_raw_query, $Implicit_plus) or die($qp->err);
		$self->log->debug("orig_parsed_query: " . Dumper($orig_parsed_query));
		
		my $parsed_query = dclone($orig_parsed_query); #dclone so recursion doesn't mess up original
		
		# Recursively parse the query terms
		$self->_parse_query_term($args, $parsed_query);
	}
	else {
		die('No query terms given');
	}
	
	# One-off for dealing with hosts as fields
	foreach my $boolean qw(and or not){
		foreach my $op (keys %{ $args->{attr_terms}->{$boolean} }){
			if ($args->{attr_terms}->{$boolean}->{$op}->{host} 
				and $args->{attr_terms}->{$boolean}->{$op}->{host}->{0}
				and $args->{attr_terms}->{$boolean}->{$op}->{host}->{0}->{host_id}){
				foreach my $host_int (@{ $args->{attr_terms}->{$boolean}->{$op}->{host}->{0}->{host_id} }){
					if ($self->_is_permitted($args, 'host_id', $host_int)){
						$self->log->trace('adding host_int ' . $host_int);
						push @{ $args->{any_field_terms}->{$boolean} }, '(@host ' . $host_int . ')';
					}
					else {
						die "Insufficient permissions to query host_int $host_int";
					}
				}
			}
		}
	}

	$self->log->debug('attr before conversion: ' . Dumper($args->{attr_terms}));
	
	# Check for blanket allow on classes
	if ($args->{user_info}->{permissions}->{class_id}->{0}){
		$self->log->trace('User has access to all classes');
		$args->{permitted_classes} = $args->{node_info}->{classes_by_id};
	}
	else {
		$args->{permitted_classes} = { %{ $args->{user_info}->{permissions}->{class_id} } };
		# Drop any query terms that wanted to use an unpermitted class
		
		foreach my $boolean qw(and or not range_and range_not range_or){
			foreach my $op (keys %{ $args->{attr_terms}->{$boolean} }){
				foreach my $field_name (keys %{ $args->{attr_terms}->{$boolean}->{$op} }){
					#for (my $i = 0; $i < scalar @{ $args->{attr_terms}->{$boolean}->{$op}->{$field_name} }; $i++){
					foreach my $class_id (keys %{ $args->{attr_terms}->{$boolean}->{$op}->{$field_name} }){
						#my $class_id = $args->{attr_terms}->{$boolean}->{$op}->{$field_name}->[$i];
						next if $class_id eq 0; # this is handled specially below
						unless ($args->{permitted_classes}->{$class_id}){
							my $forbidden = delete $args->{attr_terms}->{$boolean}->{$op}->{$field_name}->{$class_id};
							$self->log->warn('Forbidding attr_term from class_id ' . $class_id . ' with ' . Dumper($forbidden));
						}
					}
				}
			}
			
			foreach my $class_id (keys %{ $args->{field_terms}->{$boolean} }){
				next if $class_id eq 0; # this is handled specially below
				unless ($args->{permitted_classes}->{$class_id}){
					my $forbidden = delete $args->{field_terms}->{$boolean}->{$class_id};
					$self->log->warn('Forbidding field_term from class_id ' . $class_id . ' with ' . Dumper($forbidden));
				}
			}
		}
	}
	
	# Adjust classes if necessary
	$self->log->trace('given_classes before adjustments: ' . Dumper($args->{given_classes}));
	if (scalar keys %{ $args->{given_classes} } == 1 and $args->{given_classes}->{0}){
		$args->{distinct_classes} = $args->{permitted_classes};
	}
	elsif (scalar keys %{ $args->{given_classes} }){ #if 0 (meaning any) is given, go with permitted classes
		$args->{distinct_classes} = {};
		foreach my $key (keys %{ $args->{given_classes} }){
			if ($args->{permitted_classes}->{$key}){
				$args->{distinct_classes}->{$key} = 1;
			}
		}
	}
	elsif (scalar keys %{ $args->{distinct_classes} }) {
		foreach my $key (keys %{ $args->{distinct_classes} }){
			unless ($args->{permitted_classes}->{$key}){
				delete $args->{distinct_classes}->{$key};
			}
		}
	}
	else {
		$args->{distinct_classes} = $args->{permitted_classes};
	}
	$self->log->trace('distinct_classes after adjustments: ' . Dumper($args->{distinct_classes}));
	
	if (scalar keys %{ $args->{excluded_classes} }){
		foreach my $class_id (keys %{ $args->{excluded_classes} }){
			$self->log->trace("Excluding class_id $class_id");
			delete $args->{distinct_classes}->{$class_id};
		}
	}
	
	$self->log->debug('attr_terms: ' . Dumper($args->{attr_terms}));
	
	my $num_added_terms = 0;
	my $num_removed_terms = 0;
	
	# Adjust hosts/programs based on permissions
	foreach my $attr qw(host_id program_id node_id){
		# Do we have a blanket allow permission?
		if ($args->{user_info}->{permissions}->{$attr}->{0}){
			$self->log->debug('Permissions grant access to any ' . $attr);
			next;
		}
		else {
			# Need to only allow access to the whitelist in permissions
			
			# Add filters for the whitelisted items
			# If there are no exceptions to the whitelist, no query will succeed
			if (not scalar keys %{ $args->{user_info}->{permissions}->{$attr} }){
				die 'Insufficient privileges for querying any ' . $attr; 
			}
			
			# Remove items not explicitly whitelisted
			foreach my $boolean qw(and or){
				foreach my $op ('', '='){
					next unless $args->{attr_terms}->{$boolean}
						and $args->{attr_terms}->{$boolean}->{$op}
						and $args->{attr_terms}->{$boolean}->{$op}->{0} 
						and $args->{attr_terms}->{$boolean}->{$op}->{0}->{$attr};
					foreach my $id (keys %{ $args->{attr_terms}->{$boolean}->{$op}->{0}->{$attr} }){
						unless($self->_is_permitted($args, $attr, $id)){
							die "Insufficient permissions to query $id from $attr";
						}
					}
				}
			}


			# Add required items to filter if no filter exists
			unless (($args->{attr_terms}->{and} 
				and $args->{attr_terms}->{and}->{0} 
				and $args->{attr_terms}->{and}->{0}->{$attr}
				and scalar keys %{ $args->{attr_terms}->{and}->{0}->{$attr} })
				or ($args->{attr_terms}->{or} 
				and $args->{attr_terms}->{or}->{0} 
				and $args->{attr_terms}->{or}->{0}->{$attr}
				and scalar keys %{ $args->{attr_terms}->{or}->{0}->{$attr} })){
				foreach my $id (keys %{ $args->{user_info}->{permissions}->{$attr} }){
					$self->log->trace("Adding id $id to $attr based on permissions");
					push @{ $args->{attr_terms}->{and}->{ $args->{user_info}->{permissions}->{$attr}->{$id} }->{0}->{$attr} }, $id;
					$num_added_terms++;
				}
			}
		}
	}
	
#	foreach my $boolean qw(and or not){
#		if ($args->{field_terms}->{$boolean}->{0} and $args->{field_terms}->{$boolean}->{0}->{host}){
#			foreach my $host_int (@{ $args->{field_terms}->{$boolean}->{0}->{host} }){
#				if ($self->_is_permitted($args, 'host_id', $host_int)){
#					$self->log->trace('adding host_int ' . $host_int);
#					push @{ $args->{any_field_terms}->{$boolean} }, '(@host ' . $host_int . ')';
#					# Also add as an attr
#					push @{ $args->{attr_terms}->{$boolean}->{'='}->{0}->{host_id} }, $host_int;
#				}
#				else {
#					die "Insufficient permissions to query host_int $host_int";
#				}
#			}
#			delete $args->{field_terms}->{$boolean}->{0}->{host};
#			unless (scalar keys %{ $args->{field_terms}->{$boolean}->{0} }){
#				delete $args->{field_terms}->{$boolean}->{0};
#			}
#		}
#	}
	
	# Optimization: for the any-term fields, only search on the first term and use the rest as filters if the fields are int fields
	foreach my $boolean qw(and not){
		unless (scalar @{ $args->{any_field_terms}->{$boolean} }){
			$args->{any_field_terms}->{$boolean} = {};
			next;
		}
		my %deletion_candidates;
		foreach my $op (keys %{ $args->{attr_terms}->{$boolean} }){
			foreach my $field_name (keys %{ $args->{attr_terms}->{$boolean}->{$op} }){
				foreach my $class_id (keys %{ $args->{attr_terms}->{$boolean}->{$field_name} }){
					foreach my $attr (keys %{ $args->{attr_terms}->{$boolean}->{$field_name}->{$class_id} }){
						foreach my $raw_value (@{ $args->{attr_terms}->{$boolean}->{$field_name}->{$class_id}->{$attr} }){
							my $col = $attr;
							$col =~ s/^attr\_//;
							my $resolved_value = $self->_resolve_value($args, $class_id, $raw_value, $Field_to_order->{$col});
							$deletion_candidates{$resolved_value} = 1;
						}
					}
				}
			}
		}
	
		my @keep = shift @{ $args->{any_field_terms}->{$boolean} };
		foreach my $term (@{ $args->{any_field_terms}->{$boolean} }){
			if ($deletion_candidates{$term}){
				$self->log->trace('Optimizing out any-field term search for term ' . $term);
			}
			else {
				push @keep, $term;
			}
		}
		$args->{any_field_terms}->{$boolean} = { map { $_ => 1 } @keep };
	}
	$args->{any_field_terms}->{or} = { map { $_ => 1 } @{ $args->{any_field_terms}->{or} } };
	
	# Check all field terms to see if they are a stopword and warn if necessary
	if ($stopwords and ref($stopwords) and ref($stopwords) eq 'HASH'){
		$self->log->debug('checking terms against ' . (scalar keys %$stopwords) . ' stopwords');
		foreach my $boolean qw(and or not){
			foreach my $class_id (keys %{ $args->{field_terms}->{$boolean} }){
				foreach my $raw_field (keys %{ $args->{field_terms}->{$boolean}->{$class_id} }){
					next unless $args->{field_terms}->{$boolean}->{$class_id}->{$raw_field};
					for (my $i = 0; $i < (scalar @{ $args->{field_terms}->{$boolean}->{$class_id}->{$raw_field} }); $i++){
						my $term = $args->{field_terms}->{$boolean}->{$class_id}->{$raw_field}->[$i];
						if ($stopwords->{$term}){
							my $err = 'Removed term ' . $term . ' which is too common';
							$self->add_warning($err);
							$self->log->warn($err);
							$num_removed_terms++;
							# Drop the term
							if (scalar @{ $args->{field_terms}->{$boolean}->{$class_id}->{$raw_field} } == 1){
								delete $args->{field_terms}->{$boolean}->{$class_id}->{$raw_field};
								last;
							}
							else {
								splice(@{ $args->{field_terms}->{$boolean}->{$class_id}->{$raw_field} }, $i, 1);
							}
						}
					}
				}
			}
			foreach my $term (keys %{ $args->{any_field_terms}->{$boolean} }){ 
				if ($stopwords->{$term}){
					my $err = 'Removed term ' . $term . ' which is too common';
					$self->add_warning($err);
					$self->log->warn($err);
					$num_removed_terms++;
					# Drop the term
					delete $args->{any_field_terms}->{$boolean}->{$term};
				}
			}
		}
	}
	
	# Determine if there are any other search fields.  If there are, then use host as a filter.
	$self->log->debug('field_terms: ' . Dumper($args->{field_terms}));
	$self->log->debug('any_field_terms: ' . Dumper($args->{any_field_terms}));
	my $host_is_filter = 0;
	foreach my $boolean qw(and or){
		foreach my $class_id (keys %{ $args->{field_terms}->{$boolean} }){
			next unless $class_id;
			$host_is_filter++;
		}
		foreach my $term (sort keys %{ $args->{any_field_terms}->{$boolean} }){
			next if $term =~ /^\(\@host \d+\)$/; # Don't count host here
			$host_is_filter++;
		}
	}
	if ($host_is_filter){
		$self->log->trace('Using host as a filter because there were ' . $host_is_filter . ' query terms.');
		foreach my $boolean qw(or and not){
			foreach my $term (sort keys %{ $args->{any_field_terms}->{$boolean} }){
				if ($term =~ /^\(\@host \d+\)$/){
					$self->log->trace('Deleted term ' . $term);
					delete $args->{any_field_terms}->{$boolean}->{$term};
				}
			}
		}
	}
	
	foreach my $item qw(attr_terms field_terms any_field_terms permitted_classes given_classes distinct_classes){
		$self->log->trace("$item: " . Dumper($args->{$item}));
	}
	
	# Verify that we're still going to actually have query terms after the filtering has taken place	
	my $query_term_count = 0;
	
	foreach my $boolean qw(and or range_and){
		next unless $args->{field_terms}->{$boolean};
		foreach my $class_id (keys %{ $args->{field_terms}->{$boolean} }){
			foreach my $attr (keys %{ $args->{field_terms}->{$boolean}->{$class_id} }){
				$query_term_count++;
			}
		}
	}
	
	foreach my $boolean qw(or and){
		$query_term_count += scalar keys %{ $args->{any_field_terms}->{$boolean} }; 
	}
	
	# we might have a class-only query
	foreach my $class (keys %{ $args->{distinct_classes} }){
		unless ($num_removed_terms){ # this query used to have terms, so it wasn't really class-only
			$query_term_count++;
		}
	}
	
	$self->log->debug('query_term_count: ' . $query_term_count . ', num_added_terms: ' . $num_added_terms);
	
	unless ($query_term_count and $query_term_count > $num_added_terms){
		die 'All query terms were stripped based on permissions or they were too common';
	}
	
	if ($args->{query_meta_params}->{start}){
		$args->{start_int} = sprintf('%d', $args->{query_meta_params}->{start});
	}
	if ($args->{query_meta_params}->{end}){
		$args->{end_int} =sprintf('%d', $args->{query_meta_params}->{end});
	}
	$self->log->debug('META_PARAMS: ' . Dumper($args->{query_meta_params}));
	
	# Adjust query time params as necessary
	if ($args->{query_meta_params}->{adjust_query_times}){
		if ($args->{start_int} < $args->{min}){
			$args->{start_int} = $args->{min};
			$self->log->warn("Given start time too early, adjusting to " 
				. _epoch2iso($args->{start_int}));
		}
		elsif ($args->{start_int} > $args->{max}){
			$args->{start_int} = $args->{max} - $self->conf->get('sphinx/index_interval');
			$self->log->warn("Given start time too late, adjusting to " 
				. _epoch2iso($args->{start_int}));
		}
	}
	
	# Failsafe for times
	if ($args->{query_meta_params}->{start} or $args->{query_meta_params}->{end}){
		unless ($args->{start_int}){
			$args->{start_int} = 0;
		}
		unless ($args->{end_int}){
			$args->{end_int} = time();
		}
	}
	
	# Check to see if the query is after the latest end, but not in the future (this happens if the indexing process is backed up)
	if ($args->{start_int} and $args->{start_int} <= time() and $args->{start_int} > $args->{node_info}->{start_max}){
		$self->log->warn('Adjusted start_int ' . $args->{start_int} . ' to ' . $args->{node_info}->{start_max});
		$args->{start_int} = $args->{node_info}->{start_max};
	}
	if ($args->{end_int} and $args->{end_int} <= time() and $args->{end_int} > $args->{node_info}->{max}){
		$self->log->warn('Adjusted end_int ' . $args->{end_int} . ' to ' . $args->{node_info}->{max});
		$args->{end_int} = $args->{node_info}->{max};
	}
	
	return 1;
}

sub _parse_query_term {
	my $self = shift;
	my $args = shift;
	my $terms = shift;
	
	$self->log->debug('terms: ' . Dumper($terms));
			
	foreach my $operator (keys %{$terms}){
		my $arr = $terms->{$operator};
		foreach my $term_hash (@{$arr}){
			next unless defined $term_hash->{value};
			
			# Recursively handle parenthetical directives
			if (ref($term_hash->{value}) eq 'HASH'){
				$self->_parse_query_term($args, $term_hash->{value});
				next;
			}
			
			# Escape any digit-dash-word combos (except for host or program)
			$term_hash->{value} =~ s/(\d+)\-/$1\\\-/g unless ($term_hash->{field} eq 'program' or $term_hash->{field} eq 'host');
			
			if ($args->{archive_query}){
				# Escape any special chars
				$term_hash->{value} =~ s/([^a-zA-Z0-9\.\_\-\@])/\\$1/g;
			}
			else {
				# Get rid of any non-indexed chars
				$term_hash->{value} =~ s/[^a-zA-Z0-9\.\@\-\_\\]/\ /g;
				# Escape any '@' or sphinx will error out thinking it's a field prefix
				#$term_hash->{value} =~ s/\@/\\\@/g;
				if ($term_hash->{value} =~ /\@/){
					# need to quote
					$term_hash->{value} = '"' . $term_hash->{value} . '"';
				}
				# Sphinx can only handle numbers up to 15 places (though this is fixed in very recent versions)
				if ($term_hash->{value} =~ /^[0-9]{15,}$/){
					die('Integer search terms must be 15 or fewer digits, received ' 
						. $term_hash->{value} . ' which is ' .  length($term_hash->{value}) . ' digits.');
				}
			}
			
			if ($term_hash->{field} eq 'start'){
				# special case for start/end
				$args->{start_int} = UnixDate($term_hash->{value}, "%s");
				$self->log->trace("START: " . $args->{start_int});
				next;
			}
			elsif ($term_hash->{field} eq 'end'){
				# special case for start/end
				$args->{end_int} = UnixDate($term_hash->{value}, "%s");
				$self->log->trace("END: " . $args->{end_int});
				next;
			}
			elsif ($term_hash->{field} eq 'limit'){
				# special case for limit
				$args->{limit} = sprintf("%d", $term_hash->{value});
				$self->log->trace("Set limit " . $args->{limit});
				next;
			}
			elsif ($term_hash->{field} eq 'offset'){
				# special case for offset
				$args->{offset} = sprintf("%d", $term_hash->{value});
				$self->log->trace("Set offset " . $args->{offset});
				next;
			}
			elsif ($term_hash->{field} eq 'class'){
				# special case for class
				my $class;
				$self->log->trace('classes: ' . Dumper($args->{node_info}->{classes}));
				if ($args->{node_info}->{classes}->{ uc($term_hash->{value}) }){
					$class = lc($args->{node_info}->{classes}->{ uc($term_hash->{value}) });
				}
				else {
					die("Unknown class $term_hash->{value}");
				}
				
				if ($operator eq '-'){
					# We're explicitly removing this class
					$args->{excluded_classes}->{ $class } = 1;
				}
				else {
					$args->{given_classes}->{ $class } = 1;
				}
				$self->log->debug("Set operator $operator for given class " . $term_hash->{value});		
				next;
			}
			elsif ($term_hash->{field} eq 'groupby'){
				my $field_infos = $self->_get_field($args, $term_hash->{value});
				if ($field_infos or $term_hash->{value} eq 'node'){
					$args->{groupby} ||= [];
					push @{ $args->{groupby} }, lc($term_hash->{value});
					foreach my $class_id (keys %$field_infos){
						$args->{given_classes}->{$class_id} = 1;
					}
					$self->log->trace("Set groupby " . Dumper($args->{groupby}));
				}
				next;
			}
			elsif ($term_hash->{field} eq 'node'){
				if ($term_hash->{value} =~ /^[\w\.]+$/){
					if ($operator eq '-'){
						$args->{excluded_nodes} ||= {};
						$args->{excluded_nodes}->{ $term_hash->{value} } = 1;
					}
					else {
						$args->{given_nodes} ||= {};
						$args->{given_nodes}->{ $term_hash->{value} } = 1;
					}
				}
				next;
			}
			
			my $boolean = 'or';
				
			# Reverse if necessary
			if ($operator eq '-' and $term_hash->{op} eq '!='){
				$boolean = 'and';
			}
			elsif ($operator eq '-' and $term_hash->{op} eq '='){
				$boolean = 'not';
			}
			elsif ($operator eq '+'){
				$boolean = 'and';
			}
			elsif ($operator eq '-'){
				$boolean = 'not';
			}
						
			# Process a field/value or attr/value
			if ($term_hash->{field} and $term_hash->{value}){
				
				my $operators = {
					'>' => 1,
					'>=' => 1,
					'<' => 1,
					'<=' => 1,
					'!=' => 1, 
				};
				# Default unknown operators to AND
				unless ($operators->{ $term_hash->{op} }){
					$term_hash->{op} = '=';
				}
				
				my $values = $self->_resolve(
					$args,
					$term_hash->{field}, 
					$term_hash->{value}, 
					$term_hash->{op}
				);
				
#				if ($term_hash->{field} eq 'host'){
#					# special case for host which is also a field
#					$values->{fields}->{0}->{host} = unpack('N*', inet_aton($term_hash->{value}));
#				}
			
				
				if ($term_hash->{op} !~ /[\<\>]/){ # ignore ranges
					foreach my $class_id (keys %{ $values->{fields} }){
						foreach my $real_field (keys %{ $values->{fields}->{$class_id} }){
							$args->{field_terms}->{$boolean}->{$class_id}->{$real_field} ||= [];
							push @{ $args->{field_terms}->{$boolean}->{$class_id}->{$real_field} }, 
								$values->{fields}->{$class_id}->{$real_field};
						}	
					}
				}
				foreach my $class_id (keys %{ $values->{attrs} }){
					if ($term_hash->{op} !~ /[\<\>]/ and not exists $args->{field_terms}->{$boolean}->{$class_id}){
						push @{ $args->{any_field_terms}->{$boolean} }, $term_hash->{value} if $class_id; #skip class 0
					}
					my $field_info = $self->_get_field($args, $term_hash->{field})->{$class_id};
					next if $field_info->{field_type} eq 'string'; # skip string attributes
					$args->{attr_terms}->{$boolean}->{ $term_hash->{op} }->{ $term_hash->{field} } ||= {};
					foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
						$args->{attr_terms}->{$boolean}->{ $term_hash->{op} }->{ $term_hash->{field} }->{$class_id}->{$real_field} ||= [];
						push @{ $args->{attr_terms}->{$boolean}->{ $term_hash->{op} }->{ $term_hash->{field} }->{$class_id}->{$real_field} }, $values->{attrs}->{$class_id}->{$real_field};
					}
				}
			}				
				
			# Otherwise there was no field given, search all fields
			elsif (defined $term_hash->{value}){
				if($term_hash->{quote}){
					$term_hash->{value} = $self->_normalize_quoted_value($term_hash->{value});
				}
				push @{ $args->{any_field_terms}->{$boolean} }, $term_hash->{value};
			}
			else {
				die "no field or value given: " . Dumper($term_hash);
			}
		}
	}
	
	return 1;
}

sub _resolve {
	my $self = shift;
	my $args = shift;
	my $raw_field = shift;
	my $raw_value = shift;
	my $operator = shift;
	
	# Return all possible class_id, real_field, real_value combos
	$self->log->debug("resolving: raw_field: $raw_field, raw_value: $raw_value, operator: $operator");
	
	my %values = ( fields => {}, attrs => {} );
	# Find all possible real fields/classes for this raw field
	
	my $operator_xlate = {
		'=' => 'and',
		'' => 'or',
		'-' => 'not',
	};

	my $field_infos = $self->_get_field($args, $raw_field);
	$self->log->trace('field_infos: ' . Dumper($field_infos));
	foreach my $class_id (keys %{$field_infos}){
		if (scalar keys %{ $args->{given_classes} } and not $args->{given_classes}->{0}){
			unless ($args->{given_classes}->{$class_id} or $class_id == 0){
				$self->log->debug("Skipping class $class_id because it was not given");
				next;
			}
		}
		# we don't want to count class_id 0 as "distinct"
		if ($class_id){
			$args->{distinct_classes}->{$class_id} = 1;
		}
		
		my $field_order = $field_infos->{$class_id}->{field_order};
		# Check for string match and make that a term
		if (exists $args->{node_info}->{fields_by_type}->{string}->{ $raw_field } and
			($operator eq '=' or $operator eq '-' or $operator eq '')){
			$values{fields}->{$class_id}->{ $Field_order_to_field->{ $field_order } } = $raw_value;
					#[ $raw_value ]; #[ $self->_normalize_value($args, $class_id, $raw_value, $field_order) ];
		}
		elsif (exists $args->{node_info}->{fields_by_type}->{string}->{ $raw_field }){
			die('Invalid operator for string field');
		}
		elsif ($Field_order_to_attr->{ $field_order }){
			$values{attrs}->{$class_id}->{ $Field_order_to_attr->{ $field_order } } =
				$self->_normalize_value($args, $class_id, $raw_value, $field_order);			
		}
		else {
			$self->log->warn("Unknown field: $raw_field");
		}
	}
	$self->log->trace('values: ' . Dumper(\%values));
	return \%values;
}

sub _normalize_value {
	my $self = shift;
	my $args = shift;
	my $class_id = shift;
	my $value = shift;
	my $field_order = shift;
	#$self->log->trace('args: ' . Dumper($args) . ' value: ' . $value . ' field_order: ' . $field_order);
	
	unless (defined $class_id and defined $value and defined $field_order){
		$self->log->error('Missing an arg: ' . $class_id . ', ' . $value . ', ' . $field_order);
		return $value;
	}
	
	return $value unless $args->{node_info}->{field_conversions}->{ $class_id };
	#$self->log->debug("normalizing for class_id $class_id with the following: " . Dumper($args->{node_info}->{field_conversions}->{ $class_id }));
	
	if ($field_order == $Field_to_order->{host}){ #host is handled specially
		my @ret;
		if ($value =~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/) {
			@ret = ( unpack('N*', inet_aton($value)) ); 
		}
		elsif ($value =~ /^[a-zA-Z0-9\-\.]+$/){
			my $host_to_resolve = $value;
			unless ($value =~ /\./){
				my $fqdn_hostname = Sys::Hostname::FQDN::fqdn();
				$fqdn_hostname =~ /^[^\.]+\.(.+)/;
				my $domain = $1;
				$self->log->debug('non-fqdn given, assuming to be domain: ' . $domain);
				$host_to_resolve .= '.' . $domain;
			}
			$self->log->debug('resolving and converting host ' . $host_to_resolve. ' to inet_aton');
			my $res   = Net::DNS::Resolver->new;
			my $query = $res->search($host_to_resolve);
			if ($query){
				my @ips;
				foreach my $rr ($query->answer){
					next unless $rr->type eq "A";
					$self->log->debug('resolved host ' . $host_to_resolve . ' to ' . $rr->address);
					push @ips, $rr->address;
				}
				if (scalar @ips){
					foreach my $ip (@ips){
						my $ip_int = unpack('N*', inet_aton($ip));
						push @ret, $ip_int;
					}
				}
				else {
					die 'Unable to resolve host ' . $host_to_resolve . ': ' . $res->errorstring;
				}
			}
			else {
				die 'Unable to resolve host ' . $host_to_resolve . ': ' . $res->errorstring;
			}
		}
		else {
			die 'Invalid host given: ' . $value;
		}
		if (wantarray){
			return @ret;
		}
		else {
			return $ret[0];
		}
	}
	elsif ($args->{node_info}->{field_conversions}->{ $class_id }->{'IPv4'}
		and $args->{node_info}->{field_conversions}->{ $class_id }->{'IPv4'}->{$field_order}){
		return unpack('N', inet_aton($value));
	}
	elsif ($args->{node_info}->{field_conversions}->{ $class_id }->{PROTO} 
		and $args->{node_info}->{field_conversions}->{ $class_id }->{PROTO}->{$field_order}){
		$self->log->trace("Converting $value to proto");
		return $Proto_map->{ $value };
	}
	elsif ($args->{node_info}->{field_conversions}->{ $class_id }->{COUNTRY_CODE} 
		and $args->{node_info}->{field_conversions}->{ $class_id }->{COUNTRY_CODE}->{$field_order}){
		$self->log->trace("Converting $value to country_code");
		return join('', unpack('c*', pack('A*', $value)));
	}
	elsif ($Field_order_to_attr->{$field_order} eq 'program_id'){
		$self->log->trace("Converting $value to attr");
		$value =~ s/[^a-zA-Z0-9\_\-]/\_/g;
		return crc32($value);
	}
	elsif ($Field_order_to_attr->{$field_order} =~ /^attr_s\d+$/){
		# String attributes need to be crc'd
		return crc32($value);
	}
	else {
		#apparently we don't know about any conversions
		#$self->log->debug("No conversion for $value and class_id $class_id, field_order $field_order.");
		return $value; 
	}
}

# Opposite of normalize
sub _resolve_value {
	my $self = shift;
	my $args = shift;
	my $class_id = shift;
	my $value = shift;
	my $field_order = shift;
	
	if ($Field_order_to_meta_attr->{$field_order}){
		#$self->log->trace('interpreting field_order ' . $field_order . ' with class ' . $class_id . ' to be meta');
		$class_id = 0;
	}
	
	if ($args->{node_info}->{field_conversions}->{ $class_id }->{TIME}->{$field_order}){
		return _epoch2iso($value * $Time_values->{ $Field_order_to_attr->{$field_order} });
	}
	elsif ($args->{node_info}->{field_conversions}->{ $class_id }->{IPv4}->{$field_order}){
		#$self->log->debug("Converting $value from IPv4");
		return inet_ntoa(pack('N', $value));
	}
	elsif ($args->{node_info}->{field_conversions}->{ $class_id }->{PROTO}->{$field_order}){
		#$self->log->debug("Converting $value from proto");
		return $Inverse_proto_map->{ $value };
	}
	elsif ($args->{node_info}->{field_conversions}->{ $class_id }->{COUNTRY_CODE} 
		and $args->{node_info}->{field_conversions}->{ $class_id }->{COUNTRY_CODE}->{$field_order}){
		my @arr = $value =~ /(\d{2})(\d{2})/;
		return unpack('A*', pack('c*', @arr));
	}
	elsif ($Field_order_to_attr->{$field_order} eq 'class_id'){
		return $args->{node_info}->{classes_by_id}->{$class_id};
	}
	else {
		#apparently we don't know about any conversions
		#$self->log->debug("No conversion for $value and class_id $class_id");
		return $value; 
	}
}

sub _normalize_quoted_value {
	my $self = shift;
	my $value = shift;
	
	# Strip punctuation
	$value =~ s/[^a-zA-Z0-9\.\@\s\-]/\ /g;
	return '"' . $value . '"';
}

sub _is_permitted {
	my ($self, $args, $attr, $attr_id) = @_;
	
	if ($args->{user_info}->{permissions}->{$attr}->{0} # all are allowed
		or $args->{user_info}->{permissions}->{$attr}->{$attr_id}){
		return 1;
	}
	else {
		foreach my $id (keys %{ $args->{meta_params}->{permissions}->{$attr} }){
			if ($id =~ /^(\d+)\-(\d+)$/){
				my ($min, $max) = ($1, $2);
				if ($min <= $attr_id and $attr_id <= $max){
					return 1;
				}
			}
		}
		return 0;
	}
}

sub _build_sphinx_match_str {
	my ($self, $args) = @_;

	# Create the Sphinx Extended2 matching mode query string to be placed in MATCH()
	
	# No-field match str
	my $match_str = '';
	my (%and, %or, %not);
	foreach my $term (keys %{ $args->{any_field_terms}->{and} }){
		$and{$term} = 1;
	}
		
	my @or = ();
	foreach my $term (keys %{ $args->{any_field_terms}->{or} }){
		$or{$term} = 1;
	}
	
	my @not = ();
	foreach my $term (keys %{ $args->{any_field_terms}->{not} }){
		$not{$term} = 1;
	}
	
	foreach my $class_id (sort keys %{ $args->{distinct_classes} }){
		# First, the ANDs
		foreach my $field (sort keys %{ $args->{field_terms}->{and}->{$class_id} }){
			foreach my $value (@{ $args->{field_terms}->{and}->{$class_id}->{$field} }){
				$and{'(@' . $field . ' ' . $value . ')'} = 1;
			}
		}
				
		# Then, the NOTs
		foreach my $field (sort keys %{ $args->{field_terms}->{not}->{$class_id} }){
			foreach my $value (@{ $args->{field_terms}->{not}->{$class_id}->{$field} }){
				$not{'(@' . $field . ' ' . $value . ')'} = 1;
			}
		}
		
		# Then, the ORs
		foreach my $field (sort keys %{ $args->{field_terms}->{or}->{$class_id} }){
			foreach my $value (@{ $args->{field_terms}->{or}->{$class_id}->{$field} }){
				$or{'(@' . $field . ' ' . $value . ')'} = 1;
			}
		}
	}
	
	if (scalar keys %and){
		$match_str .= ' (' . join(' ', sort keys %and) . ')';
	}
	if (scalar keys %or){
		$match_str .= ' (' . join('|', sort keys %or) . ')';
	}
	if (scalar keys %not){
		$match_str .= ' !(' . join('|', sort keys %not) . ')';
	}
		
	$self->log->trace('match str: ' . $match_str);		
	
	return $match_str;
}

sub _build_archive_match_str {
	my ($self, $args) = @_;

	# Create the Sphinx Extended2 matching mode query string to be placed in MATCH()
	
	# No-field match str
	my $match_str = '';
	my (%and, %or, %not);
	foreach my $term (keys %{ $args->{any_field_terms}->{and} }){
		$and{'msg LIKE "%' . $term . '%"'} = 1;
	}
		
	my @or = ();
	foreach my $term (keys %{ $args->{any_field_terms}->{or} }){
		$or{'msg LIKE "%' . $term . '%"'} = 1;
	}
	
	my @not = ();
	foreach my $term (keys %{ $args->{any_field_terms}->{not} }){
		$not{'msg LIKE "%' . $term . '%"'} = 1;
	}
	
	foreach my $class_id (sort keys %{ $args->{distinct_classes} }){
		# First, the ANDs
		foreach my $field (sort keys %{ $args->{field_terms}->{and}->{$class_id} }){
			foreach my $value (@{ $args->{field_terms}->{and}->{$class_id}->{$field} }){
				$and{$field . ' LIKE "%' . $value . '%"'} = 1;
			}
		}
				
		# Then, the NOTs
		foreach my $field (sort keys %{ $args->{field_terms}->{not}->{$class_id} }){
			foreach my $value (@{ $args->{field_terms}->{not}->{$class_id}->{$field} }){
				$not{$field . ' LIKE "%' . $value . '%"'} = 1;
			}
		}
		
		# Then, the ORs
		foreach my $field (sort keys %{ $args->{field_terms}->{or}->{$class_id} }){
			foreach my $value (@{ $args->{field_terms}->{or}->{$class_id}->{$field} }){
				$or{$field . ' LIKE "%' . $value . '%"'} = 1;
			}
		}
	}
	
	my @strs;
	if (scalar keys %and){
		push @strs, ' (' . join(' AND ', sort keys %and) . ')';
	}
	if (scalar keys %or){
		push @strs, ' (' . join(' OR ', sort keys %or) . ')';
	}
	if (scalar keys %not){
		push @strs, ' NOT (' . join(' OR ', sort keys %not) . ')';
	}
	$match_str .= join(' AND ', @strs);
		
	$self->log->trace('match str: ' . $match_str);		
	
	return $match_str;
}

sub _build_sphinx_query {
	my $self = shift;
	my $args = shift;
	
	die('args') unless $args and ref($args) eq 'HASH' and $args->{user_info};
	
	$args->{queries} = []; # place to store our query with our result in a multi-query
	my %clauses = ( and => { clauses => [], vals => [] }, or => { clauses => [], vals => [] }, not => { clauses => [], vals => [] } );
	
	# Handle our basic equalities
	foreach my $boolean (keys %clauses){
		foreach my $field (sort keys %{ $args->{attr_terms}->{$boolean}->{'='} }){
			my @clause;
			foreach my $class_id (sort keys %{ $args->{attr_terms}->{$boolean}->{'='}->{$field} }){
				next unless $args->{distinct_classes}->{$class_id} or $class_id eq 0;
				foreach my $attr (sort keys %{ $args->{attr_terms}->{$boolean}->{'='}->{$field}->{$class_id} }){
					foreach my $value (@{ $args->{attr_terms}->{$boolean}->{'='}->{$field}->{$class_id}->{$attr} }){
						if ($class_id){
							push @clause, '(class_id=? AND ' . $attr . '=?)';
							push @{ $clauses{$boolean}->{vals} }, $class_id, $value;
						}
						else {
							push @clause, $attr . '=?';
							push @{ $clauses{$boolean}->{vals} }, $value;
						}
					}
				}
			}
			push @{ $clauses{$boolean}->{clauses} }, [ @clause ];
		}
	}
	
	# Ranges are tougher: First sort by field name so we can group the ranges for the same field together in an OR
	my %ranges;
	foreach my $boolean qw(and or not){
		foreach my $op (sort keys %{ $args->{attr_terms}->{$boolean} }){
			next if $op eq '=';
			foreach my $field (sort keys %{ $args->{attr_terms}->{$boolean}->{$op} }){
				foreach my $class_id (sort keys %{ $args->{attr_terms}->{$boolean}->{$op}->{$field} }){
					next unless $args->{distinct_classes}->{$class_id} or $class_id eq 0;
					foreach my $attr (sort keys %{ $args->{attr_terms}->{$boolean}->{$op}->{$field}->{$class_id} }){
						$ranges{$boolean} ||= {};
						$ranges{$boolean}->{$field} ||= {};
						$ranges{$boolean}->{$field}->{$attr} ||= {};
						$ranges{$boolean}->{$field}->{$attr}->{$class_id} ||= {};
						$ranges{$boolean}->{$field}->{$attr}->{$class_id}->{$op} ||= [];
						foreach my $value (sort { $a < $b } @{ $args->{attr_terms}->{$boolean}->{$op}->{$field}->{$class_id}->{$attr} }){
							push @{ $ranges{$boolean}->{$field}->{$attr}->{$class_id}->{$op} }, $value;
						}					
					}
				}				
			}
		}
	}
	
	# Then divine which range operators go together by sorting them and dequeuing the appropriate operator until there are none left
	foreach my $boolean qw(and or not){
		foreach my $field (sort keys %{ $ranges{$boolean} }){
			my @clause;
			foreach my $attr (sort keys %{ $ranges{$boolean}->{$field} }){
				foreach my $class_id (sort keys %{ $ranges{$boolean}->{$field}->{$attr} }){
					while (scalar keys %{ $ranges{$boolean}->{$field}->{$attr}->{$class_id} }){
						my ($min, $max, $min_op, $max_op);
						$min = shift @{ $ranges{$boolean}->{$field}->{$attr}->{$class_id}->{'>'} };
						$min_op = '>';
						unless ($min){
							$min = shift @{ $ranges{$boolean}->{$field}->{$attr}->{$class_id}->{'>='} };
							$min_op = '>=';
						}
						unless ($min){
							$min = 0;
						}
						$max = shift @{ $ranges{$boolean}->{$field}->{$attr}->{$class_id}->{'<'} };
						$max_op = '<';
						unless ($max){
							$max = shift @{ $ranges{$boolean}->{$field}->{$attr}->{$class_id}->{'<='} };
							$max_op = '<=';
						}
						unless ($max){
							$max = 2**32;
						}
						if ($class_id){
							push @clause, '(class_id=? AND ' . $attr . $min_op . '? AND ' . $attr . $max_op . '?)';
							push @{ $clauses{$boolean}->{vals} }, $class_id, $min, $max;
						}
						else {
							push @clause, '(' . $attr . $min_op . '? AND ' . $attr . $max_op . '?)';
							push @{ $clauses{$boolean}->{vals} }, $min, $max;
						}
						foreach my $op ('>', '<', '>=', '<='){
							if (exists $ranges{$boolean}->{$field}->{$attr}->{$class_id}->{$op}){
								delete $ranges{$boolean}->{$field}->{$attr}->{$class_id}->{$op}
									unless scalar @{ $ranges{$boolean}->{$field}->{$attr}->{$class_id}->{$op} };
							}
						}
					}
				}
			}
			my $joined_clause = join(' OR ', @clause);
			push @{ $clauses{$boolean}->{clauses} }, [ $joined_clause ];
		}
	}
	
	my $positive_qualifier = 1;
	if (@{ $clauses{and}->{clauses} }){
		my @clauses;
		foreach my $clause_arr (@{ $clauses{and}->{clauses} }){
			push @clauses, '(' . join(' OR ', @$clause_arr) . ')';
		}
		$positive_qualifier = join("\n" . ' AND ', @clauses);
	}
	if (@{ $clauses{or}->{clauses} }){
		my @clauses;
		foreach my $clause_arr (@{ $clauses{or}->{clauses} }){
			push @clauses, '(' . join(' OR ', @$clause_arr) . ')';
		}
		$positive_qualifier .= "\n" . ' AND (' . join(' OR ', @clauses) . ')';
	}
	
	my $negative_qualifier = 0;
	if (@{ $clauses{not}->{clauses} }){
		my @clauses;
		foreach my $clause_arr (@{ $clauses{not}->{clauses} }){
			push @clauses, '(' . join(' OR ', @$clause_arr) . ')';
		}
		$negative_qualifier = join("\n" . ' OR ', @clauses);
	}
	
	my $select = "$positive_qualifier AS positive_qualifier, $negative_qualifier AS negative_qualifier";
	my $where;
	if ($args->{archive_query}){
		$where = $self->_build_archive_match_str($args) . ' AND ' . $positive_qualifier . ' AND NOT ' . $negative_qualifier;
	}
	else {
		$where = 'MATCH(\'' . $self->_build_sphinx_match_str($args) .'\')';
		$where .=  ' AND positive_qualifier=1 AND negative_qualifier=0';
	}
	
	my @values = (@{ $clauses{and}->{vals} }, @{ $clauses{or}->{vals} }, @{ $clauses{not}->{vals} });
	
	# Check for no-class super-user query
#	unless (($args->{user_info}->{permissions}->{class_id}->{0} and $args->{given_classes}->{0})
#		#not (scalar keys %{ $args->{given_classes} }))
#		or $args->{groupby}){
		$where .= ' AND class_id IN (' . join(',', map { '?' } keys %{ $args->{distinct_classes} }) . ')';
		push @values, sort keys %{ $args->{distinct_classes} };
#	}
	# Check for time given
	if ($args->{start_int} and $args->{end_int}){
		$where .= ' AND timestamp BETWEEN ? AND ?';
		push @values, $args->{start_int}, $args->{end_int};
	}
	
	# Add a groupby query if necessary
	my $groupby;	
	if ($args->{groupby}){
		foreach my $field (@{ $args->{groupby} }){
			if ($field eq 'node'){ # special case for node
				# We'll do a normal query
				push @{ $args->{queries} }, {
					select => $select,
					where => $where,
					values => [ @values ],
				};
				next;
			}
			
			my $field_infos = $self->_get_field($args, $field);
			$self->log->trace('field_infos: ' . Dumper($field_infos));
			foreach my $class_id (keys %{$field_infos}){
				next unless $args->{distinct_classes}->{$class_id} or $class_id == 0;
				push @{ $args->{queries} }, {
					select => $select,
					where => $where . ($class_id ? ' AND class_id=?' : ''),
					values => [ @values, $class_id ? $class_id : () ],
					groupby => $Field_order_to_attr->{ $field_infos->{$class_id}->{field_order} },
					groupby_field => $field,
				};
			}
		}
	}
	else {
		# We can get away with a single query
		push @{ $args->{queries} }, {
			select => $select,
			where => $where,
			values => [ @values ],
		};
	}	
		
	return 1;
}

sub format_results {
	my ($self, $args) = @_;
	
	my $ret = '';
	if ($args->{format} eq 'tsv'){
		if ($args->{groupby}){
			foreach my $groupby (@{ $args->{groupby} }){
				foreach my $row (@{ $args->{results}->{$groupby} }){
					print join("\t", $row->{'@groupby'}, $row->{'@count'}) . "\n";
				}
			}
		}
		else {
			my @default_columns = qw(timestamp class host program msg);
			$ret .= join("\t", @default_columns, 'fields') . "\n";
			foreach my $row (@{ $args->{results} }){
				my @tmp;
				foreach my $key (@default_columns){
					push @tmp, $row->{$key};
				}
				my @fields;
				foreach my $field (@{ $row->{_fields} }){
					push @fields, $field->{field} . '=' . $field->{value};
				}
				$ret .= join("\t", @tmp, join(' ', @fields)) . "\n";
			}
		}
	}
	else {
		# default to JSON
		$ret .= $self->json->encode($args->{results}) . "\n";
	}
	return $ret;
}

sub export {
	my ($self, $args) = @_;
	
	if ( $args and ref($args) eq 'HASH' and $args->{data} and $args->{plugin} ) {
		my $decode;
		eval {
			$decode = $self->json->decode(uri_unescape($args->{data}));
			$self->log->debug( "Decoded data as : " . Dumper($decode) );
		};
		if ($@){
			$self->log->error("invalid args, error: $@, args: " . Dumper($args));
			return 'Unable to build results object from args';
		}
		
		my $results_obj;
		my $plugin_fqdn = 'Export::' . $args->{plugin};
		foreach my $plugin ($self->plugins()){
			if ($plugin eq $plugin_fqdn){
				$self->log->debug('loading plugin ' . $plugin);
				$results_obj = $plugin->new(results => $decode);
				$self->log->debug('results_obj:' . Dumper($results_obj));
			}
		}
		if ($results_obj){
			return { 
				ret => $results_obj->results(), 
				mime_type => $results_obj->mime_type(), 
				filename => CORE::time() . $results_obj->extension,
			};
		}
		
		$self->log->error("failed to find plugin " . $args->{plugin} . ', only have plugins ' .
			join(', ', $self->plugins()) . ' ' . Dumper($args));
		return 'Unable to build results object from args';
	}
	else {
		$self->log->error('Invalid args: ' . Dumper($args));
		return 'Unable to build results object from args';
	}
}

sub run_schedule {
	my ($self, $args) = @_;
	
	if ($args and $args->{user_info} and $args->{user_info}->{username} ne 'system'){
		die('Only system can run the schedule');
	}
	
	my ($query, $sth);
	
	# Find the last run time from the bookmark table
	$query = 'SELECT UNIX_TIMESTAMP(last_run) FROM schedule_bookmark';
	$sth = $self->db->prepare($query);
	$sth->execute();
	my $row = $sth->fetchrow_arrayref;
	my $last_run_bookmark = $self->conf->get('schedule_interval'); # init to interval here so we don't underflow if 0
	if ($row){
		$last_run_bookmark = $row->[0];
	}
	
	my $form_params = $self->get_form_params();
	
	# Expire schedule entries
	$query = 'SELECT id, query, username FROM query_schedule JOIN users ON (query_schedule.uid=users.uid) WHERE end < UNIX_TIMESTAMP() AND enabled=1';
	$sth = $self->db->prepare($query);
	$sth->execute();
	my @ids;
	my $counter = 0;
	while (my $row = $sth->fetchrow_hashref){
		push @ids, $row->{id};
		my $user_info = $self->get_user_info($row->{username});
		my $decode = $self->json->decode($row->{query});
		
		my $headers = {
			To => $user_info->{email},
			From => $self->conf->get('email/display_address') ? $self->conf->get('email/display_address') : 'system',
			Subject => 'ELSA alert has expired for query ' . $decode->{query_string},
		};
		my $body = 'The alert set for query ' . $decode->{query_string} . ' has expired and has been disabled.  ' .
			'If you wish to continue receiving this query, please log into ELSA, enable the query, and set a new expiration date.';
		
		$self->_send_email({headers => $headers, body => $body});
	}
	if (scalar @ids){
		$self->log->info('Expiring query schedule for ids ' . join(',', @ids));
		$query = 'UPDATE query_schedule SET enabled=0 WHERE id IN (' . join(',', @ids) . ')';
		$sth = $self->db->prepare($query);
		$sth->execute;
	}
	
	# Run schedule	
	$query = 'SELECT t1.id AS query_schedule_id, username, t1.uid, query, frequency, start, end, action_subroutine, action_params' . "\n" .
		'FROM query_schedule t1' . "\n" .
		'JOIN users ON (t1.uid=users.uid)' . "\n" .
		'JOIN query_schedule_actions t2 ON (t1.action_id=t2.action_id)' . "\n" .
		'WHERE start <= ? AND end >= ? AND enabled=1' . "\n" .
		'AND UNIX_TIMESTAMP() - UNIX_TIMESTAMP(last_alert) > alert_threshold';  # we won't even run queries we know we won't alert on
	$sth = $self->db->prepare($query);
	
	my $cur_time = $form_params->{end_int};
	$sth->execute($cur_time, $cur_time);
	
	my $user_info_cache = {};
	
	while (my $row = $sth->fetchrow_hashref){
		my @freq_arr = split(':', $row->{frequency});
		my $last_run;
		my $farthest_back_to_check = $cur_time - $self->conf->get('schedule_interval');
		my $how_far_back = $self->conf->get('schedule_interval');
		while (not $last_run and $farthest_back_to_check > ($cur_time - (86400 * 366 * 2))){ # sanity check
			$self->log->debug('$farthest_back_to_check:' . $farthest_back_to_check);
			my @prev_dates = ParseRecur($row->{frequency}, 
				ParseDate(scalar localtime($cur_time)), 
				ParseDate(scalar localtime($farthest_back_to_check)),
				ParseDate(scalar localtime($cur_time - 1))
			);
			if (scalar @prev_dates){
				$self->log->trace('prev: ' . Dumper(\@prev_dates));
				$last_run = UnixDate($prev_dates[$#prev_dates], '%s');
				$self->log->trace('last_run:' . $prev_dates[$#prev_dates]);
			}
			else {
				# Keep squaring the distance we'll go back to find the last date
				$farthest_back_to_check -= $how_far_back;
				$self->log->trace('how_far_back: ' . $how_far_back);
				$how_far_back *= $self->conf->get('schedule_interval');
			}
		}
		unless ($last_run){
			$self->log->error('Could not find the last time we ran, aborting');
			next;
		}
		# If the bookmark is earlier, use that because we could've missed runs between them
		if ($last_run_bookmark < $last_run){
			$self->log->info('Setting last_run to ' . $last_run_bookmark . ' because it is before ' . $last_run);
			$last_run = $last_run_bookmark;
		}
		my @dates = ParseRecur($row->{frequency}, 
			ParseDate(scalar localtime($cur_time)), 
			ParseDate(scalar localtime($cur_time)),
			ParseDate(scalar localtime($cur_time + $self->conf->get('schedule_interval')))
		);
		$self->log->trace('dates: ' . Dumper(\@dates) . ' row: ' . Dumper($row));
		if (scalar @dates){
			# Adjust the query time to avoid time that is potentially unindexed by offsetting by the schedule interval
			my $query_params = $self->json->decode($row->{query});
			$query_params->{query_meta_params}->{start} = ($last_run - $self->conf->get('schedule_interval'));
			$query_params->{query_meta_params}->{end} = ($cur_time - $self->conf->get('schedule_interval'));
			$query_params->{query_string} = delete $query_params->{query_string};
			$query_params->{query_schedule_id} = $row->{query_schedule_id};
			
			if (!$user_info_cache->{ $row->{uid} }){
				$user_info_cache->{ $row->{uid} } = $self->get_user_info($row->{username});
				$self->log->trace('Got user info: ' . Dumper($user_info_cache->{ $row->{uid} }));
			}
			else {
				$self->log->trace('Using existing user info');
			}
			$query_params->{user_info} = $user_info_cache->{ $row->{uid} };
			
			# Perform query
			my $results = $self->query($query_params);
			$counter++;
			
			# Take given action
			unless ($self->can($row->{action_subroutine})){
				$self->log->error('Invalid alert action: ' . $row->{action_subroutine});
				next;
			}
			
			if ($results and $results->{recordsReturned}){
				my $action_params = $self->json->decode($row->{action_params});
				$action_params->{comments} = 'Scheduled Query ' . $row->{query_schedule_id};
				$action_params->{query_schedule_id} = $row->{query_schedule_id};
				$action_params->{query} = $query_params;
				$action_params->{results} = $results;
				$self->log->debug('executing action ' . $row->{action_subroutine} . ' with params ' . Dumper($action_params));
				my $sub = $row->{action_subroutine};
				$self->$sub($action_params);
			}
		}
	}
	
	# Update our bookmark to the current run
	$query = 'UPDATE schedule_bookmark SET last_run=FROM_UNIXTIME(?)';
	$sth = $self->db->prepare($query);
	$sth->execute($cur_time);
	unless ($sth->rows){
		$query = 'INSERT INTO schedule_bookmark (last_run) VALUES (FROM_UNIXTIME(?))';
		$sth = $self->db->prepare($query);
		$sth->execute($cur_time);
	}
	
	return $counter;
}

sub _send_email {
	my ($self, $args) = @_;
	
	# Send the email
	my $email_headers = new Mail::Header();
	$email_headers->header_hashref($args->{headers});
	my $email = new Mail::Internet( Header => $email_headers, Body => [ split(/\n/, $args->{body}) ] );
	
	$self->log->debug('email: ' . $email->as_string());
	my $ret;
	if ($self->conf->get('email/smtp_server')){
		$ret = $email->smtpsend(
			Host => $self->conf->get('email/smtp_server'), 
			Debug => 1, 
			MailFrom => $self->conf->get('email/display_address')
		);
	}
	else {
		($ret) = Email::LocalDelivery->deliver($email->as_string);
	}
	if ($ret){
		$self->log->debug('done sending email');
		return 1;
	}
	else {
		$self->log->error('Unable to send email: ' . $email->as_string());
		return 0;
	}
}


sub _open_ticket {
	my ($self, $args) = @_;
	$self->log->debug('got results to create ticket on: ' . Dumper($args));
	unless (ref($args) eq 'HASH' 
		and $args->{results}){
		$self->log->info('No results for query');
		return 0;
	}
	
	unless ($self->conf->get('ticketing/email')){
		$self->log->error('No ticketing config setup.');
		return;
	}
	
	my $headers = {
		To => $self->conf->get('ticketing/email'),
		From => $self->conf->get('email/display_address') ? $self->conf->get('email/display_address') : 'system',
		Subject => $self->conf->get('email/subject') ? $self->conf->get('email/subject') : 'system',
	};
	my $body = sprintf($self->conf->get('ticketing/template'), $args->{query}->{query_string},
		sprintf('%s/get_results?qid=%d&hash=%s', 
			$self->conf->get('email/base_url') ? $self->conf->get('email/base_url') : 'http://localhost',
			$args->{qid},
			$self->_get_hash($args->{qid}),
		)
	);
	
	$self->_send_email({ headers => $headers, body => $body });
}

sub _alert {
	my ($self, $args) = @_;
	$self->log->debug('got results to alert on: ' . Dumper($args->{results}));
		
	unless (ref($args) eq 'HASH' 
		and $args->{results}->{results} 
		and ref($args->{results}->{results}) eq 'ARRAY'
		and scalar @{ $args->{results}->{results} }){
		$self->log->info('No results for query');
		return 0;
	}
	
	my $headers = {
		To => $args->{query}->{user_info}->{email},
		From => $self->conf->get('email/display_address') ? $self->conf->get('email/display_address') : 'system',
		Subject => $self->conf->get('email/subject') ? $self->conf->get('email/subject') : 'system',
	};
	my $body = sprintf('%d results for query %s', $args->{results}->{recordsReturned}, $args->{query}->{query_string}) .
		"\r\n" . sprintf('%s/get_results?qid=%d&hash=%s', 
			$self->conf->get('email/base_url') ? $self->conf->get('email/base_url') : 'http://localhost',
			$args->{results}->{qid},
			$self->_get_hash($args->{results}->{qid}),
	);
	
	my ($query, $sth);
	$query = 'SELECT UNIX_TIMESTAMP(last_alert) AS last_alert, alert_threshold FROM query_schedule WHERE id=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{query_schedule_id});
	my $row = $sth->fetchrow_hashref;
	if ((time() - $row->{last_alert}) < $row->{alert_threshold}){
		$self->log->warn('Not alerting because last alert was at ' . (scalar localtime($row->{last_alert})) 
			. ' and threshold is at ' . $row->{alert_threshold} . ' seconds.' );
		return;
	}
	else {
		$query = 'UPDATE query_schedule SET last_alert=NOW() WHERE id=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{query_schedule_id});
	}
	
	$self->_send_email({ headers => $headers, body => $body});
	
	# Save the results
	$self->_save_results({
		meta_info => { groupby => $args->{groupby} },
		qid => $args->{results}->{qid}, 
		results => $args->{results}->{results}, 
		comments => 'Scheduled Query ' . $args->{query_schedule_id} 
	});
}

sub _batch_notify {
	my ($self, $args) = @_;
	#$self->log->trace('got results for batch: ' . Dumper($args));
	
	my $headers = {
		To => $args->{user_info}->{email},
		From => $self->conf->get('email/display_address') ? $self->conf->get('email/display_address') : 'system',
		Subject => sprintf('ELSA archive query %d complete with %d results', $args->{qid}, 
			$args->{recordsReturned}),
	};
	my $body = sprintf('%d results for query %s', $args->{recordsReturned}, $args->{query_string}) .
		"\r\n" . sprintf('%s/get_results?qid=%d&hash=%s', 
			$self->conf->get('email/base_url') ? $self->conf->get('email/base_url') : 'http://localhost',
			$args->{qid},
			$self->_get_hash($args->{qid}),
	);
	
	$self->_send_email({ headers => $headers, body => $body});
}

sub run_archive_queries {
	my ($self, $args) = @_;
	
	if ($args and $args->{user_info} and $args->{user_info}->{username} ne 'system'){
		die('Not authorized to run the schedule');
	}
	
	my ($query, $sth);
	$query = 'SELECT qid, username, query FROM query_log t1 JOIN users t2 ON (t1.uid=t2.uid) WHERE ISNULL(num_results) AND archive=1';
	$sth = $self->db->prepare($query);
	$sth->execute;
	
	while (my $row = $sth->fetchrow_hashref){
		my $user_info = $self->get_user_info($row->{username});
		my $args = {
			q => $row->{query},
			user_info => $user_info,
		};
		# Record that we're starting so no one else starts it
		my $sth2 = $self->db->prepare('UPDATE query_log SET num_results=-1 WHERE qid=?');
		$sth2->execute($row->{qid});
		
		# Run the query
		$args->{qid} = $row->{qid};
		$self->_archive_query($args);
		next if $args->{cancelled};
		
		# Record the results
		$self->log->trace('got archive results: ' . Dumper($args->{results}) . ' ' . $args->{totalRecords});
		$sth2 = $self->db->prepare('UPDATE query_log SET num_results=?, milliseconds=? WHERE qid=?');
		$sth2->execute($args->{recordsReturned}, (1000 * $args->{timeTaken}), $row->{qid});
		$sth2->finish;
		
		
		$self->_save_results({ 
			qid => $row->{qid}, 
			comments => 'archive query',
			results => $args->{results},
			meta_info => $args->{groupby} ? { groupby => $args->{groupby} } : {},
		});
		$self->_batch_notify($args);
	} 
}	

sub _archive_query {
	my ($self, $args) = @_;
	$self->clear_warnings;
	$self->log->trace('running archive query with args: ' . Dumper($args));
	if ($args->{q} ) {
		# JSON-encoded query from web
		my $decode = $self->json->decode($args->{q});
		# q should contain query_string and query_meta_params
		$self->log->debug( "Decoded as : " . Dumper($decode) );
		$args->{query_meta_params} = $decode->{query_meta_params};
		$args->{query_string} = $decode->{query_string};
		if ($args->{query_meta_params}->{groupby}){
			$args->{groupby} = $args->{query_meta_params}->{groupby}
		}
		if ($args->{query_meta_params}->{timeout}){
			$args->{timeout} = sprintf("%d", ($args->{query_meta_params}->{timeout} * 1000)); #time is in milleseconds
		}
		if ($args->{query_meta_params}->{archive_query}){
			$args->{archive_query} = $args->{query_meta_params}->{archive_query}
		}
	}
	$self->log->trace('args: ' . Dumper($args));
	
	my $overall_start = time();
	
	# Set some sane defaults		
	$args->{limit} ||= $Default_limit;
	$args->{offset} ||= 0;
		
	my $ret = {};
	$args->{node_info} = $self->_get_node_info();
	$self->log->trace('using node-info: ' . Dumper($args->{node_info}));
	$self->_parse_query_string($args);
	$self->_build_sphinx_query($args);
	foreach my $query (@{ $args->{queries} }){
		$self->log->trace('query: ' . Dumper($query));
	}
	
	my %queries; # per-node hash
	foreach my $node (keys %{ $args->{node_info}->{nodes} }){
		$ret->{$node} = { rows => [] };
		$queries{$node} = [];
		my $node_info = $args->{node_info}->{nodes}->{$node};
		# Prune tables
		my @table_arr;
		foreach my $table (@{ $node_info->{tables}->{tables} }){
			if ($args->{start_int} and $args->{end_int}){
				if ($table->{table_type} eq 'archive' and
					(($args->{start_int} >= $table->{start_int} and $args->{start_int} <= $table->{end_int})
					or ($args->{end_int} >= $table->{start_int} and $args->{end_int} <= $table->{end_int})
					or ($args->{start_int} <= $table->{start_int} and $args->{end_int} >= $table->{end_int})
					or ($table->{start_int} <= $args->{start_int} and $table->{end_int} >= $args->{end_int}))
				){
					push @table_arr, $table->{table_name};
				}
			}
			else {
				push @table_arr, $table->{table_name};
			}
		}	
		unless (@table_arr){
			$self->log->debug('no tables for node ' . $node);
			next;
		}
		
		foreach my $table (@table_arr){
			my $start = time();
			foreach my $query (@{ $args->{queries} }){
				# strip sphinx-specific attr_ prefix
				$query->{where} =~ s/attr\_((?:i|s)\d)=\?/$1=\?/g; 
				my $search_query;
				if ($query->{groupby}){
					$query->{groupby} =~ s/attr\_((?:i|s)\d)/$1/g;
					$search_query = "SELECT COUNT(*) AS count, class_id, $query->{groupby} AS $query->{groupby_field}\n" .
						"FROM $table main\n" .
						'WHERE ' . $query->{where} . "\nGROUP BY $query->{groupby_field}\n" . 'ORDER BY 1 DESC LIMIT ?,?';
				}
				else {
					$search_query = "SELECT main.id,\n" .
						"\"" . $node . "\" AS node,\n" .
						"DATE_FORMAT(FROM_UNIXTIME(timestamp), \"%Y/%m/%d %H:%i:%s\") AS timestamp,\n" .
						"INET_NTOA(host_id) AS host, program, class_id, class, msg,\n" .
						"i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5\n" .
						"FROM $table main\n" .
						"LEFT JOIN " . $node_info->{db} . ".programs ON main.program_id=programs.id\n" .
						"LEFT JOIN " . $node_info->{db} . ".classes ON main.class_id=classes.id\n" .
						'WHERE ' . $query->{where} . "\n" . 'LIMIT ?,?';
				}
				#$self->log->debug('archive_query: ' . $search_query . ', values: ' . 
				#	Dumper($query->{values}, $args->{offset}, $args->{limit}));
				push @{ $queries{$node} }, 
					{ query => $search_query, values => [ @{ $query->{values} }, $args->{offset}, $args->{limit} ] };
			}
		}
	}
	my $total_found = 0;
	my ($query, $sth);
	my $queries_todo_count = 0;
	foreach my $node (keys %queries){
		$queries_todo_count += scalar @{ $queries{$node} };
	}
	
	QUERY_LOOP: while ($queries_todo_count){
		my $cv = AnyEvent->condvar;
		$cv->begin(sub {
			$cv->send;
		});
		
		foreach my $node (keys %queries){
			my $query_hash = shift @{ $queries{$node} };
			next unless $query_hash;
			# Check if the query was cancelled
			$query = 'SELECT num_results FROM query_log WHERE qid=?';
			$sth = $self->db->prepare($query);
			$sth->execute($args->{qid});
			my $row = $sth->fetchrow_hashref;
			if ($row->{num_results} eq -2){
				$self->log->info('Query ' . $args->{qid} . ' has been cancelled');
				$args->{cancelled} = 1;
				return;
			}
			
			eval {
				my $start = time();
				foreach my $key (keys %$query_hash){
					$self->log->debug('node: ' . $node . ', key: ' . $key . ', val: ' . Dumper($query_hash->{$key}));
				}
				$self->log->debug('running query ' . $query_hash->{query});
				$self->log->debug(' with values ' . join(',', @{ $query_hash->{values} }));
				$cv->begin;
				$args->{node_info}->{nodes}->{$node}->{dbh}->query($query_hash->{query}, sub { 
						$self->log->debug('Archive query for node ' . $node . ' finished in ' . (time() - $start));
						my ($dbh, $rows, $rv) = @_;
						$self->log->trace('node ' . $node . ' got archive result: ' . Dumper($rows));
						if (not $rv){
							my $e = 'node ' . $node . ' got error ' . $rows;
							$self->log->error($e);
							$self->add_warning($e);
							$cv->end;
							next;
						}
						push @{ $ret->{$node}->{rows} }, @$rows;
						$cv->end; #end archive query
					},
					@{ $query_hash->{values} });
			};
			if ($@){
				$ret->{$node}->{error} = 'sphinx query error: ' . $@;
				$self->log->error('sphinx query error: ' . $@);
				$cv->end;
			}

		}
		$cv->end; # bookend initial begin
		$cv->recv; # block until all of the above completes
		
		# See how many we have left to do in case we're done
		$queries_todo_count = 0;
		foreach my $node (keys %queries){
			$queries_todo_count += scalar @{ $queries{$node} };
		}
	}
	
	$args->{totalRecords} = 0;
	if ($args->{groupby}){
		$args->{results} = {};
		foreach my $groupby (@{ $args->{groupby} }){
			my %agg;
			foreach my $node (sort keys %$ret){
				# One-off for grouping by node
				if ($groupby eq 'node'){
					$agg{$node} = scalar @{ $ret->{$node}->{rows} };
					next;
				}
				
				foreach my $row (@{ $ret->{$node}->{rows} }){
					my $field_infos = $self->_resolve($args, $groupby, $row->{$groupby}, '=');
					my $field = (keys %{ $field_infos->{attrs}->{ $row->{class_id} } })[0];
					$field =~ s/attr\_//;
					my $key;
					if (exists $Field_to_order->{ $field }){
						# Resolve normally
						$key = $self->_resolve_value($args, $row->{class_id}, 
							$row->{$groupby}, $Field_to_order->{ $field });
					}
					elsif (exists $Time_values->{ $field }){
						# We will resolve later
						$key = $groupby;
					}
					$agg{ $key } += $row->{count};	
				}
			}
			$self->log->trace('got agg ' . Dumper(\%agg) . ' for groupby ' . $groupby);
			if (exists $Time_values->{ $groupby }){
				# Sort these in ascending label order
				my @tmp;
				foreach my $key (sort { $a <=> $b } keys %agg){
					my $unixtime = ($key * $Time_values->{ $groupby });
					push @tmp, { 
						intval => $unixtime, 
						'@groupby' => $self->_resolve_value($args, 0, 
							$key, $Field_to_order->{ $groupby }), 
						'@count' => $agg{$key}
					};
				}
				
				# Fill in zeroes for missing data so the graph looks right
				my @zero_filled;
				my $increment = $Time_values->{ $groupby };
				$self->log->trace('using increment ' . $increment . ' for time value ' . $groupby);
				OUTER: for (my $i = 0; $i < @tmp; $i++){
					push @zero_filled, $tmp[$i];
					if (exists $tmp[$i+1]){
						for (my $j = $tmp[$i]->{intval} + $increment; $j < $tmp[$i+1]->{intval}; $j += $increment){
							$self->log->trace('i: ' . $tmp[$i]->{intval} . ', j: ' . ($tmp[$i]->{intval} + $increment) . ', next: ' . $tmp[$i+1]->{intval});
							push @zero_filled, { 
								'@groupby' => _epoch2iso($j), 
								intval => $j,
								'@count' => 0
							};
							last OUTER if scalar @zero_filled > $args->{limit};
						}
					}
				}
				$args->{results} = [ @zero_filled ];
			}
			else { 
				# Sort these in descending value order
				my @tmp;
				foreach my $key (sort { $agg{$b} <=> $agg{$a} } keys %agg){
					push @tmp, { intval => $agg{$key}, '@groupby' => $key, '@count' => $agg{$key} };
					$args->{totalRecords} += $agg{$key};
					last if scalar keys %agg > $args->{limit};
				}
				$args->{results}->{$groupby} = [ @tmp ];
				$args->{recordsReturned} += scalar @tmp;
			}
		}	
	}
	else {
		$args->{results} = [];
		my @tmp; # we need to sort chronologically
		NODE_LOOP: foreach my $node (keys %$ret){
			$args->{totalRecords} += scalar @{ $ret->{$node}->{rows} };
			foreach my $row (@{ $ret->{$node}->{rows} }){
				$row->{_fields} = [
						{ field => 'host', value => $row->{host}, class => 'any' },
						{ field => 'program', value => $row->{program}, class => 'any' },
						{ field => 'class', value => $row->{class}, class => 'any' },
					];
				# Resolve column names for fields
				foreach my $col qw(i0 i1 i2 i3 i4 i5 s0 s1 s2 s3 s4 s5){
					my $value = delete $row->{$col};
					# Swap the generic name with the specific field name for this class
					my $field = $args->{node_info}->{fields_by_order}->{ $row->{class_id} }->{ $Field_to_order->{$col} }->{value};
					if (defined $value and $field){
						# See if we need to apply a conversion
						$value = $self->_resolve_value($args, $row->{class_id}, $value, $Field_to_order->{$col});
						push @{ $row->{_fields} }, { 'field' => $field, 'value' => $value, 'class' => $args->{node_info}->{classes_by_id}->{ $row->{class_id} } };
					}
				}
				push @tmp, $row;
			}
		}
		foreach my $row (sort { $a->{timestamp} cmp $b->{timestamp} } @tmp){
			push @{ $args->{results} }, $row;
			last if scalar @{ $args->{results} } >= $args->{limit};
		}
		$args->{recordsReturned} = scalar @{ $args->{results} };
	}
	
	$args->{errors} = $self->warnings;
	
	$args->{timeTaken} = (time() - $overall_start);
	
	$self->log->debug('completed query in ' . $args->{timeTaken} . ' with ' . $args->{recordsReturned} . ' rows');
	
	return 1;
}

sub cancel_query {
	my ($self, $args) = @_;
	
	my ($query, $sth);
	$query = 'UPDATE query_log SET num_results=-2 WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{qid});
	return { ok => 1 };
}

1;