package API;
use Moose;
with 'MooseX::Traits';
use Data::Dumper;
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

use AnyEvent::DBI;
# Override the native request format to retrieve hashes instead of arrays and allow multi-queries
BEGIN {
	sub AnyEvent::DBI::req_exec  {
	   my (undef, $st, @args) = @{+shift};
	   my $sth = $AnyEvent::DBI::DBH->prepare_cached ($st, undef, 1)
	      or die [$DBI::errstr];
	
	   my $rv = $sth->execute (@args)
	      or die [$sth->errstr];
	        my @rows;
	        do {
	                while (my $row = $sth->fetchrow_hashref){
	                        push @rows, $row;
	                }
	        } while ($sth->more_results);
	
	   [1, \@rows, $rv]
	}
}

our $Default_limit = 100;
our $Max_limit = 1000;
our $Implicit_plus = 0;
our $Max_batch_queries = 32; # Sphinx default

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

sub BUILD {
	my $self = shift;
	
	if ( $self->conf->get('auth/method') eq 'LDAP' ) {
		require Net::LDAP::Express;
		require Net::LDAP::FilterBuilder;
		$self->ldap($self->_get_ldap());
	}
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
	unless ($username){
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
				},
				filter => '',
				email => $self->conf->get('user_email') ? $self->conf->get('user_email') : 'root@localhost',
			};
		}
		else {
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

	$user_info->{permissions} = $self->_get_permissions($user_info->{groups})
		or ($self->log->error('Unable to get permissions') and return 0);
	$self->log->debug('got permissions: ' . Dumper($user_info->{permissions}));

	# Tack on a place to store queries
	$user_info->{qids} = {};

	# Record when the session started for timeout purposes
	$user_info->{session_start_time} = time();

	return $user_info;
}


sub _get_permissions {
	my ($self, $groups) = @_;
	return {} unless $groups and ref($groups) eq 'ARRAY' and scalar @$groups;
	my ($query, $sth);
	
	# Find group permissions
	my %permissions;
	ATTR_LOOP: foreach my $attr qw(class_id host_id program_id){
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
		# Special case for program which defaults to allow
		if (scalar @arr == 0 and $attr eq 'program_id'){
			$permissions{$attr} = { 0 => 1 };
			next ATTR_LOOP;
		}
		$permissions{$attr} = { map { $_ => 1 } @arr };
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
	$query = 'SELECT t2.uid, t2.query, meta_info FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid)' . "\n" .
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
	my $results = decode_json($row->{meta_info});
	my $saved_query = decode_json($row->{query});
	foreach my $item qw(query_params query_meta_params){
		$results->{$item} = $saved_query->{$item};
	}
	$results->{results} = [];
	if ($results->{groupby}){
		$results->{groups} = {};
		$results->{groups}->{ $results->{groupby} } = [];
	}
	
	$query = 'SELECT data FROM saved_results_rows WHERE qid=? ORDER BY rowid ASC';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{qid});
	while (my $row = $sth->fetchrow_hashref){
		if ($results->{groupby}){
			push @{ $results->{groups}->{ $results->{groupby} } }, decode_json($row->{data}); 
		}
		else {	
			push @{ $results->{results} }, decode_json($row->{data});
		}
	}
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
					$row->{attr_value} = $form_params->{classes}->{ $row->{attr_id} };
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
		
	# Build programs hash
	my $programs = {};
	foreach my $class_id (keys %{ $form_params->{programs} }){
		foreach my $program_name (keys %{ $form_params->{programs}->{$class_id} }){
			$programs->{ $form_params->{programs}->{$class_id}->{$program_name} } = $program_name;
		}
	}
	
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
				$row->{attr_value} = $form_params->{classes}->{ $row->{attr_id} };
			}
			elsif ($row->{attr} eq 'program_id'){
				$row->{attr_value} = $programs->{ $row->{attr_id} };
			}
			else {
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
	eval { $args->{permissions} = decode_json( $args->{permissions} ); };
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
		#$self->log->error('Not implemented');
		#return;
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
	
	#TODO
	$stats->{nodes} = $self->_parallel_sql();
		
	# Combine the stats info for the nodes
	my $combined = {};
	$self->log->debug('got stats: ' . Dumper($stats->{nodes}));
	
	foreach my $stat qw(load index archive){
		$combined->{$stat} = { x => [], LogsPerSec => [], KBytesPerSec => [] };
		foreach my $node (keys %{ $stats->{nodes} }){
			if ($stats->{nodes}->{$node} and $stats->{nodes}->{$node}->{results} 
				and $stats->{nodes}->{$node}->{results}->{load_stats}){ 
				my $load_data = $stats->{nodes}->{$node}->{results}->{load_stats}->{$stat}->{data};
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
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub { shift->send });
	foreach my $node (keys %$node_conf){
		$cv->begin;
		my $db_name = 'syslog';
		if ($node_conf->{$node}->{db}){
			$db_name = $node_conf->{$node}->{db};
		}
		$nodes{$node} = { db => $db_name };
		$nodes{$node}->{dbh} = AnyEvent::DBI->new('dbi:mysql:database=' . $db_name . ';host=' . $node, 
			$node_conf->{$node}->{username}, $node_conf->{$node}->{password}, 
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
	my %nodes;
	my $node_conf = $self->conf->get('nodes');
	
	my $cv = AnyEvent->condvar;
	$cv->begin(sub { shift->send });
	foreach my $node (keys %$node_conf){
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
		$cv->begin;
		$nodes{$node}->{sphinx} = AnyEvent::DBI->new('dbi:mysql:port=' . $sphinx_port .';host=' . $node, undef, undef, 
			PrintError => 0, 
			mysql_multi_statements => 1, 
			mysql_bind_type_guessing => 1, 
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

sub old_get_sphinx_nodes {
	my $self = shift;
	my %nodes;
	my $node_conf = $self->conf->get('nodes');
	
	foreach my $node (keys %$node_conf){
		my $db_name = 'syslog';
		if ($node_conf->{$node}->{db}){
			$db_name = $node_conf->{$node}->{db};
		}
		$nodes{$node} = { db => $db_name };
		my $sphinx_port = 3307;
		if ($node_conf->{$node}->{sphinx_port}){
			$sphinx_port = $node_conf->{$node}->{sphinx_port};
		}
		eval {
			$nodes{$node} = { 
				dbh => AnyEvent::DBI->new('dbi:mysql:database=' . $db_name . ';host=' . $node, 
					$node_conf->{$node}->{username}, $node_conf->{$node}->{password}, RaiseError => 1, mysql_multi_statements => 1),
				sphinx => AnyEvent::DBI->new('dbi:mysql:port=' . $sphinx_port .';host=' . $node, 
					undef, undef, RaiseError => 1, mysql_multi_statements => 1, mysql_bind_type_guessing => 1),
				#cv => AnyEvent->condvar,
			};
		};
		if ($@){
			$nodes{$node}->{error} = $@;
			$self->log->error($@);
		}
	}
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
		next if exists $nodes->{$node}->{error};
		$ret->{nodes}->{$node} = {};
		if (exists $nodes->{$node}->{error}){
			$ret->{nodes}->{$node}->{error} = $nodes->{$node}->{error};
			next;
		}
		
		# Get indexes
		$query = sprintf('SELECT CONCAT(SUBSTR(type, 1, 4), "_", id) AS name, start, UNIX_TIMESTAMP(start) AS start_int, end, UNIX_TIMESTAMP(end) AS end_int, type, records FROM %s.v_indexes WHERE ISNULL(locked_by) AND type!="unavailable" ORDER BY start', 
			$nodes->{$node}->{db});
		$cv->begin;
		$self->log->trace($query);
		$nodes->{$node}->{dbh}->exec($query, sub {
			my ($dbh, $rows, $rv) = @_;
			
			if ($rows){
				$self->log->trace('node returned rv: ' . $rv);
				$ret->{nodes}->{$node}->{indexes} = {
					indexes => $rows,
					min => $rows->[0]->{start_int},
					max => $rows->[$#$rows]->{end_int},
				};
			}
			else {
				$self->log->error('No indexes for node ' . $node);
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
		$nodes->{$node}->{dbh}->exec($query, sub {
			my ($dbh, $rows, $rv) = @_;
			
			if ($rows){
				$self->log->trace('node returned rv: ' . $rv);
				$ret->{nodes}->{$node}->{tables} = $rows;
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
		$nodes->{$node}->{dbh}->exec($query, sub {
			my ($dbh, $rows, $rv) = @_;
			
			if ($rows){
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
		$nodes->{$node}->{dbh}->exec($query, sub {
			my ($dbh, $rows, $rv) = @_;
			
			if ($rows){
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
			$start_max = $ret->{nodes}->{$node}->{indexes}->{min};
		}
	}
	$ret->{min} = $min;
	$ret->{max} = $max;
	$ret->{start_max} = $start_max;
	
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
		elsif ($field_hash->{value} eq 'country_code' and $field_hash->{pattern_type} eq 'QSTRING'){
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
	
	return $ret;
}


sub get_form_params {
	my ( $self, $args) = @_;
	my $user = $args->{user_info};
	
	my $node_info = $self->_get_node_info();
	$self->log->trace('got node_info: ' . Dumper($node_info));
	
	my $form_params = {
		start => epoch2iso($node_info->{min}),
		start_int => $node_info->{min},
		end => epoch2iso($node_info->{max}),
		end_int => $node_info->{max},
		classes => $node_info->{classes},
		fields => $node_info->{fields},
	};
	
	
	if ($args->{permissions}){
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
		next if $given_arg eq 'id';
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
		$args = decode_json($args->{results});
	};
	if ($@){
		$self->_error($@);
		return;
	}
	unless ($args->{qid} and $args->{results} and ref($args->{results}) eq 'ARRAY'){
		$self->_error('Invalid args: ' . Dumper($args));
		return;
	}
	$args->{comments} = $comments;
	
	$self->log->debug('got results to save: ' . Dumper($args));
		
	my $meta_info = {};
	my $results;
	
	if (scalar @{ $args->{results} }){
		$results = [ @{ $args->{results} } ];
	}
	elsif (scalar keys %{ $args->{groups} }){
		foreach my $group_name (keys %{ $args->{groups} }){
			$meta_info->{groupby} = $group_name;
			$results = [ @{ $args->{groups}->{$group_name} } ];
			last; # only do the first one
		}
	}
	else {
		$self->log->info('No results for query');
		$self->_error('No results to save');
		return 0;
	}
	
	my ($query, $sth);
	
	$self->db->begin_work;
	$query = 'INSERT INTO saved_results (qid, meta_info, comments) VALUES(?,?,?)';
	$sth = $self->db->prepare($query);
	
	if ($args->{action_params} and ref($args->{action_params}) eq 'HASH'){
		foreach my $key (keys %{ $args->{action_params} }){
			$meta_info->{$key} = $args->{action_params}->{$key};
		}
	}
	
	$meta_info->{totalRecords} = (scalar @{ $results });
	$meta_info->{qid} = $args->{qid};
	
	eval {
		$sth->execute($args->{qid}, $self->json->encode($meta_info), $args->{comments});
		$query = 'INSERT INTO saved_results_rows (qid, data) VALUES (?,?)';
		$sth = $self->db->prepare($query);
		foreach my $row (@{ $results }){
			$sth->execute($meta_info->{qid}, $self->json->encode($row));
		}
	};
	if ($@){
		$self->db->rollback;
		$self->_error($@);
		return;
	}
	
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
	
	$query = 'SELECT t1.qid, t2.query, comments, meta_info' . "\n" .
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
			'(SELECT TOP ? t1.qid, t2.query, comments, meta_info FROM ' . "\n" .
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
		    'SELECT t1.qid, t2.query, comments, num_results, UNIX_TIMESTAMP(timestamp) AS timestamp, meta_info ' . "\n"
		  . 'FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid) ' . "\n"
		  . 'WHERE uid=?' . "\n"
		  . 'ORDER BY qid DESC LIMIT ?,?';
		$sth = $self->db->prepare($query) or die( $self->db->errstr );
		$sth->execute( $uid, $offset, $limit );
	}

	my $queries = [];    # only save the latest unique query
	while ( my $row = $sth->fetchrow_hashref ) {
		# we have to decode this to make sure it doesn't end up as a string
		my $meta_info = decode_json( $row->{meta_info} );
		my $decode = decode_json($row->{query});
		my $query = $decode->{query_params};
		push @{$queries}, { 
			qid => $row->{qid},
			timestamp => $row->{timestamp}, #$meta_info->{time},
			query => $query, 
			num_results => $row->{num_results}, #$meta_info->{recordsReturned}, 
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
			my $prev_query = decode_json( $row->{query} );
			if (    $prev_query
				and ref($prev_query) eq 'HASH'
				and $prev_query->{query_params} )
			{
				push @{$queries},
				  {
					qid          => $row->{qid},
					query        => $prev_query->{query_params},
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
	my $like = q/%},"query_params":"/ . $args->{query} . '%';
	
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
		$sth = $self->db->prepare($query) or die( $self->db->errstr );
		$sth->execute( $args->{user_info}->{uid}, $like, $limit );
	}

	my $queries = {};    # only save the latest unique query
	while ( my $row = $sth->fetchrow_hashref ) {
		if ( $row->{query} ) {
			my $prev_query = decode_json( $row->{query} );
			if (    $prev_query
				and ref($prev_query) eq 'HASH'
				and $prev_query->{query_params} )
			{
				unless (
					    $queries->{ $prev_query->{query_params} }
					and $queries->{ $prev_query->{query_params} }->{timestamp}
					cmp    # stored date is older
					$row->{timestamp} < 0
				  )
				{
					$queries->{ $prev_query->{query_params} } = {
						qid          => $row->{qid},
						query        => $prev_query->{query_params},
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
	
	eval { $args->{exception} = decode_json( $args->{exception} ); };
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
	if ($args->{exception}->{attr} eq 'host_id'){
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
		my $query_params = decode_json($row->{query});
		 return { qid => $row->{qid}, query => $query_params->{query_params} };
	}
	else {
		 return {qid => 0};
	}
}


sub query {
	my ($self, $args) = @_;
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
		$args->{query_string} = $decode->{query_params};
		if ($args->{query_meta_params}->{groupby}){
			$args->{groupby} = $args->{query_meta_params}->{groupby}
		}
		if ($args->{query_meta_params}->{timeout}){
			$args->{timeout} = sprintf("%d", ($args->{query_meta_params}->{timeout} * 1000)); #time is in milleseconds
		}
	}
	
	my $ret = { query_string => $args->{query_string} };	
	
	if ($args->{query_string} ){
		
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
			$args->{q} ? $args->{q} : $self->json->encode({ query_string => $args->{query_string} }), 
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
		}		
		else {
			# Actually perform the query
			#$self->stats->mark('query');
			
			# Parse our query
			#$self->stats->mark('query_parse');
			$args->{node_info} = $self->cache->get('node_info');
			unless ($args->{node_info}){
				$args->{node_info} = $self->_get_node_info();
				$self->cache->set('node_info', $args->{node_info}, $self->conf->get('sphinx/index_interval'));
			}
			#$self->log->trace('using node-info: ' . Dumper($args->{node_info}));
			# propagate node errors
			foreach my $node (keys %{ $args->{node_info} }){
				if (exists $args->{node_info}->{nodes}->{$node}->{error}){
					$ret->{errors} ||= [];
					push @{ $ret->{errors} }, $args->{node_info}->{nodes}->{$node}->{error};
				}
			}
			$self->_parse_query_string($args);
			
			#$self->stats->mark('query_parse', 1);
			
			# Execute search
			$self->_sphinx_query($args);
			$ret->{results} = $args->{results};
			
			#$self->stats->mark('query', 1);	
			$self->log->info(sprintf("Query $qid returned %d rows", scalar @{ $args->{results} }));
				
			$ret->{hash} = $self->_get_hash($qid); #tack on the hash for permalinking on the frontend
			$ret->{totalTime} = int(
				(Time::HiRes::time() - $args->{start_time}) * 1000
			);
			
			# Update the db to ack
			$query = 'UPDATE query_log SET num_results=?, milliseconds=? '
			  		. 'WHERE qid=?';
			$sth = $self->db->prepare($query);
			
			$ret->{totalRecords} = scalar @{ $args->{results} };
			if ($args->{groupby}){
				$ret->{groupby} = $args->{groupby};
			}
			$sth->execute( $ret->{totalRecords}, $ret->{totalTime}, $qid );
			
			if (scalar @{ $args->{errors} }){
				if ($ret->{errors}){
					push @{ $ret->{errors} }, $args->{errors};
				}
				else{
					$ret->{errors} = $args->{errors};
				}
			}
		}
		
		return $ret;
	}
	else {
		$self->_error('Invalid args: ' . Dumper($args));
		return;
	}
}

sub get_log_info {
	my ($self, $args) = @_;
	my $user = $args->{user_info};
	
	my $decode;
	eval {
		$decode = decode_json(decode_base64($args->{q}));
	};
	if ($@){
		$self->_error('Invalid JSON args: ' . Dumper($args));
		return;
	}
	
	unless ($decode and ref($decode) eq 'HASH'){
		$self->_error('Invalid args: ' . Dumper($decode));
		return;
	}
	
	my $data;
	
	unless ($decode->{class} and $self->conf->get('plugins/' . $decode->{class})){
		$self->log->debug('no plugins for class ' . $decode->{class});
		$data =  { summary => 'No info.', urls => [], plugins => [] };
		return $data;
	}
	
	eval {
		my $plugin = $self->conf->get('plugins/' . $decode->{class})->new({conf => $self->conf, data => $decode});
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

#sub _query {
#	my ($self, $args) = @_;
#	
#	die('Invalid params')	unless $args and ref($args) eq 'HASH';
#	die('Invalid params') unless $args->{query_meta_params} and ref($args->{query_meta_params}) eq 'HASH';
#	die('Invalid params')	unless $args->{query_string};
#	
#	#$self->stats->mark('query');
#	$args->{limit} = $Default_limit;
#	$args->{offset} = 0;
#	if ($args->{query_meta_params}->{timeout}){
#		$args->{timeout} = sprintf("%d", ($args->{query_meta_params}->{timeout} * 1000)); #time is in milleseconds
#	}
#	else {
#		$args->{timeout} = sprintf("%d", ($self->conf->get('query_timeout') * 1000));
#	}
#	$self->log->debug("Using timeout of $args->{timeout}");
#		
#	if ($args->{query_meta_params}->{groupby}){
#		$args->{groupby} = $args->{query_meta_params}->{groupby}
#	}
#		
#	if ($args->{query_meta_params}->{archive_query}){
#		return $self->_archive_query($args);
#	}
#	
#	# Parse our query
#	#$self->stats->mark('query_parse');
#	$args->{node_info} = $self->cache->get('node_info');
#	unless ($args->{node_info}){
#		$args->{node_info} = $self->_get_node_info();
#		$self->cache->set('node_info', $args->{node_info}, $self->conf->get('sphinx/index_interval'));
#	}
#	$self->_parse_query_string($args);
#	
#	#$self->stats->mark('query_parse', 1);
#	
#	# Execute search
#	$self->_sphinx_query($args);
#	
#	#$self->stats->mark('query', 1);	
#	$self->log->info(sprintf("Query returned %d rows", scalar @{ $args->{results} }));
#	
#}

sub _sphinx_query {
	my ($self, $args) = @_;
	
	$self->_build_sphinx_query($args);
	
	my $nodes = $self->_get_sphinx_nodes();
	my $ret = {};
	my $overall_start = time();
	$args->{errors} = [];
	foreach my $node (keys %{ $nodes }){
		if (exists $nodes->{$node}->{error}){
			push @{ $args->{errors} }, $nodes->{$node}->{error};
			$self->log->warn('not using node ' . $node . ' because ' . $nodes->{$node}->{error});
			delete $nodes->{$node};
		}
	}
	
	# Get indexes from all nodes in parallel
	my $cv = AnyEvent->condvar;
	$cv->begin(sub {
		$cv->send;
	});
	
	foreach my $node (keys %$nodes){
		$ret->{$node} = {};
		my $node_info = $args->{node_info}->{nodes}->{$node};
		# Prune indexes
		my @index_arr;
		foreach my $index (@{ $node_info->{indexes}->{indexes} }){
			if ($args->{start_int} and $args->{end_int}){
				if (($args->{start_int} >= $index->{start_int} and $args->{start_int} <= $index->{end_int})
					or ($args->{end_int} >= $index->{start_int} and $args->{end_int} <= $index->{end_int})){
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
				my $search_query = $query->{select} . ' FROM ' . $indexes . ' WHERE ' . $query->{where};
				if (exists $query->{groupby}){
					$search_query .= ' GROUP BY ' . $query->{groupby};
				}
				$search_query .= ' LIMIT ?,?';
				push @multi_values, @{ $query->{values } }, $args->{offset}, $args->{limit};
				$self->log->debug('sphinx_query: ' . $search_query . ', values: ' . 
					Dumper($query->{values}));
				push @multi_queries, $search_query;
			}
			
			$self->log->trace('multiquery: ' . join(';', @multi_queries));
			$self->log->trace('values: ' . join(',', @multi_values));
			$cv->begin;
			$nodes->{$node}->{sphinx}->exec(join(';', @multi_queries), 
				@multi_values,# $query->{groupby}, $args->{offset}, $args->{limit}, 
				sub { 
					$self->log->debug('Sphinx query for node ' . $node . ' finished in ' . (time() - $start));
					my ($dbh, $rows, $rv) = @_;
					$self->log->trace('node ' . $node . ' got sphinx rows: ' . Dumper($rows));
					$ret->{$node}->{sphinx_rows} = $rows;
					
					# Find what tables we need to query to resolve rows
					my %tables;
					ROW_LOOP: foreach my $row (@$rows){
						foreach my $table_hash (@{ $args->{node_info}->{nodes}->{$node}->{tables} }){
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
							"\"" . $node . "\" AS node,\n" .
							"DATE_FORMAT(FROM_UNIXTIME(timestamp), \"%%Y/%%m/%%d %%H:%%i:%%s\") AS timestamp,\n" .
							"INET_NTOA(host_id) AS host, program, class_id, class, msg,\n" .
							"i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5\n" .
							"FROM %1\$s main\n" .
							"LEFT JOIN %2\$s.programs ON main.program_id=programs.id\n" .
							"LEFT JOIN %2\$s.classes ON main.class_id=classes.id\n" .
							' WHERE main.id IN (' . $placeholders . ')',
							$table, $nodes->{$node}->{db});
						$self->log->trace('table query for node ' . $node . ': ' . $table_query);
						$cv->begin;
						$nodes->{$node}->{dbh}->exec($table_query, @{ $tables{$table} }, 
							sub { 
								my ($dbh, $rows, $rv) = @_;
								$self->log->trace('node '. $node . ' got db rows: ' . (scalar @$rows));
								foreach my $row (@$rows){
									$ret->{$node}->{results} ||= {};
									$ret->{$node}->{results}->{ $row->{id} } = $row;
								}
								$cv->end;
							});
					}	
					$cv->end; #end sphinx query
				}); 
			
		};
		if ($@){
			$ret->{$node}->{error} = 'sphinx query error: ' . $@;
			$self->log->error('sphinx query error: ' . $@);
			$cv->end;
		}
	}
	$cv->end; # bookend initial begin
	$cv->recv; # block until all of the above completes
	
	$args->{results} = [];
	if (exists $args->{groupby}){
		my %agg;
		foreach my $node (sort keys %$ret){
			foreach my $sphinx_row (@{ $ret->{$node}->{sphinx_rows} }){
				# Resolve the @groupby col with the mysql col
				unless (exists $ret->{$node}->{results}->{ $sphinx_row->{id} }){
					$self->log->warn('mysql row for sphinx id ' . $sphinx_row->{id} . ' did not exist');
					next;
				}
				my $key;
				if (exists $Field_to_order->{ $args->{groupby} }){
					# Resolve normally
					$key = $self->_resolve_value($args, $sphinx_row->{class_id}, 
						$sphinx_row->{'@groupby'}, $Field_to_order->{ $args->{groupby} });
				}
				elsif (exists $Time_values->{ $args->{groupby} }){
					# We will resolve later
					$key = $sphinx_row->{'@groupby'};
				}
				else {
					# Resolve with the mysql row
					my $field_order = $self->_get_field($args, $args->{groupby})->{ $sphinx_row->{class_id} }->{field_order};
					#$key = $self->_resolve_value($args, $sphinx_row->{class_id}, $sphinx_row->{'@groupby'}, $field_order);
					$key = $ret->{$node}->{results}->{ $sphinx_row->{id} }->{ $Field_order_to_field->{$field_order} };
				}
				$agg{ $key } += $sphinx_row->{'@count'};	
			}
		}
		if (exists $Time_values->{ $args->{groupby} }){
			# Sort these in ascending label order
			my @tmp;
			foreach my $key (sort { $a <=> $b } keys %agg){
				my $unixtime = ($key * $Time_values->{ $args->{groupby} });
				push @tmp, { 
					intval => $unixtime, 
					'@groupby' => $self->_resolve_value($args, 0, 
						$key, $Field_to_order->{ $args->{groupby} }), 
					'@count' => $agg{$key}
				};
			}
			
			# Fill in zeroes for missing data so the graph looks right
			my @zero_filled;
			my $increment = $Time_values->{ $args->{groupby} };
			$self->log->trace('using increment ' . $increment . ' for time value ' . $args->{groupby});
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
			}
			$args->{results} = [ @tmp ];
		}
	}
	else {
		foreach my $node (keys %$ret){
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
					my $field = $args->{node_info}->{fields_by_order}->{ $row->{class_id} }->{ $Field_to_order->{$col} }->{fqdn_field};
					if (defined $value and $field){
						# See if we need to apply a conversion
						$value = $self->_resolve_value($args, $row->{class_id}, $value, $Field_to_order->{$col});
						push @{ $row->{_fields} }, { 'field' => $field, 'value' => $value, 'class' => $args->{node_info}->{classes_by_id}->{ $row->{class_id} } };
					}
				}
				push @{ $args->{results} }, $row;
			}
		}
	}
	
	# Check for errors
	foreach my $node (keys %{ $ret->{nodes} }){
		if (exists $ret->{$node}->{error}){
			push @{ $args->{errors} }, $ret->{$node}->{error};
		}
	}
	
	$self->log->debug('completed query in ' . (time() - $overall_start) . ' with ' . (scalar @{ $args->{results} }) . ' rows');
	
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
			$fields{ $row->{class_id} } = { 
				'value' => $row->{field}, 
				'text' => uc($row->{field}),
				'field_id' => $row->{field_id},
				'class_id' => $row->{class_id},
				'field_order' => $row->{field_order},
				'field_type' => $row->{field_type},
			};
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
		$args->{given_classes}->{ sprintf("%d", $args->{node_info}->{classes}->{ lc($args->{query_meta_params}->{class}) }) } = 1;
	}
	
	# If no class was given anywhere, see if we can divine it from a groupby or local_groupby
	if (not scalar keys %{ $args->{given_classes} }){
		if (exists $args->{groupby}){
			foreach my $field (@{ $args->{groupby} }){
				my $field_infos = $self->_get_field($args, $field);
				foreach my $class_id (keys %{$field_infos}){
					$args->{given_classes}->{$class_id} = 1;
				}
			}
		}
#		elsif ($args->{query_meta_params}->{local_groupby}){
#			foreach my $field (@{ $args->{query_meta_params}->{local_groupby} }){
#				my $field_infos = $self->_get_field($args, $field);
#				foreach my $class_id (keys %{$field_infos}){
#					$args->{given_classes}->{$class_id} = 1;
#				}
#			}
#		}
	}
		
	# Check for meta limit
	if ($args->{query_meta_params}->{limit}){
		$args->{limit} = sprintf("%d", $args->{query_meta_params}->{limit});
		$self->log->debug("Set limit " . $args->{limit});
	}
	
	$args->{field_terms} = {
		'or' => {},
		'and' => {},
		'not' => {},
	};
	
	$args->{any_field_terms} = {
		'or' => [],
		'and' => [],
		'not' => [],
	};
	
	$args->{attr_terms} = {
		'and' => {},
		'not' => {},
		'range_and' => {},
		'range_not' => {},
	};
		
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
	
	# Determine if there are any other search fields.  If there are, then use host as a filter.
	$self->log->debug('_FIELD_TERMS: ' . Dumper($args->{field_terms}));
	my $host_is_filter = 0;
	foreach my $boolean qw(and or){
		foreach my $class_id (keys %{ $args->{field_terms}->{$boolean} }){
			next unless $class_id;
			$host_is_filter++;
		}
		foreach my $term (@{ $args->{any_field_terms}->{$boolean} }){
			$host_is_filter++;
		}
	}
	if ($host_is_filter){
		$self->log->trace('Using host as a filter because there were ' . $host_is_filter . ' query terms.');
		$self->log->trace('$args->{field_terms} before adjustment: ' . Dumper($args->{field_terms}));
		foreach my $boolean qw(or and not){
			next unless $args->{field_terms}->{$boolean} 
				and $args->{field_terms}->{$boolean}->{0} 
				and $args->{field_terms}->{$boolean}->{0}->{host};
			# OR doesn't make sense as an attr filter, change to and
			my $attr_boolean = $boolean;
			if ($attr_boolean eq 'or'){
				$attr_boolean = 'and';
			}
			$args->{attr_terms}->{$attr_boolean} ||= {};
			$args->{attr_terms}->{$attr_boolean}->{0} ||= {};
			$args->{attr_terms}->{$attr_boolean}->{0}->{host_id} = { map { $_ => 1 } @{ delete $args->{field_terms}->{$boolean}->{0}->{host} } };
			$self->log->debug('swapping host_id field terms to be attr terms for boolean ' . $boolean);
		}
	}

	$self->log->debug('attr before conversion: ' . Dumper($args->{attr_terms}));
	# convert the ranges found in the query string from hash to array.  there can be only one range per attr in the query terms.
	foreach my $boolean qw(range_and range_not){
		foreach my $class_id (keys %{ $args->{attr_terms}->{$boolean} }){
			foreach my $attr (keys %{ $args->{attr_terms}->{$boolean}->{$class_id} }){
				$args->{attr_terms}->{$boolean}->{$class_id}->{$attr} =  [
					{ 
						attr => $attr, 
						min => $args->{attr_terms}->{$boolean}->{$class_id}->{$attr}->{min},
						max => $args->{attr_terms}->{$boolean}->{$class_id}->{$attr}->{max},
						exclude => $boolean eq 'range_and' ? 0 : 1,
					}
				];
			}
		}
	}
	
	# Check for blanket allow on classes
	if ($args->{user_info}->{permissions}->{class_id}->{0}){
		$self->log->trace('User has access to all classes');
		$args->{permitted_classes} = $args->{node_info}->{classes_by_id};
	}
	else {
		$args->{permitted_classes} = { %{ $args->{query_meta_params}->{permissions}->{class_id} } };
		# Drop any query terms that wanted to use an unpermitted class
		foreach my $item qw(field_terms attr_terms){
			foreach my $boolean qw(and or not range_and range_not){
				foreach my $class_id (keys %{ $args->{$item}->{$boolean} }){
					next if $class_id eq 0; # this is handled specially below
					unless ($args->{permitted_classes}->{$class_id}){
						my $forbidden = delete $self->{$item}->{$boolean}->{$class_id};
						$self->log->warn('Forbidding ' . $item . ' from class_id ' . $class_id . ' with ' . Dumper($forbidden));
					}
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
	
	# Find all field names in the AND
	my %required_fields;
	$self->log->trace('field_terms and: ' . Dumper($args->{field_terms}->{and}));
	foreach my $class_id (keys %{ $args->{field_terms}->{and} }){
		foreach my $raw_field (keys %{ $args->{field_terms}->{and}->{$class_id} }){
			my $field_order = $Field_to_order->{ $raw_field };
			my $field = $args->{node_info}->{fields_by_order}->{$class_id}->{$field_order}->{value};
			$self->log->trace('field_terms boolean:and, class_id: ' . $class_id . ' raw_field:' . $raw_field . ', field: ' . $field);
			next unless $field;
			$required_fields{ $field } = 1;
		}
	}
	foreach my $boolean qw(and range_and){
		$self->log->debug('attr_terms ' . $boolean . ': ' . Dumper($args->{attr_terms}->{$boolean}));
		foreach my $class_id (keys %{ $args->{attr_terms}->{$boolean} }){
			foreach my $raw_field (keys %{ $args->{attr_terms}->{$boolean}->{$class_id} }){
				$raw_field =~ s/^attr\_//g; #strip off the attr_ to get the actual field name
				$self->log->trace('raw_field: ' . $raw_field);
				my $field_order = $Field_to_order->{ $raw_field };
				my $field = $args->{node_info}->{fields_by_order}->{$class_id}->{$field_order}->{value};
				$self->log->trace('attr_terms boolean:' . $boolean . ', class_id: ' . $class_id . ' raw_field:' . $raw_field . ', field: ' . $field);
				next unless $field;
				$required_fields{ $field } = 1;
			}
		}
	}
	$self->log->debug('required_fields: ' . Dumper(\%required_fields));
	# Remove any classes that won't provide the field needed from the query	
	foreach my $candidate_class_id (keys %{ $args->{distinct_classes} }){
		foreach my $required_field (keys %required_fields){
			$self->log->trace('checking for required field: ' . $required_field);
			my $found = 0;
			foreach my $row (@{ $args->{node_info}->{fields_by_name}->{$required_field} }){
				if ($row->{class_id} eq $candidate_class_id){
					$self->log->trace('found required_field ' . $required_field . ' in class_id ' . $candidate_class_id . ' at row: ' . Dumper($row));
					$found = 1;
					last;
				}
				elsif ($row->{class_id} == 0){
					$self->log->trace('required_field ' . $required_field . ' is a meta attr and exists in all classes');
					$found = 1;
					last;
				}
			}
			unless ($found){
				$self->log->trace('removing class_id ' . $candidate_class_id);
				delete $args->{distinct_classes}->{$candidate_class_id};
			}
		}
	}		
	
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
	foreach my $attr qw(host_id program_id){
		# Do we have a blanket allow permission?
		if ($args->{user_info}->{permissions}->{$attr}->{0}){
			$self->log->debug('Permissions grant access to any ' . $attr);
			next;
		}
		else {
			# Need to only allow access to the whitelist in permissions
			
			# Add filters for the whitelisted items
			# If there are no exceptions to the whitelist, no query will succeed
			if (not scalar keys %{ $args->{query_meta_params}->{permissions}->{$attr} }){
				die 'Insufficient privileges for querying any ' . $attr; 
			}
			
			# Remove items not explicitly whitelisted
			foreach my $boolean qw(and or){
				next unless $args->{attr_terms}->{$boolean} 
					and $args->{attr_terms}->{$boolean}->{0} 
					and $args->{attr_terms}->{$boolean}->{0}->{$attr};
				foreach my $id (keys %{ $args->{attr_terms}->{$boolean}->{0}->{$attr} }){
					unless($self->_is_permitted($args, $attr, $id)){
						die "Insufficient permissions to query $id from $attr";
					}
				}
			}
			
			# Handle range_and
			if ($args->{attr_terms}->{range_and} 
				and $args->{attr_terms}->{range_and}->{0} 
				and $args->{attr_terms}->{range_and}->{0}->{$attr}){
				for (my $i = 0; $i < scalar @{ $args->{attr_terms}->{range_and}->{0}->{$attr} }; $i++){
					my $hash = $args->{attr_terms}->{range_and}->{0}->{$attr}->[$i];
					unless ($self->_is_permitted($args, $attr, $hash->{min}) and $self->_is_permitted($args, $attr, $hash->{max})){
						die 'Insufficient permissions to query: ' . Dumper($hash);
					}
				}
			}
			
			# Add required items to filter if no filter exists
			unless (($args->{attr_terms}->{range_and} 
				and $args->{attr_terms}->{range_and}->{0} 
				and $args->{attr_terms}->{range_and}->{0}->{$attr}
				and scalar @{ $args->{attr_terms}->{range_and}->{0}->{$attr} })
				or ($args->{attr_terms}->{and} 
				and $args->{attr_terms}->{and}->{0} 
				and $args->{attr_terms}->{and}->{0}->{$attr}
				and scalar keys %{ $args->{attr_terms}->{and}->{0}->{$attr} })
				or ($args->{attr_terms}->{or} 
				and $args->{attr_terms}->{or}->{0} 
				and $args->{attr_terms}->{or}->{0}->{$attr}
				and scalar keys %{ $args->{attr_terms}->{or}->{0}->{$attr} })){
				foreach my $id (keys %{ $args->{query_meta_params}->{permissions}->{$attr} }){
					$self->log->trace("Adding id $id to $attr based on permissions");
					# Deal with ranges
					if ($id =~ /(\d+)\-(\d+)/){
						$args->{attr_terms}->{range_and}->{0}->{$attr} ||= [];
						push @{ $args->{attr_terms}->{range_and}->{0}->{$attr} }, { attr => $attr, min => $1, max => $2, exclude => 0 };
					}
					else {
						push @{ $args->{attr_terms}->{and}->{0}->{$attr} }, $id;
					}
					$num_added_terms++;
				}
			}
		}
	}
	
	# One-off for dealing with hosts as fields
	foreach my $boolean qw(and or not){
		if ($args->{field_terms}->{$boolean}->{0} and $args->{field_terms}->{$boolean}->{0}->{host}){
			foreach my $host_int (@{ $args->{field_terms}->{$boolean}->{0}->{host} }){
				if ($self->_is_permitted($args, 'host_id', $host_int)){
					$self->log->trace('adding host_int ' . $host_int);
					push @{ $args->{any_field_terms}->{$boolean} }, '(@host ' . $host_int . ')';
					# Also add as an attr
					push @{ $args->{attr_terms}->{$boolean}->{0}->{host_id} }, $host_int;
				}
				else {
					die "Insufficient permissions to query host_int $host_int";
				}
			}
			delete $args->{field_terms}->{$boolean}->{0}->{host};
		}
	}
	
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
		}
	}
	
	# Remove duplicate filters
	foreach my $boolean qw(range_and range_not){
		foreach my $class_id (keys %{ $args->{attr_terms}->{$boolean} }){
			foreach my $attr (keys %{ $args->{attr_terms}->{$boolean}->{$class_id} }){
				my %uniq;
				my @deduped;
				foreach my $filter_hash (@{ $args->{attr_terms}->{$boolean}->{$class_id}->{$attr} }){
					if (exists $uniq{ $filter_hash->{min} } and $uniq{ $filter_hash->{min} } eq $filter_hash->{max}){
						next;
					}
					else {
						$uniq{ $filter_hash->{min} } = $filter_hash->{max};
						push @deduped, $filter_hash;
					}
				}
				
				# Remove any ranges eclipsed by larger ranges
				my @final;
				$self->log->trace('uniq: ' . Dumper(\%uniq));
				$self->log->trace('deduped: ' . Dumper(\@deduped));
				FILTER_LOOP: foreach my $filter_hash (@deduped){
					$self->log->trace('eval for eclipse: ' . Dumper($filter_hash));
					foreach my $min (keys %uniq){
						if ($min < $filter_hash->{min} and $filter_hash->{max} < $uniq{$min}){
							$self->log->trace('min ' . $min . ' eclipsed by ' . $filter_hash->{min} . ' and ' . $uniq{$min} . ' ' . $filter_hash->{max});
							next FILTER_LOOP;
						}
					}
					push @final, $filter_hash;
				}
				
				$args->{attr_terms}->{$boolean}->{$class_id}->{$attr} = [ @final ];
			}
		}
	}
	
#	# Loop through and see if we have "between" statements where min and max are supplied by two separate range ops
#	foreach my $boolean qw(range_and range_not){
#		foreach my $class_id (keys %{ $args->{attr_terms}->{$boolean} }){
#			foreach my $attr (keys %{ $args->{attr_terms}->{$boolean}->{$class_id} }){
#				if (scalar @{ $args->{attr_terms}->{range_and}->{$class_id}->{$attr} }){
#					# If there is more than one range given for AND, we have to do a workaround:
#					# A<=VALUE<=B OR C<=VALUE<=D
#					# becomes A<=VALUE<=D AND NOT (B+1<=VALUE<=C-1)
#					
#					# Find ranges in order
#					my @values;
#					
#					# Find any stray values from single attr_id's included in AND and OR booleans.  
#					#  These will have to count when finding the blanket min/max values for the umbrella include
#					foreach my $and_or_boolean qw(and or){
#						if (ref($args->{attr_terms}->{$and_or_boolean}->{$class_id}->{$attr}) eq 'HASH'){
#							foreach my $val (keys %{ $args->{attr_terms}->{$and_or_boolean}->{$class_id}->{$attr} }){
#								push @values, $val, $val;
#							}
#						}
#						elsif (ref($args->{attr_terms}->{$and_or_boolean}->{$class_id}->{$attr}) eq 'ARRAY'){
#							foreach my $val (@{ $args->{attr_terms}->{$and_or_boolean}->{$class_id}->{$attr} }){
#								push @values, $val, $val;
#							}
#						}
#					}
#									
#					foreach my $filter_hash (@{ $args->{attr_terms}->{range_and}->{$class_id}->{$attr} }){
#						push @values, $filter_hash->{min};
#						push @values, $filter_hash->{max};
#					}
#					@values = sort { $a <=> $b } @values;
#					$self->log->trace('values: ' . join(',', @values));
#					
#					delete $args->{attr_terms}->{range_and}->{$class_id}->{$attr}; # clear what was there
#					
#					# Set the wide include
#					push @{ $args->{attr_terms}->{range_and}->{$class_id}->{$attr} }, { attr => $attr, min => $values[0], max => $values[-1], exclude => 0 };
#					$self->log->trace('including ' . $values[0] . '-' . $values[-1]);
#					
#					# Set the individual excludes
#					for (my $i = 1; $i < ((scalar @values) - 1); $i += 2){
#						my ($min, $max) = ($values[$i], $values[$i+1]);
#						if ($max - $min <= 1){
#							next;
#						}
#						$min++;
#						$max--;
#						$self->log->trace('excluding ' . $min . '-' . $max);
#						push @{ $args->{attr_terms}->{range_and}->{$class_id}->{$attr} }, { attr => $attr, min => $min, max => $max, exclude => 1 };
#					}
#					
#					# Remove the unused individual attr_id's now that they're in the ranges
#					delete $args->{attr_terms}->{and}->{$class_id}->{$attr};
#					delete $args->{attr_terms}->{or}->{$class_id}->{$attr};
#				}
#			}
#		}
#	}
	
	foreach my $item qw(attr_terms field_terms any_field_terms permitted_classes given_classes distinct_classes){
		$self->log->trace("$item: " . Dumper($args->{$item}));
	}
	
	# Verify that we're still going to actually have query terms after the filtering has taken place	
	my $query_term_count = 0;
	if (scalar keys %{ $args->{distinct_classes} }){
		foreach my $term_type qw(field_terms attr_terms){
			foreach my $boolean qw(and or range_and){
				next unless $args->{$term_type}->{$boolean};
				foreach my $class_id (keys %{ $args->{$term_type}->{$boolean} }){
					foreach my $attr (keys %{ $args->{$term_type}->{$boolean}->{$class_id} }){
						if (ref($args->{$term_type}->{$boolean}->{$class_id}->{$attr}) eq 'ARRAY'){
							$query_term_count += scalar @{ $args->{$term_type}->{$boolean}->{$class_id}->{$attr} };
						}
						elsif (ref($args->{$term_type}->{$boolean}->{$class_id}->{$attr}) eq 'HASH'){
							$query_term_count += scalar keys %{ $args->{$term_type}->{$boolean}->{$class_id}->{$attr} };
						}
					}
				}
			}
		}
	}
	
	foreach my $boolean qw(or and){
		$query_term_count += scalar @{ $args->{any_field_terms}->{$boolean} }; 
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
#		# If no end given, default to max
#		if (not $args->{end_int}){
#			$args->{end_int} = $args->{max};
#		}
#		elsif ($args->{end_int} > $args->{max}){
#			$args->{end_int} = $args->{max};
#			$self->log->warn("Given end time too late, adjusting to " 
#				. _epoch2iso($args->{end_int}));
#		}
#		elsif ($args->{end_int} < $args->{min}){
#			$args->{end_int} = $args->{min} + $self->conf->get('sphinx/index_interval');
#			$self->log->warn("Given end time too early, adjusting to " 
#				. _epoch2iso($args->{end_int}));
#		}
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
		$args->{start_int} = $args->{node_info}->{start_max};
	}
	if ($args->{end_int} and $args->{end_int} <= time() and $args->{end_int} > $args->{node_info}->{max}){
		$args->{end_int} = $args->{node_info}->{max};
	}
	
	return 1;
}

sub _parse_query_term {
	my $self = shift;
	my $args = shift;
	my $terms = shift;
	
	$self->log->debug('terms: ' . Dumper($terms));
	
	my $min_val = 0;
	my $max_val = 2**32 - 1; #uint
			
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
			
			# Get rid of any non-indexed chars
			$term_hash->{value} =~ s/[^a-zA-Z0-9\.\@\-\_\\]/\ /g;
			
			# Escape any '@' or sphinx will error out thinking it's a field prefix
			$term_hash->{value} =~ s/\@/\\\@/g;
			
			# Sphinx can only handle numbers up to 15 places (though this is fixed in very recent versions)
			if ($term_hash->{value} =~ /^[0-9]{15,}$/){
				die('Integer search terms must be 15 or fewer digits, received ' 
					. $term_hash->{value} . ' which is ' .  length($term_hash->{value}) . ' digits.');
				
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
				if ($field_infos){
					$args->{groupby} = lc($term_hash->{value});
					$self->log->trace("Set groupby " . $args->{groupby});
				}
				next;
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
								
				if ($operator eq '-'){
					if ($term_hash->{op} eq '='){
						foreach my $class_id (keys %{ $values->{fields} }){
							foreach my $real_field (keys %{ $values->{fields}->{$class_id} }){
								$args->{field_terms}->{not}->{$class_id}->{$real_field} ||= [];
								push @{ $args->{field_terms}->{not}->{$class_id}->{$real_field} }, 
									@{ $values->{fields}->{$class_id}->{$real_field} };
							}	
						}
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								$args->{attr_terms}->{not}->{$class_id}->{$real_field} ||= [];
								push @{ $args->{attr_terms}->{not}->{$class_id}->{$real_field} },
									@{ $values->{attrs}->{$class_id}->{$real_field} };
							}
						}
					}
					elsif ($term_hash->{op} eq '<' or $term_hash->{op} eq '<='){
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								unless ($args->{attr_terms}->{range_not}->{$class_id}->{$real_field}){
									$args->{attr_terms}->{range_not}->{$class_id}->{$real_field} = { min => $min_val, max => $max_val };
								}
								$args->{attr_terms}->{range_not}->{$class_id}->{$real_field}->{max} = $values->{attrs}->{$class_id}->{$real_field}->[0];
							}
						};
						
					}
					elsif ($term_hash->{op} eq '>' or $term_hash->{op} eq '>='){
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								unless ($args->{attr_terms}->{range_not}->{$class_id}->{$real_field}){
									$args->{attr_terms}->{range_not}->{$class_id}->{$real_field} = { min => $min_val, max => $max_val };
								}
								$args->{attr_terms}->{range_not}->{$class_id}->{$real_field}->{min} = $values->{attrs}->{$class_id}->{$real_field}->[0];
							}
						};
					}
					else {
						# Only thing left is '!=' which in this context is a double-negative
						foreach my $class_id (keys %{ $values->{fields} }){
							foreach my $real_field (keys %{ $values->{fields}->{$class_id} }){
								if ($args->{field_terms}->{and}->{$class_id}->{$real_field}){
									 push @{ $args->{field_terms}->{and}->{$class_id}->{$real_field} }, @{ $values->{fields}->{$class_id}->{$real_field} };
								}
								else {
									$args->{field_terms}->{and}->{$class_id}->{$real_field} = [ @{ $values->{fields}->{$class_id}->{$real_field} } ];
								}
							}	
						}
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								if ($args->{attr_terms}->{and}->{$class_id}->{$real_field}){
									 push @{ $args->{attr_terms}->{and}->{$class_id}->{$real_field} }, @{ $values->{attrs}->{$class_id}->{$real_field} };
								}
								else {
									$args->{attr_terms}->{and}->{$class_id}->{$real_field} = [ @{ $values->{attrs}->{$class_id}->{$real_field} } ];
								}
							}
						}
					}
				}
				elsif ($operator eq '+') {
					if ($term_hash->{op} eq '='){
						foreach my $class_id (keys %{ $values->{fields} }){
							foreach my $real_field (keys %{ $values->{fields}->{$class_id} }){
								if ($args->{field_terms}->{and}->{$class_id}->{$real_field}){
									 push @{ $args->{field_terms}->{and}->{$class_id}->{$real_field} }, @{ $values->{fields}->{$class_id}->{$real_field} };
								}
								else {
									$args->{field_terms}->{and}->{$class_id}->{$real_field} = [ @{ $values->{fields}->{$class_id}->{$real_field} } ];
								}
							}	
						}
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								if ($args->{attr_terms}->{and}->{$class_id}->{$real_field}){
									 push @{ $args->{attr_terms}->{and}->{$class_id}->{$real_field} }, @{ $values->{attrs}->{$class_id}->{$real_field} };
								}
								else {
									$args->{attr_terms}->{and}->{$class_id}->{$real_field} = [ @{ $values->{attrs}->{$class_id}->{$real_field} } ];
								}
							}
						}
					}
					elsif ($term_hash->{op} eq '<' or $term_hash->{op} eq '<='){
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								unless ($args->{attr_terms}->{range_and}->{$class_id}->{$real_field}){
									$args->{attr_terms}->{range_and}->{$class_id}->{$real_field} = { min => $min_val, max => $max_val };
								}
								$args->{attr_terms}->{range_and}->{$class_id}->{$real_field}->{max} = $values->{attrs}->{$class_id}->{$real_field}->[0];
							}
						}
					}
					elsif ($term_hash->{op} eq '>' or $term_hash->{op} eq '>='){
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								unless ($args->{attr_terms}->{range_and}->{$class_id}->{$real_field}){
									$args->{attr_terms}->{range_and}->{$class_id}->{$real_field} = { min => $min_val, max => $max_val };
								}
								$args->{attr_terms}->{range_and}->{$class_id}->{$real_field}->{min} = $values->{attrs}->{$class_id}->{$real_field}->[0];
							}
						}
					}
					else {
						# Only thing left is '!='
						foreach my $class_id (keys %{ $values->{fields} }){
							foreach my $real_field (keys %{ $values->{fields}->{$class_id} }){
								if ($args->{field_terms}->{not}->{$class_id}->{$real_field}){
									 push @{ $args->{field_terms}->{not}->{$class_id}->{$real_field} }, @{ $values->{fields}->{$class_id}->{$real_field} };
								}
								else {
									$args->{field_terms}->{not}->{$class_id}->{$real_field} = [ @{ $values->{fields}->{$class_id}->{$real_field} } ];
								}
							}	
						}
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								if ($args->{attr_terms}->{not}->{$class_id}->{$real_field}){
									 push @{ $args->{attr_terms}->{not}->{$class_id}->{$real_field} }, @{ $values->{attrs}->{$class_id}->{$real_field} };
								}
								else {
									$args->{attr_terms}->{not}->{$class_id}->{$real_field} = [ @{ $values->{attrs}->{$class_id}->{$real_field} } ];
								}
							}
						}
					}
				}
				else { #OR
					if ($term_hash->{op} eq '='){
						foreach my $class_id (keys %{ $values->{fields} }){
							foreach my $real_field (keys %{ $values->{fields}->{$class_id} }){
								if ($args->{field_terms}->{or}->{$class_id}->{$real_field}){
									 push @{ $args->{field_terms}->{or}->{$class_id}->{$real_field} }, @{ $values->{fields}->{$class_id}->{$real_field} };
								}
								else {
									$args->{field_terms}->{or}->{$class_id}->{$real_field} = [ @{ $values->{fields}->{$class_id}->{$real_field} } ];
								}
							}	
						}
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								$self->log->warn("OR on attr $real_field is impossible, converting to AND");
								if ($args->{attr_terms}->{and}->{$class_id}->{$real_field}){
									 push @{ $args->{attr_terms}->{and}->{$class_id}->{$real_field} }, @{ $values->{attrs}->{$class_id}->{$real_field} };
								}
								else {
									$args->{attr_terms}->{and}->{$class_id}->{$real_field} = [ @{ $values->{attrs}->{$class_id}->{$real_field} } ];
								}
							}
						}
					}
					elsif ($term_hash->{op} eq '<' or $term_hash->{op} eq '<='){
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								unless ($args->{attr_terms}->{range_and}->{$class_id}->{$real_field}){
									$args->{attr_terms}->{range_and}->{$class_id}->{$real_field} = { min => $min_val, max => $max_val };
								}
								$args->{attr_terms}->{range_and}->{$class_id}->{$real_field}->{max} = $values->{attrs}->{$class_id}->{$real_field}->[0];
							}
						};
						
					}
					elsif ($term_hash->{op} eq '>' or $term_hash->{op} eq '>='){
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								unless ($args->{attr_terms}->{range_and}->{$class_id}->{$real_field}){
									$args->{attr_terms}->{range_and}->{$class_id}->{$real_field} = { min => $min_val, max => $max_val };
								}
								$args->{attr_terms}->{range_and}->{$class_id}->{$real_field}->{min} = $values->{attrs}->{$class_id}->{$real_field}->[0];
							}
						};
					}
					else {
						# Only thing left is '!='
						foreach my $class_id (keys %{ $values->{fields} }){
							foreach my $real_field (keys %{ $values->{fields}->{$class_id} }){
								if ($args->{field_terms}->{not}->{$class_id}->{$real_field}){
									 push @{ $args->{field_terms}->{not}->{$class_id}->{$real_field} }, @{ $values->{fields}->{$class_id}->{$real_field} };
								}
								else {
									$args->{field_terms}->{not}->{$class_id}->{$real_field} = [ @{ $values->{fields}->{$class_id}->{$real_field} } ];
								}
							}	
						}
						foreach my $class_id (keys %{ $values->{attrs} }){
							foreach my $real_field (keys %{ $values->{attrs}->{$class_id} }){
								if ($args->{attr_terms}->{not}->{$class_id}->{$real_field}){
									 push @{ $args->{attr_terms}->{not}->{$class_id}->{$real_field} }, @{ $values->{attrs}->{$class_id}->{$real_field} };
								}
								else {
									$args->{attr_terms}->{not}->{$class_id}->{$real_field} = [ @{ $values->{attrs}->{$class_id}->{$real_field} } ];
								}
							}
						}
					}
				}
			}
			# Otherwise there was no field given, search all fields
			elsif (defined $term_hash->{value}){
				if($term_hash->{quote}){
					$term_hash->{value} = $self->_normalize_quoted_value($term_hash->{value});
				}
				
				if ($operator eq '-'){
					push @{ $args->{any_field_terms}->{not} }, $term_hash->{value};
				}
				elsif ($operator eq '+'){
					push @{ $args->{any_field_terms}->{and} }, $term_hash->{value};
				}
				else {
					push @{ $args->{any_field_terms}->{or} }, $term_hash->{value};
				}
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

	my $field_infos = $self->_get_field($args, $raw_field);
	#$self->log->trace('field_infos: ' . Dumper($field_infos));
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
		if ($Field_order_to_field->{ $field_order }
			and ($operator eq '=' or $operator eq '-' or $operator eq '')){
			$values{fields}->{$class_id}->{ $Field_order_to_field->{ $field_order } } =
					[ $self->_normalize_value($args, $class_id, $raw_value, $field_order) ];
		}
		elsif ($Field_order_to_attr->{ $field_order }){
			$values{attrs}->{$class_id}->{ $Field_order_to_attr->{ $field_order } } =
				[ $self->_normalize_value($args, $class_id, $raw_value, $field_order) ];			
		}
		else {
			$self->log->warn("Unknown field: $raw_field");
		}
	}
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
	foreach my $term (@{ $args->{any_field_terms}->{and} }){
		$and{$term} = 1;
	}
		
	my @or = ();
	foreach my $term (@{ $args->{any_field_terms}->{or} }){
		$or{$term} = 1;
	}
	
	my @not = ();
	foreach my $term (@{ $args->{any_field_terms}->{not} }){
		$not{$term} = 1;
	}
	
	foreach my $class_id (sort keys %{ $args->{distinct_classes} }){
		# First, the ANDs
		foreach my $field (sort keys %{ $args->{field_terms}->{and}->{$class_id} }){
			foreach my $value (@{ $args->{field_terms}->{and}->{$class_id}->{$field} }){
				$and{$value} = 1;
			}
		}
				
		# Then, the NOTs
		foreach my $field (sort keys %{ $args->{field_terms}->{not}->{$class_id} }){
			foreach my $value (@{ $args->{field_terms}->{not}->{$class_id}->{$field} }){
				$not{$value} = 1;
			}
		}
		
		# Then, the ORs
		foreach my $field (sort keys %{ $args->{field_terms}->{or}->{$class_id} }){
			foreach my $value (@{ $args->{field_terms}->{or}->{$class_id}->{$field} }){
				$or{$value} = 1;
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

sub _per_field_build_sphinx_match_str {
	my ($self, $args) = @_;

	# Create the Sphinx Extended2 matching mode query string to be placed in MATCH()
	
	# No-field match str
	my $match_str = '';
	my @tmp;
	foreach my $term (@{ $args->{any_field_terms}->{and} }){
		push @tmp, '(' . $term . ')';
	}
	if (scalar @tmp){
		$match_str .= ' (' . join(' ', @tmp) . ')';
	}
	
	@tmp = ();
	foreach my $term (@{ $args->{any_field_terms}->{or} }){
		push @tmp, '(' . $term . ')';
	}
	if (scalar @tmp){
		$match_str .= ' (' . join('|', @tmp) . ')';
	}
	
	@tmp = ();
	foreach my $term (@{ $args->{any_field_terms}->{not} }){
		push @tmp, '(' . $term . ')';
	}
	if (scalar @tmp){
		$match_str .= ' !(' . join('|', @tmp) . ')';
	}
	
	foreach my $class_id (sort keys %{ $args->{distinct_classes} }){
		@tmp = ();
		
		# First, the ANDs
		@tmp = ();
		foreach my $field (sort keys %{ $args->{field_terms}->{and}->{$class_id} }){
			push @tmp, '(@' . $field . ' ' . join(' ', @{ $args->{field_terms}->{and}->{$class_id}->{$field} }) . ')';
		}
		if (scalar @tmp){
			$match_str .= ' (' . join(' ', @tmp) . ')';
		}
				
		# Then, the NOTs
		@tmp = ();
		foreach my $field (sort keys %{ $args->{field_terms}->{not}->{$class_id} }){
			push @tmp, '(@' . $field . ' ' . join(' ', @{ $args->{field_terms}->{not}->{$class_id}->{$field} }) . ')';
		}
		if (scalar @tmp){
			$match_str .= ' !(' . join('|', @tmp) . ')';
		}
		
		# Then, the ORs
		@tmp = ();
		foreach my $field (sort keys %{ $args->{field_terms}->{or}->{$class_id} }){
			push @tmp, '(@' . $field . ' ' . join(' ', @{ $args->{field_terms}->{or}->{$class_id}->{$field} }) . ')';
		}
		if (scalar @tmp){
			$match_str .= ' (' . join('|', @tmp) . ')';
		}
	}	
	
	$self->log->trace('match str: ' . $match_str);		
	
	return $match_str;
}

sub _build_sphinx_query {
	my $self = shift;
	my $args = shift;
	
	die('args') unless $args and ref($args) eq 'HASH' and $args->{user_info};
	
	$args->{queries} = []; # place to store our query with our result in a multi-query
	my @or_clause;
	my @or_vals;
	foreach my $type qw(attr_terms field_terms){
		foreach my $boolean qw(and or){
			foreach my $class_id (sort keys %{ $args->{$type}->{$boolean} }){
				$self->log->trace('type: ' . $type . ', boolean: ' . $boolean . ', class_id: ' . $class_id);
				foreach my $field (sort keys %{ $args->{$type}->{$boolean}->{$class_id} }){
					#my $field_hash = $self->_get_field($args, $field)->{$class_id};
					foreach my $value (@{ $args->{$type}->{$boolean}->{$class_id}->{$field} }){
						$self->log->trace('field: ' . $field . ', class: ' . $class_id);
						if ($class_id){
							#$self->log->trace('field_hash: ' . Dumper($field_hash) . ', class_id: ' . $class_id . ', field_order: ' . $field_hash->{field_order } .
							#	', attr: ' . $Field_order_to_attr->{ $field_hash->{field_order } });
							#push @or_clause, '(class_id=? AND ' . $Field_order_to_attr->{ $field_hash->{field_order } } . '=?)';
							push @or_clause, '(class_id=? AND attr_' . $field . '=?)';
							push @or_vals, $class_id, int($value);
						}
						else {
							#push @or_clause, $Field_order_to_attr->{ $field_hash->{field_order } } . '=?';
							push @or_clause, 'attr_' . $field . '=?';
							push @or_vals, int($value);
						}
					}
				} 
			}
		}
	}
	
	foreach my $class_id (sort keys %{ $args->{attr_terms}->{range_and} }){
		foreach my $field (sort keys %{ $args->{attr_terms}->{range_and}->{$class_id} }){
			my $field_hash = $self->_get_field($args, $field)->{$class_id};
			my $raw_attr;
			if ($class_id){
				push @or_clause, '(class_id=? AND ' . $Field_order_to_attr->{ $field_hash->{field_order} } . '>=? AND ' . $Field_order_to_attr->{ $field_hash->{field_order } } . '<=?)';
				push @or_vals, $class_id, $args->{attr_terms}->{range_and}->{$class_id}->{$field}->{min},
					$args->{attr_terms}->{range_and}->{$class_id}->{$field}->{max};
			}
			else {
				push @or_clause, '(' . $Field_order_to_attr->{ $field_hash->{field_order} } . '>=? AND ' . $Field_order_to_attr->{ $field_hash->{field_order } } . '<=?)';
				push @or_vals, $args->{attr_terms}->{range_and}->{$class_id}->{$field}->{min},
					$args->{attr_terms}->{range_and}->{$class_id}->{$field}->{max};
			}
			
		}
	}
		
	my @not_or_clause;
	my @not_or_vals;
	
	foreach my $type qw(attr_terms field_terms){
		foreach my $class_id (sort keys %{ $args->{$type}->{not} }){
			foreach my $field (sort keys %{ $args->{$type}->{not}->{$class_id} }){
				my $field_hash = $self->_get_field($args, $field)->{$class_id};
				foreach my $value (@{ $args->{$type}->{not}->{$class_id}->{$field} }){
					if ($class_id){
						push @not_or_clause, '(class_id=? AND ' . $Field_order_to_attr->{ $field_hash->{field_order} } . '=?)';
						push @not_or_vals, $class_id, $value;
					}
					else {
						push @not_or_clause, $Field_order_to_attr->{ $field_hash->{field_order} } . '=?';
						push @not_or_vals, $value;
					}
				}
			}
		}
	}
	foreach my $class_id (sort keys %{ $args->{attr_terms}->{range_not} }){
		foreach my $field (sort keys %{ $args->{attr_terms}->{range_not}->{$class_id} }){
			my $field_hash = $self->_get_field($args, $field)->{$class_id};
			my $raw_attr = $Field_order_to_attr->{ $field_hash->{field_order} };
			if ($class_id){
				push @not_or_clause, '(class_id=? AND ' . $raw_attr . '>=? AND ' . $raw_attr . '<=?)';
				push @not_or_vals, $class_id, $args->{attr_terms}->{range_not}->{$class_id}->{$field}->{min}, 
					$args->{attr_terms}->{range_not}->{$class_id}->{$field}->{max};
			}
			else {
				push @not_or_clause, '(' . $raw_attr . '>=? AND ' . $raw_attr . '<=?)';
				push @not_or_vals, $args->{attr_terms}->{range_not}->{$class_id}->{$field}->{min}, 
					$args->{attr_terms}->{range_not}->{$class_id}->{$field}->{max};
			}
		}
	}
	
	my $positive_qualifier;
	if (@or_clause){
		$positive_qualifier = join(' OR ', @or_clause);
	}
	else {
		$positive_qualifier = 1;
	}
	my $negative_qualifier;
	if (@not_or_clause){
		$negative_qualifier = join(' OR ', @not_or_clause);
	}
	else {
		$negative_qualifier = 0;
	}
	my $select = "SELECT *, $positive_qualifier AS positive_qualifier, $negative_qualifier AS negative_qualifier";
	my $where = 'MATCH(\'' . $self->_build_sphinx_match_str($args) .'\') AND positive_qualifier=1 AND negative_qualifier=0';
	my @values = (@or_vals, @not_or_vals);
	
	# Check for no-class super-user query
	unless (($args->{user_info}->{permissions}->{class_id}->{0} and not (scalar keys %{ $args->{given_classes} }))
		or $args->{groupby}){
		$where .= ' AND class_id IN (' . join(',', map { '?' } keys %{ $args->{distinct_classes} }) . ')';
		push @values, sort keys %{ $args->{distinct_classes} };
	}
	# Check for time given
	if ($args->{start_int} and $args->{end_int}){
		$where .= ' AND timestamp BETWEEN ? AND ?';
		push @values, $args->{start_int}, $args->{end_int};
	}
	
	# Add a groupby query if necessary
	my $groupby;	
	if ($args->{groupby}){
		#foreach my $field (@{ $args->{groupby} }){
			my $field_infos = $self->_get_field($args, $args->{groupby});
			$self->log->trace('field_infos: ' . Dumper($field_infos));
			foreach my $class_id (keys %{$field_infos}){
				push @{ $args->{queries} }, {
					select => $select,
					where => $where . ($class_id ? ' AND class_id=?' : ''),
					values => [ @values, $class_id ? $class_id : () ],
					groupby => $Field_order_to_attr->{ $field_infos->{$class_id}->{field_order} },
				};
			}
		#}
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

1;