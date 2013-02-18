package API;
use Moose;
with 'MooseX::Traits';
with 'Utils';
with 'Fields';
use Data::Dumper;
use Date::Manip;
use AnyEvent;
use DBI;
use MIME::Base64;
use Socket qw(inet_aton inet_ntoa);
use CHI;
use Time::HiRes qw(time);
use Time::Local;
use Module::Pluggable require => 1, search_path => [ qw( Export Info Transform Connector Datasource ) ];
use URI::Escape qw(uri_unescape);
use Mail::Internet;
use Email::LocalDelivery;
use Carp;
use Log::Log4perl::Level;
use Storable qw(freeze thaw);

use User;
use Query;
use Results;
use AsyncMysql;

our $Max_limit = 1000;
our $Max_query_terms = 128;
our $Livetail_poll_interval = 5;
our $Scheduled_query_cols = { map { $_ => 1 } (qw(id username query frequency start end connector params enabled last_alert alert_threshold)) };

has 'ldap' => (is => 'rw', isa => 'Object', required => 0);
has 'last_error' => (is => 'rw', isa => 'Str', required => 1, default => '');
has 'cache' => (is => 'rw', isa => 'Object', required => 1, default => sub { return CHI->new( driver => 'RawMemory', global => 1) });
has 'warnings' => (traits => [qw(Array)], is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] },
	handles => { 'has_warnings' => 'count', 'add_warning' => 'push', 'clear_warnings' => 'clear' });
has 'system_datasources' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { {
} });
has 'web_datasources' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { {
	_system_web_queries_count => {
		query_template => 'SELECT %s FROM (SELECT username, timestamp, milliseconds, archive, num_results FROM query_log t1 JOIN users t2 ON (t1.uid=t2.uid) WHERE %s) derived %s ORDER BY %s LIMIT %d,%d',
		fields => [
			{ name => 'username' },
			{ name => 'timestamp', type => 'timestamp', alias => 'timestamp' },
		],
	},
	_system_web_queries_time => {
		query_template => 'SELECT %s FROM (SELECT username, timestamp, milliseconds, archive, num_results FROM query_log t1 JOIN users t2 ON (t1.uid=t2.uid) WHERE %s) derived %s ORDER BY %s LIMIT %d,%d',
		fields => [
			{ name => 'username' },
			{ name => 'timestamp', type => 'timestamp', alias => 'timestamp' },
			{ name => 'milliseconds', type => 'int', alias => 'count' }
		],
	},
} });
has 'node_datasources' => (traits => [qw(Hash)], is => 'rw', isa => 'HashRef', required => 1, default => sub { {
	_system_event_rates => { 
		alias => '_node_stats_%s', # changed from %d to %s because of architecture problems with %d and unsigned integers
		dsn => 'dbi:mysql:host=%s;port=%d;database=%s',
		query_template => 'SELECT %s FROM (SELECT host_id, INET_NTOA(host_id) AS host, timestamp, class, count FROM host_stats t1 JOIN classes t2 ON (t1.class_id=t2.id) WHERE %s) derived %s ORDER BY %s LIMIT %d,%d',
		fields => [
			{ name => 'host_id', type => 'ip_int' },
			{ name => 'host' },
			{ name => 'timestamp', type => 'timestamp', alias => 'timestamp' },
			{ name => 'class' },
			{ name => 'count', type => 'int', alias => 'count' }
		],
	}
} });

sub BUILD {
	my $self = shift;
	
	if ( uc($self->conf->get('auth/method')) eq 'LDAP' ) {
		require Net::LDAP::Express;
		require Net::LDAP::FilterBuilder;
		$self->ldap($self->_get_ldap());
	}
	
	# setup dynamic (config based) plugins
	if ($self->conf->get('transforms/database')){
		foreach my $db_lookup_plugin (keys %{ $self->conf->get('transforms/database') }){
			my $conf = $self->conf->get('transforms/database/' . $db_lookup_plugin);
			my $alias = delete $conf->{alias};
			my $metaclass = Moose::Meta::Class->create( 'Transform::' . $alias, 
				superclasses => [ 'Transform::Database' ],
			);
			foreach my $attr (keys %$conf){
				$metaclass->add_attribute($attr => (is => 'rw', default => sub { $conf->{$attr} } ) );
			}
			# Set name
			$metaclass->add_attribute('name' => (is => 'rw', default => $db_lookup_plugin ) );
		}
	}
	
	# Setup system datasources
	foreach my $datasource_type (keys %{ $self->web_datasources }){
		my $template_conf = $self->web_datasources->{$datasource_type};
		$self->system_datasources->{$datasource_type} = [];
		my $conf = { 
			alias => $datasource_type,
			dsn => $self->conf->get('meta_db/dsn'), 
			username => $self->conf->get('meta_db/username'),
			password => $self->conf->get('meta_db/password'),
			query_template => $template_conf->{query_template},
			fields => $template_conf->{fields},
		};
		
		my $metaclass = Moose::Meta::Class->create( 'Datasource::' . $conf->{alias}, 
			superclasses => [ 'Datasource::Database' ],
		);
		foreach my $attr (keys %$conf){
			$metaclass->add_attribute($attr => (is => 'rw', default => sub { $conf->{$attr} } ) );
		}
		# Set name
		$metaclass->add_attribute('name' => (is => 'rw', default => $conf->{alias} ) );
		#push @{ $self->system_datasources->{$datasource_type} }, $conf->{alias};
		$self->system_datasources->{$datasource_type} = 1;
	}
	foreach my $datasource_type (keys %{ $self->node_datasources }){
		my $template_conf = $self->node_datasources->{$datasource_type};
		$self->system_datasources->{$datasource_type} = [];
		foreach my $node (keys %{ $self->conf->get('nodes') }){
			my $conf = { 
				alias => sprintf($template_conf->{alias}, unpack('N*', inet_aton($node))),
				dsn => sprintf($template_conf->{dsn}, $node, 
					($self->conf->get('nodes/' . $node . '/port') ? $self->conf->get('nodes/' . $node . '/port') : 3306),
					$self->conf->get('nodes/' . $node . '/db')),
				username => $self->conf->get('nodes/' . $node . '/username'),
				password => $self->conf->get('nodes/' . $node . '/password'),
				query_template => $template_conf->{query_template},
				fields => $template_conf->{fields},
			};
			
			my $metaclass = Moose::Meta::Class->create( 'Datasource::' . $conf->{alias}, 
				superclasses => [ 'Datasource::Database' ],
			);
			foreach my $attr (keys %$conf){
				$metaclass->add_attribute($attr => (is => 'rw', default => sub { $conf->{$attr} } ) );
			}
			# Set name
			$metaclass->add_attribute('name' => (is => 'rw', default => $conf->{alias} ) );
			push @{ $self->system_datasources->{$datasource_type} }, $conf->{alias};
		}
	}
	#$self->log->debug('$self->system_datasources: ' . Dumper($self->system_datasources));
	
	# Setup custom datasources
	if ($self->conf->get('datasources')){
		foreach my $datasource_class (keys %{ $self->conf->get('datasources') }){
			# Upper case the first letter
			my @class_name_letters = split(//, $datasource_class);
			$class_name_letters[0] = uc($class_name_letters[0]);
			my $datasource_class_name = join('', @class_name_letters);
			
			foreach my $datasource_plugin (keys %{ $self->conf->get('datasources/' . $datasource_class) }){
				my $conf = $self->conf->get('datasources/' . $datasource_class . '/' . $datasource_plugin);
				die('No conf found for ' . 'datasources/' . $datasource_class . '/' . $datasource_plugin) unless $conf;
				my $alias = delete $conf->{alias};
				$alias ||= $datasource_plugin;
				my $metaclass = Moose::Meta::Class->create( 'Datasource::' . $alias, 
					superclasses => [ 'Datasource::' . $datasource_class_name ],
				);
				foreach my $attr (keys %$conf){
					$metaclass->add_attribute($attr => (is => 'rw', default => sub { $conf->{$attr} } ) );
				}
				# Set name
				$metaclass->add_attribute('name' => (is => 'rw', default => $datasource_plugin ) );
			}
		}
	}
	
	# init plugins
	$self->plugins();
	
	# Update livetail_poll_interval if necessary
	if ($self->conf->get('livetail/poll_interval')){
		$Livetail_poll_interval = $self->conf->get('livetail/poll_interval');
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

sub get_user {
	my $self = shift;
	my $username = shift;
	
	my $user = User->new(username => $username, conf => $self->conf);
	$self->resolve_field_permissions($user);
	return $user;
}

sub get_stored_user {
	my $self = shift;
	my $user_info = shift;
	
	my ($class, $params) = User->thaw($user_info);
	return $class->new(%$params);
}

sub get_user_info {
	my $self = shift;
	my $user_info = shift;
	
	my ($class, $params) = User->thaw($user_info);
	return $params;
	
}

sub get_saved_result {
	my ($self, $args) = @_;
	
	unless ($args and ref($args) eq 'HASH' and $args->{qid}){
		$self->log->error('Invalid args: ' . Dumper($args));
		return;
	}
	
	# Authenticate the hash if given (so that the uid doesn't have to match)
	if ($args->{hash} and $args->{hash} ne $self->get_hash($args->{qid}) ){
		$self->_error(q{You are not authorized to view another user's saved queries});
		return;
	}
	
	my @values = ($args->{qid});
	
	my ($query, $sth);
	$query = 'SELECT t2.uid, t2.query, milliseconds FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid)' . "\n" .
		'WHERE t1.qid=?';
	if (not $args->{hash}){
		$query .= ' AND uid=?';
		push @values, $args->{user}->uid;
	}
	
	$sth = $self->db->prepare($query);
	$sth->execute(@values);
	my $row = $sth->fetchrow_hashref;
	unless ($row){
		$self->_error('No saved results for qid ' . $args->{qid} . ' found.');
		return;
	}
	
	$query = 'SELECT data FROM saved_results_data WHERE qid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{qid});
	$row = $sth->fetchrow_hashref;
	
	my $data = $self->json->decode($row->{data});
	$self->log->debug('returning data: ' . Dumper($data));
	if (ref($data) and ref($data) eq 'ARRAY'){
		return { results => $data };
	}
	else {
		return $data;
	}
}

sub _error {
	my $self = shift;
	my $err = shift;
	$self->log->error($err);
	return $self->last_error($err);
}

sub get_permissions {
	my ($self, $args) = @_;
		
	my $form_params = $self->get_form_params($args->{user});
	
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
		'WHERE groupname LIKE CONCAT("%", ?, "%")' . "\n" . 
		'GROUP BY t1.gid' . "\n" .
		'ORDER BY t1.gid ASC';
	
	my @values;
	if ($args->{search}){
		push @values, $args->{search};
	}
	else {
		push @values, '';
	}
	$sth = $self->db->prepare($query);
	$sth->execute(@values);
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
					$exceptions{ $row->{attr} }->{ $row->{attr_id} } = $row->{attr_id};
				}
				$self->log->debug('attr=' . $row->{attr} . ', attr_id=' . $row->{attr_id} . ', attr_value=' . $row->{attr_value});
			}
		}
		$ldap_entry->{_exceptions} = { %exceptions };
	}
	
	$query = 'SELECT gid, filter FROM filters WHERE gid=?';
	$sth = $self->db->prepare($query);
	my %filters;
	foreach my $ldap_entry (@ldap_entries){
		$sth->execute($ldap_entry->{gid});
		while (my $row = $sth->fetchrow_hashref){
			$ldap_entry->{_exceptions}->{filter}->{ $row->{filter} } = $row->{filter};
		}
	}
	
	my $permissions = {
		totalRecords => scalar @ldap_entries,
		records_returned => scalar @ldap_entries,
		results => [ @ldap_entries ],	
	};
	
	$permissions->{form_params} = $form_params;
	
	return $permissions;
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
	my $rows_updated = 0;
	foreach my $perm (@{ $args->{permissions} }){
		my $short_attr = $perm->{attr};
		$short_attr =~ /([^\.]+)$/;
		$short_attr = $1;
		if ($Fields::IP_fields->{ $short_attr } and $perm->{attr_id} =~ /(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\-(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/){
			$perm->{attr_id} = unpack('N*', inet_aton($1)) . '-' . unpack('N*', inet_aton($2));
		}
		elsif ($Fields::IP_fields->{ $short_attr } and $perm->{attr_id} !~ /^[\d\-]+$/){
			$perm->{attr_id} = unpack('N*', inet_aton($perm->{attr_id}));
		}
		
		$self->log->info('Changing permissions: ' . join(', ', $args->{action}, $perm->{gid}, $perm->{attr}, $perm->{attr_id}));
		if ($args->{action} eq 'add'){
			if ($perm->{attr} eq 'filter'){
				$query = 'INSERT INTO filters (gid, filter) VALUES(?,?)';
				$sth = $self->db->prepare($query);
				$sth->execute($perm->{gid}, $perm->{attr_id});
			}
			else {
				$query = 'INSERT INTO permissions (gid, attr, attr_id) VALUES (?,?,?)';
				$sth = $self->db->prepare($query);
				$sth->execute($perm->{gid}, $perm->{attr}, $perm->{attr_id});
			}
		}
		elsif ($args->{action} eq 'delete'){
			if ($perm->{attr} eq 'filter'){
				$query = 'DELETE FROM filters WHERE gid=? AND filter=?';
				$sth = $self->db->prepare($query);
				$sth->execute($perm->{gid}, $perm->{attr_id});
			}
			else {
				$query = 'DELETE FROM permissions WHERE gid=? AND attr=? AND attr_id=?';
				$sth = $self->db->prepare($query);
				$sth->execute($perm->{gid}, $perm->{attr}, $perm->{attr_id});
			}
		}
		$rows_updated += $sth->rows;
	}
	return {success => $rows_updated, groups_changed => $rows_updated};	
}

#sub _revalidate_group {
#	my ( $self, $gid ) = @_;
#	
#	my $members = $self->_get_group_members($gid);
#	unless ($members and ref($members) eq 'ARRAY' and scalar @$members){
#		$self->log->error('No members found for gid ' . $gid);
#		return;
#	}
#	my ($query, $sth);
#	$query = 'SELECT uid FROM users WHERE username=?';
#	$sth = $self->db->prepare($query);
#	my %must_revalidate;
#	foreach my $member (@$members){
#		$sth->execute($member);
#		my $row = $sth->fetchrow_hashref;
#		if ($row){
#			$must_revalidate{ $row->{uid} } = 1;
#			$self->log->info('User ' . $member . ' must revalidate');
#		}
#	}
#	#TODO find and expire these sessions
#}

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
	
	if ( uc($self->conf->get('auth/method')) eq 'LDAP' ) {
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
	elsif (lc($self->conf->get('auth/method')) eq 'db'){
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
	my $user = $args->{user};
		
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
	
	$stats->{nodes} = $self->_get_nodes($user);
		
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
		foreach my $item (qw(load archive index)){
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
			$self->log->trace('get stat ' . $item . ' for node ' . $node);
			$stats->{nodes}->{$node}->{dbh}->query($query, sub {
				my ($dbh, $rows, $rv) = @_;
				#$self->log->trace('got stat ' . $item . ' for node ' . $node . ': ' . Dumper($rows));
				$load_stats->{$item}->{summary} = $rows->[0];
				$cv->end;
			
				my $query = 'SELECT UNIX_TIMESTAMP(timestamp) AS ts, timestamp, bytes, count FROM stats WHERE type=? AND timestamp BETWEEN ? AND ?';
				$cv->begin;
				$stats->{nodes}->{$node}->{dbh}->query($query, sub {
					my ($dbh, $rows, $rv) = @_;
					unless ($intervals and $load_stats->{$item}->{summary} and $load_stats->{$item}->{summary}->{total_time}){
						$self->log->error('no stat for node ' . $node . ' and stat ' . $item);
						$cv->end;
						return;
					}
					#$self->log->trace('$load_stats->{$item}->{summary}: ' . Dumper($load_stats->{$item}->{summary}));
					# arrange in the number of buckets requested
					my $bucket_size = ($load_stats->{$item}->{summary}->{total_time} / $intervals);
					unless ($bucket_size){
						$self->log->error('no bucket size ' . $node . ' and stat ' . $item);
						$cv->end;
						return;
					}
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
			},
			$item, $args->{start}, $args->{end});
		}
		$cv->end;
		$cv->recv;	
		$stats->{nodes}->{$node} = $load_stats;
	}
	
	$self->log->trace('received');
		
	# Combine the stats info for the nodes
	my $combined = {};
	$self->log->debug('got stats: ' . Dumper($stats->{nodes}));
	
	foreach my $stat (qw(load index archive)){
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

sub _get_sphinx_nodes {
	my $self = shift;
	my $q = shift;
	my %nodes;
	my $node_conf = $self->conf->get('nodes');
	
	foreach my $node (keys %$node_conf){
		if (scalar keys %{ $q->nodes->{given} }){
			next unless $q->nodes->{given}->{$node};
		}
		elsif (scalar keys %{ $q->nodes->{excluded} }){
			next if $q->nodes->{excluded}->{$node};
		}
		
		my $db_name = 'syslog';
		if ($node_conf->{$node}->{db}){
			$db_name = $node_conf->{$node}->{db};
		}
		
		my $mysql_port = 3306;
		if ($node_conf->{$node}->{port}){
			$mysql_port = $node_conf->{$node}->{port};
		}
				
		my $sphinx_port = 9306;
		if ($node_conf->{$node}->{sphinx_port}){
			$sphinx_port = $node_conf->{$node}->{sphinx_port};
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
			
			$self->log->trace('connecting to sphinx on node ' . $node);
			
			$nodes{$node}->{sphinx} = AsyncMysql->new(log => $self->log, db_args => [
				'dbi:mysql:port=' . $sphinx_port .';host=' . $node, undef, undef,
				{
					mysql_connect_timeout => $self->db_timeout,
					PrintError => 0,
					mysql_multi_statements => 1,
					mysql_bind_type_guessing => 1,
				}
			]);
		};
		if ($@){
			$self->add_warning($@);
			delete $nodes{$node};
		}		
	}
	
	return \%nodes;
}

sub get_form_params {
	my ( $self, $user) = @_;
	
	eval {	
		$self->node_info($self->_get_node_info($user));
	};
	if ($@){
		$self->add_warning($@);
		$self->log->error($@);
		return;
	}
	#$self->log->trace('got node_info: ' . Dumper($self->node_info));
	my $form_params;
	
	eval {			
		$form_params = {
			start => $self->node_info->{indexes_min} ? epoch2iso($self->node_info->{indexes_min}) : epoch2iso($self->node_info->{archive_min}),
			start_int => $self->node_info->{indexes_min} ? $self->node_info->{indexes_min} : $self->node_info->{archive_min},
			display_start_int => $self->node_info->{indexes_min} ? $self->node_info->{indexes_min} : $self->node_info->{archive_min},
			archive_start => epoch2iso($self->node_info->{archive_min}),
			archive_start_int => $self->node_info->{archive_min},
			archive_display_start_int => $self->node_info->{archive_min},
			end => $self->node_info->{indexes_max} ? epoch2iso($self->node_info->{indexes_max}) : epoch2iso($self->node_info->{archive_max}),
			end_int => $self->node_info->{indexes_max} ? $self->node_info->{indexes_max} : $self->node_info->{archive_max},
			archive_end => epoch2iso($self->node_info->{archive_max}),
			archive_end_int => $self->node_info->{archive_max},
			classes => $self->node_info->{classes},
			classes_by_id => $self->node_info->{classes_by_id},
			fields => $self->node_info->{fields},
			nodes => [ keys %{ $self->node_info->{nodes} } ],
			groups => $user->groups,
			additional_display_columns => $self->conf->get('additional_display_columns') ? $self->conf->get('additional_display_columns') : [],
			totals => $self->node_info->{totals},
			livetail_poll_interval => $Livetail_poll_interval,
			preferences => $user->preferences,
		};
		
		# You can change the default start time displayed to web users by changing this config setting
		if ($self->conf->get('default_start_time_offset')){
			$form_params->{display_start_int} = ($form_params->{end_int} - (86400 * $self->conf->get('default_start_time_offset')));
		}
		
		
		if ($user->username ne 'system'){
			# this is for a user, restrict what gets sent back
			unless ($user->permissions->{class_id}->{0}){
				foreach my $class_id (keys %{ $form_params->{classes} }){
					unless ($user->permissions->{class_id}->{$class_id}){
						delete $form_params->{classes}->{$class_id};
					}
				}
			
				my $possible_fields = [ @{ $form_params->{fields} } ];
				$form_params->{fields} = [];
				for (my $i = 0; $i < scalar @$possible_fields; $i++){
					my $field_hash = $possible_fields->[$i];
					my $class_id = $field_hash->{class_id};
					if ($user->permissions->{class_id}->{$class_id}){
						push @{ $form_params->{fields} }, $field_hash;
					}
				}
			}
		}
		
		# Tack on the "ALL" and "NONE" special types
		unshift @{$form_params->{fields}}, 
			{'value' => 'ALL', 'text' => 'ALL', 'class_id' => 0 }, 
			{'value' => 'NONE', 'text' => 'NONE', 'class_id' => 1 };
		
		$form_params->{schedule_actions} = $self->_get_schedule_actions($user);
	};
	if ($@){
		$self->log->error('Error getting form params: ' . $@);
	}
		
	return $form_params;
}

sub _get_schedule_actions {
	my ($self, $user) = @_;
	
	my @ret;
	foreach my $plugin ($self->plugins()){
		if ($plugin =~ /^Connector::(\w+)/){
			unless ($user->is_admin){
				next if $plugin->admin_required;
			}
			my $desc = $plugin->description;
			$self->log->debug('plugin: ' . $plugin . ', desc: ' . "$desc");
			push @ret, { action => $1 . '()', description => $desc };
		}
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
	my $orderby = 'id';
	if ($args->{sort} and $Scheduled_query_cols->{ $args->{sort} }){
		$orderby = $args->{sort};
	}
	my $dir = 'DESC';
	if ($args->{dir} eq 'asc'){
		$dir = 'ASC';
	}
	
	my ($query, $sth);
	
	$query = 'SELECT COUNT(*) AS totalRecords FROM query_schedule' . "\n" .
		'WHERE uid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{user}->uid);
	my $row = $sth->fetchrow_hashref;
	my $totalRecords = $row->{totalRecords};

	$query = 'SELECT t1.id, query, frequency, start, end, connector, params, enabled, UNIX_TIMESTAMP(last_alert) As last_alert, alert_threshold' . "\n" .
		'FROM query_schedule t1' . "\n" .
		'WHERE uid=?' . "\n" .
		'ORDER BY ' . $orderby . ' ' . $dir . "\n" .
		'LIMIT ?,?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{user}->uid, $offset, $limit);
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

sub get_all_scheduled_queries {
	my ($self, $args) = @_;
	
	if ($args and ref($args) ne 'HASH'){
		$self->_error('Invalid args: ' . Dumper($args));
		return;
	}
	elsif (not $args){
		$args = {};
	}
	
	die('Admin required') unless $args->{user}->is_admin;
	
	my $offset = 0;
	if ( $args->{startIndex} ){
		$offset = sprintf('%d', $args->{startIndex});
	}
	my $limit = 10;
	if ( $args->{results} ) {
		$limit = sprintf( "%d", $args->{results} );
	}
	my $orderby = 'id';
	if ($args->{sort} and $Scheduled_query_cols->{ $args->{sort} }){
		$orderby = $args->{sort};
	}
	my $dir = 'DESC';
	if ($args->{dir} eq 'asc'){
		$dir = 'ASC';
	}
	
	my ($query, $sth);
	
	$query = 'SELECT COUNT(*) AS totalRecords FROM query_schedule';
	$sth = $self->db->prepare($query);
	$sth->execute();
	my $row = $sth->fetchrow_hashref;
	my $totalRecords = $row->{totalRecords};

	$query = 'SELECT t1.id, username, query, frequency, start, end, connector, params, enabled, UNIX_TIMESTAMP(last_alert) As last_alert, alert_threshold' . "\n" .
		'FROM query_schedule t1' . "\n" .
		'JOIN users t2 ON (t1.uid=t2.uid)' . "\n" .
		'ORDER BY ' . $orderby . ' ' . $dir . "\n" .
		'LIMIT ?,?';
	$sth = $self->db->prepare($query);
	$sth->execute($offset, $limit);
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

sub set_preference {
	my ($self, $args) = @_;
	
	foreach my $item (qw(id type name value)){	
		unless (defined $args->{$item}){
			$self->_error('Invalid args, missing arg: ' . $item);
			return;
		}
	}
	
	$args->{uid} = sprintf('%d', $args->{user}->uid);
	
	$self->log->info('Updating preferences: ' . Dumper(($args->{type}, $args->{name}, $args->{value}, $args->{id}, $args->{uid})));
	
	my ($query, $sth);
	$query = 'UPDATE preferences SET type=?, name=?, value=? WHERE id=? AND uid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{type}, $args->{name}, $args->{value}, $args->{id}, $args->{uid});
	
	$query = 'SELECT * FROM preferences WHERE id=? AND uid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{id}, $args->{uid});
	return $sth->fetchrow_hashref;
}

sub add_preference {
	my ($self, $args) = @_;
	
	$args->{uid} = sprintf('%d', $args->{user}->uid);
	
	$self->log->info('Adding new empty preference');
	
	my ($query, $sth);
	$query = 'INSERT INTO preferences (uid, type) VALUES(?, "custom")';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{uid});
	
	$query = 'SELECT * FROM preferences WHERE uid=? ORDER BY id DESC LIMIT 1';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{uid});
	
	return $sth->fetchrow_hashref;
}

sub delete_preference {
	my ($self, $args) = @_;
	
	$args->{uid} = sprintf('%d', $args->{user}->uid);
	
	$self->log->info('Deleting preferences: ' . Dumper(($args->{type}, $args->{name}, $args->{value}, $args->{id}, $args->{uid})));
	
	my ($query, $sth);
	$query = 'DELETE FROM preferences WHERE uid=? AND id=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{uid}, $args->{id});
	
	return { id => $args->{id} };
}

sub schedule_query {
	my ($self, $args) = @_;
	
	foreach my $item (qw(qid days time_unit)){	
		unless (defined $args->{$item}){
			$self->_error('Invalid args, missing arg: ' . $item);
			return;
		}
	}
	
	# Make sure these params are ints
	foreach my $item (qw(qid days time_unit count)){
		next unless $args->{$item};
		$args->{$item} = sprintf('%d', $args->{$item});
	}
	$args->{uid} = sprintf('%d', $args->{user}->uid);
	
	my %standard_vars = map { $_ => 1 } (qw(uid qid days time_unit count connector connector_params));
	my $schedule_query_params = { params => {} };
	foreach my $item (keys %{$args}){
		if ($standard_vars{$item}){
			$schedule_query_params->{$item} = $args->{$item};
		}
		else {
			$schedule_query_params->{params}->{$item} = $args->{$item};
		}
	}
	$schedule_query_params->{params} = $self->json->encode($schedule_query_params->{params});
	
	# Add on the connector params and sanitize
	my @connector_params = split(/,/, $schedule_query_params->{connector_params});
	foreach (@connector_params){
		$_ =~ s/[^a-zA-Z0-9\.\_\-\ ]//g;
	}
	if ($schedule_query_params->{connector} =~ s/\(([^\)]*)\)$//){
		unshift @connector_params, split(/,/, $1);
	}
	$schedule_query_params->{connector} .= '(' . join(',', @connector_params) . ')';
		
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
	$query = 'INSERT INTO query_schedule (uid, query, frequency, start, end, connector, params, last_alert, alert_threshold) VALUES (?, ' . "\n" .
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
	if ($schedule_query_params->{count} and $schedule_query_params->{time_unit}){
		$alert_threshold = $time_unit_map->{ $schedule_query_params->{time_unit} } * $schedule_query_params->{count};
	}
	$sth->execute($schedule_query_params->{uid}, $schedule_query_params->{qid}, $freq_str, time(), (86400 * $days) + time(), 
		$schedule_query_params->{connector}, $schedule_query_params->{params}, $alert_threshold);
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
	unless ($row->{uid} eq $args->{user}->uid or $args->{user}->is_admin){
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
	$sth->execute($args->{user}->uid, $args->{id});
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
	foreach my $item (qw(query frequency start end connector params enabled alert_threshold)){
		$attr_map->{$item} = $item;
	}
	my ($query, $sth);
	my $new_args = {};
	foreach my $given_arg (keys %{ $args }){
		next if $given_arg eq 'id' or $given_arg eq 'user';
		unless ($attr_map->{$given_arg}){
			$self->_error('Invalid arg: ' . $given_arg);
			return;
		}
		
		# Decode
		$args->{$given_arg} = uri_unescape($args->{$given_arg});
		
		# Chop quotes
		$args->{$given_arg} =~ s/^['"](.+)['"]$/$1/;
		
		# Adjust timestamps if necessary
		if ($given_arg eq 'start' or $given_arg eq 'end'){
			$args->{$given_arg} = UnixDate($args->{$given_arg}, '%s');
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
	
	if (defined $args->{qid}){ # came from another Perl program, not the web, so no need to de-JSON
		return $self->_save_results($args);
	}
	
	eval {
		my $comments = $args->{comments};
		my $num_results = $args->{num_results} if $args->{num_results};
		# Replace args so we wipe user, etc.
		$args = $self->json->decode($args->{results});
		$args->{comments} = $comments;
		$args->{num_results} = $num_results if $num_results;
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
	
	$self->_save_results($args);
}

sub _save_results {
	my ($self, $args) = @_;
	
	unless ($args->{qid}){
		$self->log->error('No qid found');
		return;
	}
	
	my ($query, $sth);
	
	$self->db->begin_work;
	$query = 'INSERT INTO saved_results (qid, comments) VALUES(?,?)';
	$sth = $self->db->prepare($query);
	
	$sth->execute($args->{qid}, $args->{comments});
	$query = 'INSERT INTO saved_results_data (qid, data) VALUES (?,?)';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{qid}, $self->json->encode($args));
	
	if ($args->{num_results}){
		$query = 'UPDATE query_log SET num_results=? WHERE qid=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{num_results}, $args->{qid});
	}
		
	$self->db->commit;
	
	$self->log->info('Saved results to qid ' . $args->{qid});
	
	return 1;
}

sub get_saved_searches {
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
	
	my $uid = $args->{user}->uid;
	if ($args->{uid}){
		$uid = sprintf('%d', $args->{uid});
	}
	if ($uid ne $args->{user}->uid and not $args->{user}->is_admin){
		$self->_error(q{You are not authorized to view another user's saved queries});
		return;	
	}
	
	my ($query, $sth);
	$query = 'SELECT COUNT(*) AS totalRecords FROM preferences WHERE uid=? AND type="saved_query"';
	$sth = $self->db->prepare($query);
	$sth->execute($uid);
	my $row = $sth->fetchrow_hashref;
	my $totalRecords = $row->{totalRecords};
	$query = 'SELECT * FROM preferences WHERE uid=? AND type="saved_query" ORDER BY id DESC LIMIT ?,?';
	$sth = $self->db->prepare($query);
	$sth->execute($uid, $offset, $limit);
	my $saved_queries = [];
	while (my $row = $sth->fetchrow_hashref){
		push @$saved_queries, $row;
	}
	$self->log->debug( "saved_queries: " . Dumper($saved_queries) );
	return { 
		totalRecords => $totalRecords,
		recordsReturned => scalar @$saved_queries,
		results => $saved_queries
	};;
}

sub get_saved_results {
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
	
	my $uid = $args->{user}->uid;
	if ($args->{uid}){
		$uid = sprintf('%d', $args->{uid});
	}
	if ($uid ne $args->{user}->uid and not $args->{user}->is_admin){
		$self->_error(q{You are not authorized to view another user's saved queries});
		return;	
	}
	
	
	my $saved_queries;
	if ($args->{qid} and not ($args->{startIndex} or $args->{results})){
		# We're just getting one known query
		$saved_queries = $self->_get_saved_query(sprintf('%d', $args->{qid}));
	}
	else {
		$saved_queries = $self->_get_saved_queries($uid, $offset, $limit, $args->{search});
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
	my ( $self, $uid, $offset, $limit, $search ) = @_;
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

	my @placeholders = ($uid);
	$query =
	    'SELECT t1.qid, t2.query, comments, num_results, UNIX_TIMESTAMP(timestamp) AS timestamp ' . "\n"
	  . 'FROM saved_results t1 JOIN query_log t2 ON (t1.qid=t2.qid) ' . "\n"
	  . 'WHERE uid=?' . "\n";
	if ($search){
		$query .= ' AND SUBSTRING_INDEX(SUBSTRING_INDEX(CONCAT(t2.query, " ", IF(ISNULL(comments), "", comments)), \'"\', 4), \'"\', -1) LIKE CONCAT("%", ?, "%")' . "\n";
		push @placeholders, $search;
	}
	push @placeholders, $offset, $limit;
	$query .= 'ORDER BY qid DESC LIMIT ?,?';
	$self->log->debug(Dumper(\@placeholders));
	$sth = $self->db->prepare($query) or die( $self->db->errstr );
	
	$sth->execute( @placeholders );


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
			hash => $self->get_hash($row->{qid}),
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
	my $uid = $args->{user}->uid;
	
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

sub get_running_archive_query {
	my ($self, $args) = @_;
	
	my ($query, $sth);
	$query = 'SELECT qid, query FROM query_log WHERE uid=? AND archive=1 AND num_results=-1';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{user}->uid);
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
	
	my $q;
	if (ref($args) eq 'Query'){
		# We were given a query object natively
		$q = $args;
	}
	else {
		unless ($args and ref($args) eq 'HASH'){
			die('Invalid query args');
		}
		# Get our node info
		if (not $self->node_info->{updated_at} 
			or ($self->conf->get('node_info_cache_timeout') and ((time() - $self->node_info->{updated_at}) >= $self->conf->get('node_info_cache_timeout')))
			or ($args->{user} and not $args->{user}->is_admin)){
			$self->node_info($self->_get_node_info($args->{user}));
		}
		if ($args->{q}){
			if ($args->{qid}){
				$self->log->level($ERROR);
				$q = new Query(conf => $self->conf, user => $args->{user}, q => $args->{q}, node_info => $self->node_info, qid => $args->{qid});
			}
			else {
				$q = new Query(conf => $self->conf, user => $args->{user}, q => $args->{q}, node_info => $self->node_info);
			}
		}
		elsif ($args->{query_string}){
			$q = new Query(
				conf => $self->conf, 
				user => $args->{user},
				node_info => $self->node_info,
				%$args,
			);
		}
		else {
			delete $args->{user};
			$self->log->error('Bad args: ' . Dumper($args));
			die('Invalid query args, no q or query_string');
		}
	}
	
	foreach my $warning (@{ $q->warnings }){
		$self->add_warning($warning);
	}

	my ($query, $sth);
	
	# Check for batching
	unless ($q->system or $q->livetail){
		my $is_batch = 0;	
		if ($q->analytics or $q->archive){
			# Find estimated query time
			my $estimated_query_time = $self->_estimate_query_time($q);
			$self->log->trace('Found estimated query time ' . $estimated_query_time . ' seconds.');
			my $query_time_batch_threshold = 120;
			if ($self->conf->get('query_time_batch_threshold')){
				$query_time_batch_threshold = $self->conf->get('query_time_batch_threshold');
			}
			if ($estimated_query_time > $query_time_batch_threshold){
				$is_batch = 'Batching because estimated query time is ' . int($estimated_query_time) . ' seconds.';
				$self->log->info($is_batch);
			}
		}
		
		# Batch if we're allowing a huge number of results
		if ($q->limit == 0 or $q->limit > $Results::Unbatched_results_limit){
			$is_batch = q{Batching because an unlimited number or large number of results has been requested.};
			$self->log->info($is_batch);
		}	
			
		if ($is_batch){
			# Check to see if this user is already running an archive query
			$query = 'SELECT qid, uid FROM query_log WHERE archive=1 AND (ISNULL(num_results) OR num_results=-1)';
			$sth = $self->db->prepare($query);
			$sth->execute();
			my $counter = 0;
			while (my $row = $sth->fetchrow_hashref){
				if ($row->{uid} eq $args->{user}->uid){
					$self->_error('User ' . $args->{user}->username . ' already has an archive query running: ' . $row->{qid});
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
		
		if ($is_batch){
			# Cron job will pickup the query from the query log and execute it from here if it's an archive query.
			$q->batch_message($is_batch . '  You will receive an email with your results.');
			$q->batch(1);
			return $q;
		}
	}
	
	# Execute search
	if (not $q->datasources->{sphinx}){
		$self->_external_query($q);
	}
	elsif ($q->livetail){
		$self->_livetail_query($q);
	}
	elsif ($q->archive){
		$self->_archive_query($q);
	}
	elsif ($q->analytics or ($q->limit > $Max_limit)){
		$self->_unlimited_sphinx_query($q);
	}
	else {
		$self->_sphinx_query($q);
	}
	
	$self->log->info(sprintf("Query " . $q->qid . " returned %d rows", $q->results->records_returned));
	
	$q->time_taken(int((Time::HiRes::time() - $q->start_time) * 1000)) unless $q->livetail;

	# Apply transforms
	if ($q->has_transforms){	
		$self->transform($q);
	}
	
	# Send to connectors
	if ($q->has_connectors){
		$self->send_to($q);
	}

	return $q;
}

sub _estimate_query_time {
	my ($self, $q) = @_;
	
	my $query_time = 0;
	
	if ($q->archive){
		my $largest = 0;
		
		my $archive_query_rows_per_second = 300_000; # guestimate
		if ($self->conf->get('archive_query_rows_per_second')){
			$archive_query_rows_per_second = $self->conf->get('archive_query_rows_per_second');
		}
		
		# For every node, find the total rows that will have to be searched and use the largest value (each node queries in parallel).
		foreach my $node (keys %{ $q->node_info->{nodes} }){
			my $node_info = $q->node_info->{nodes}->{$node};
			my $total_rows = 0;
			foreach my $table (@{ $node_info->{tables}->{tables} }){
				next unless $table->{table_type} eq 'archive';
				if ($q->start and $q->end){
					if ((($q->start >= $table->{start_int} and $q->start <= $table->{end_int})
						or ($q->end >= $table->{start_int} and $q->end <= $table->{end_int})
						or ($q->start <= $table->{start_int} and $q->end >= $table->{end_int})
						or ($table->{start_int} <= $q->start and $table->{end_int} >= $q->end))
					){
						$self->log->trace('including ' . ($table->{max_id} - $table->{min_id}) . ' rows');
						$total_rows += ($table->{max_id} - $table->{min_id});
					}
				}
				else {
					$self->log->trace('including ' . ($table->{max_id} - $table->{min_id}) . ' rows');
					$total_rows += ($table->{max_id} - $table->{min_id});
				}
			}
			if ($total_rows > $largest){
				$largest = $total_rows;
				$self->log->trace('found new largest ' . $largest);
			}
		}
		$query_time = $largest / $archive_query_rows_per_second;
	}
	else {
		# Do a query with a cutoff=1 to find the total number of docs to be filtered through and apply an estimate
		my ($save_cutoff, $save_limit) = ($q->cutoff, $q->limit);
		$q->cutoff(1);
		$q->limit(1);
		$self->_sphinx_query($q);
		
		my $sphinx_filter_rows_per_second = 500_000; # guestimate of how many found hits/sec/node sphinx will filter
		if ($self->conf->get('sphinx_filter_rows_per_second')){
			$sphinx_filter_rows_per_second = $self->conf->get('sphinx_filter_rows_per_second');
		}
		
		$self->log->trace('total_docs: ' . $q->results->total_docs);
		$query_time = ($q->results->total_docs / $sphinx_filter_rows_per_second / (scalar keys %{ $q->node_info->{nodes} }));
		
		# Reset to original vals
		$q->cutoff($save_cutoff);
		$q->limit($save_limit);
	}	
	
	return $query_time;
}

sub get_log_info {
	my ($self, $args) = @_;
	my $user = $args->{user};
	
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
	my $plugins = [];
	
	# Check to see if any connectors (external apps) are available and include
	if ($self->conf->get('connectors')){
		foreach my $conn (keys %{ $self->conf->get('connectors') }){
			unshift @$plugins, 'send_to_' . $conn;
		}
	}
	
	# Get local in case the plugin needs that
	my $remote_ip;
	foreach my $key (qw(srcip dstip ip)){
		if (exists $decode->{$key} and not $self->check_local($decode->{$key})){
			$remote_ip = $decode->{$key};
			$self->log->debug('remote_ip: ' . $key . ' ' . $remote_ip);
			last;
		}
	}
		
	unless ($decode->{class} and $self->conf->get('plugins/' . $decode->{class})){
		# Check to see if there is generic IP information for use with pcap
		if ($self->conf->get('pcap_url')){
			my %ip_fields = ( srcip => 1, dstip => 1, ip => 1);
			foreach my $field (keys %$decode){
				if ($ip_fields{$field}){
					my $plugin = Info::Pcap->new(conf => $self->conf, data => $decode);
					push @$plugins, @{ $plugin->plugins };
					return  { summary => $plugin->summary, urls => $plugin->urls, plugins => $plugins, remote_ip => $remote_ip };
				}
			}
		}
		
		$self->log->debug('no plugins for class ' . $decode->{class});
		$data =  { summary => 'No info.', urls => [], plugins => $plugins };
		return $data;
	}
	
	eval {
		my $plugin = $self->conf->get('plugins/' . $decode->{class})->new(conf => $self->conf, data => $decode);
		push @$plugins, @{ $plugin->plugins };
		$data =  { summary => $plugin->summary, urls => $plugin->urls, plugins => $plugins, remote_ip => $remote_ip };
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

sub check_local {
	my $self = shift;
	my $ip = shift;
	my $ip_int = unpack('N*', inet_aton($ip));
	
	my $subnets = $self->conf->get('transforms/whois/known_subnets');
	return unless $ip_int and $subnets;
	
	foreach my $start (keys %$subnets){
		if (unpack('N*', inet_aton($start)) <= $ip_int 
			and unpack('N*', inet_aton($subnets->{$start}->{end})) >= $ip_int){
			return 1;
		}
	}
}

sub _sphinx_query {
	my ($self, $q) = @_;
	
	my $queries = $self->_build_query($q);
	
	my $nodes = $self->_get_sphinx_nodes($q);
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
	
	unless (scalar keys %$nodes){
		die('No nodes available');
	}
	
	# Get indexes from all nodes in parallel
	my $cv = AnyEvent->condvar;
	$cv->begin(sub {
		$cv->send;
	});
	
	foreach my $node (keys %$nodes){
		$ret->{$node} = {};
		my $node_info = $self->node_info->{nodes}->{$node};
		# Prune indexes
		my @index_arr;
		
		my $total_rows_searched = 0;
		my $distributed_threshold = 1_000_000_000;
		if ($self->conf->get('distributed_threshold')){
			$distributed_threshold = $self->conf->get('distributed_threshold');
		}
		
		if ($q->start and $q->end){
			foreach my $index (@{ $node_info->{indexes}->{indexes} }){
				if (
					($q->start >= $index->{start_int} and $q->start <= $index->{end_int})
					or ($q->end >= $index->{start_int} and $q->end <= $index->{end_int})
					or ($q->start <= $index->{start_int} and $q->end >= $index->{end_int})
					or ($index->{start_int} <= $q->start and $index->{end_int} >= $q->end)
				){
					push @index_arr, $index->{name};
					$total_rows_searched += $index->{records};
				}
			}
			# If we are searching more than distributed_threshold rows, then we will query all data in parallel.
			if ($total_rows_searched > $distributed_threshold){
				$self->log->trace('using distributed_local index because total_rows_searched: ' . $total_rows_searched .
					' and distributed_threshold: ' . $distributed_threshold);
				@index_arr = ('distributed_local');
			}
		}
		else {
			# We will use the built-in distributed query to search all indexes which takes advantage of threading
			push @index_arr, 'distributed_local';
		}
			
		my $indexes = join(', ', @index_arr);
		unless ($indexes){
			$self->log->debug('no indexes for node ' . $node);
			next;
		}
		
		eval {
			my @sphinx_queries;
			my @sphinx_values;
			my $start = time();
			foreach my $query (@{ $queries }){
				my $search_query = 'SELECT *, ' . $query->{select} . ' FROM ' . $indexes . ' WHERE ' . $query->{where};
				if (exists $query->{groupby}){
					$search_query = 'SELECT *, COUNT(*) AS _count, ' . $query->{groupby} . ' AS _groupby, ' . $query->{select} . ' FROM ' . $indexes . ' WHERE ' . $query->{where} .
						' GROUP BY ' . $query->{groupby};
				}
				if ($q->orderby){
					$search_query .= ' ORDER BY _orderby ' . $q->orderby_dir;
				}
				$search_query .= ' LIMIT ?,? OPTION ranker=none';
				if ($q->cutoff){
					$search_query .= ',cutoff=' . $q->cutoff;
				}
				push @sphinx_values, @{ $query->{values } }, $q->offset, $q->limit;
				$self->log->debug('sphinx_query: ' . $search_query . ', values: ' . 
					Dumper($query->{values}));
				push @sphinx_queries, $search_query;
			}
			
			$self->log->trace('sphinx query: ' . join(';', @sphinx_queries));
			$self->log->trace('values: ' . join(',', @sphinx_values));
			$cv->begin;
			$nodes->{$node}->{sphinx}->sphinx(join(';SHOW META;', @sphinx_queries) . ';SHOW META', sub {
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
				
				#$self->log->trace('$ret->{$node}->{meta}: ' . Dumper($ret->{$node}->{meta}));
				if ($result->{meta}->{warning}){
					if ($result->{meta}->{warning} =~ /fullscan requires extern docinfo/){
						unless ($self->has_warnings){
							$self->add_warning('Incomplete results: Query did not contain any search keywords, just filters. See documentation on temporary indexes for details.');
						}
					}
					else {
						$self->add_warning($result->{meta}->{warning});
					}
				}
				
				# Find what tables we need to query to resolve rows
				my %tables;
				my %orderby_map;
				ROW_LOOP: foreach my $row (@$rows){
					$orderby_map{ $row->{id} } = $row->{_orderby};
					foreach my $table_hash (@{ $self->node_info->{nodes}->{$node}->{tables}->{tables} }){
						next unless $table_hash->{table_type} eq 'index' or $table_hash->{table_type} eq 'import';
						if ($table_hash->{min_id} <= $row->{id} and $row->{id} <= $table_hash->{max_id}){
							$tables{ $table_hash->{table_name} } ||= [];
							push @{ $tables{ $table_hash->{table_name} } }, $row->{id};
							next ROW_LOOP;
						}
					}
				}
				
				if (scalar keys %tables){				
					# Go get the actual rows from the dbh
					my @table_queries;
					my @table_query_values;
					foreach my $table (sort keys %tables){
						my $placeholders = join(',', map { '?' } @{ $tables{$table} });
						my $table_query = sprintf("SELECT %1\$s.id,\n" .
							#"DATE_FORMAT(FROM_UNIXTIME(timestamp), \"%%Y/%%m/%%d %%H:%%i:%%s\") AS timestamp,\n" .
							"timestamp,\n" .
							"imports.name AS import_name, imports.description AS import_description, imports.datatype AS import_type, imports.imported AS import_date,\n" .
							"INET_NTOA(host_id) AS host, program, class_id, class, msg,\n" .
							"i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5\n" .
							"FROM %1\$s\n" .
							"LEFT JOIN %2\$s.programs ON %1\$s.program_id=programs.id\n" .
							"LEFT JOIN %2\$s.classes ON %1\$s.class_id=classes.id\n" .
							"LEFT JOIN %2\$s.imports ON %1\$s.host_id=imports.id\n" .
							'WHERE %1$s.id IN (' . $placeholders . ')',
							$table, $nodes->{$node}->{db});
						push @table_queries, $table_query;
						push @table_query_values, @{ $tables{$table} };
					}
					my $table_query = join(';', @table_queries);
					
					$self->log->trace('table query for node ' . $node . ': ' . $table_query 
						. ', placeholders: ' . join(',', @table_query_values));
					$cv->begin;
					$nodes->{$node}->{dbh}->multi_query($table_query, 
						sub { 
							my ($dbh, $rows, $rv) = @_;
							if (not $rv or not ref($rows) or ref($rows) ne 'ARRAY'){
								my $errstr = 'node ' . $node . ' got error ' . $rows;
								$self->log->error($errstr);
								$self->add_warning($errstr);
								$cv->end;
								return;
							}
							elsif (not scalar @$rows){
								$self->log->error('Did not get rows though we had Sphinx results! tables: ' 
									. Dumper(\%tables)); 
							}
							$self->log->trace('node '. $node . ' got db rows: ' . (scalar @$rows));
							
							foreach my $row (@$rows){
								$ret->{$node}->{results} ||= {};
								$row->{node} = $node;
								$row->{node_id} = unpack('N*', inet_aton($node));
								foreach my $import_col (@{ $Fields::Import_fields }){
									unless ($row->{$import_col}){
										delete $row->{$import_col};
									}
								}
								$row->{_orderby} = $orderby_map{ $row->{id} };
								$ret->{$node}->{results}->{ $row->{id} } = $row;
							}
							$cv->end;
						},
						@table_query_values);
					$cv->end; #end sphinx query	
				}
				elsif (@$rows) {
					$self->add_warning('No MySQL tables found for search hits.');
					$self->log->error('No tables found for result. tables: ' . Dumper($self->node_info->{nodes}->{$node}->{tables}));
					$cv->end; #end sphinx query
				}
				else {
					# No results
					$cv->end; #end sphinx query
				}
			}, 0, @sphinx_values);
		};
		if ($@){
			$ret->{$node}->{error} = 'sphinx query error: ' . $@;
			$self->log->error('sphinx query error: ' . $@);
			$cv->end;
		}
	}
	$cv->end; # bookend initial begin
	$cv->recv; # block until all of the above completes
	
	my ($total_records, $records_returned) = (0,0,0);
	#$self->log->debug('conversions: ' . Dumper($self->node_info->{field_conversions}));
	
	if ($q->has_groupby){
		# Swap the host for the import_groupby if necessary
		if ($q->import_groupby){
			my @groupbys = $q->all_groupbys;
			for (my $i = 0; $i < @groupbys; $i++){
				if ($groupbys[$i] eq 'host'){
					$q->groupby->[$i] = $q->import_groupby;
				}
			}
		}
		my %results;
		foreach my $groupby ($q->all_groupbys){
			my %agg;
			foreach my $node (sort keys %$ret){
				# One-off for grouping by node
				if ($groupby eq 'node'){
					$agg{$node} = $ret->{$node}->{meta}->{total_found};
					next;
				}
				foreach my $sphinx_row (@{ $ret->{$node}->{sphinx_rows} }){
					# Be backwards compatible with older sphinxes
					if (exists $sphinx_row->{'@groupby'}){
						$sphinx_row->{_groupby} = delete $sphinx_row->{'@groupby'};
						$sphinx_row->{_count} = delete $sphinx_row->{'@count'};
					}
					# Resolve the _groupby col with the mysql col
					unless (exists $ret->{$node}->{results}->{ $sphinx_row->{id} }){
						$self->log->warn('mysql row for sphinx id ' . $sphinx_row->{id} . ' did not exist');
						next;
					}
					my $key;
					if (exists $Fields::Time_values->{ $groupby }){
						# We will resolve later
						$key = $sphinx_row->{'_groupby'};
					}
					elsif ($groupby eq 'program'){
						$key = $ret->{$node}->{results}->{ $sphinx_row->{id} }->{program};
					}
					elsif ($groupby eq 'class'){
						$key = $ret->{$node}->{results}->{ $sphinx_row->{id} }->{class};
					}
					elsif (exists $Fields::Field_to_order->{ $groupby }){
						# Resolve normally
						$key = $self->resolve_value($sphinx_row->{class_id}, 
							$sphinx_row->{'_groupby'}, $groupby);
					}
					elsif ($q->import_groupby){
						# Resolve with the mysql row
						$key = $ret->{$node}->{results}->{ $sphinx_row->{id} }->{ $q->import_groupby };
					}
					else {
						# Resolve with the mysql row
						my $field_order = $self->get_field($groupby)->{ $sphinx_row->{class_id} }->{field_order};
						#$self->log->trace('resolving with row ' . Dumper($ret->{$node}->{results}->{ $sphinx_row->{id} }));
						$key = $ret->{$node}->{results}->{ $sphinx_row->{id} }->{ $Fields::Field_order_to_field->{$field_order} };
						$key = $self->resolve_value($sphinx_row->{class_id}, $key, $Fields::Field_order_to_field->{$field_order});
						$self->log->trace('field_order: ' . $field_order . ' key ' . $key);
					}
					$agg{ $key } += $sphinx_row->{'_count'};	
				}
			}
			if (exists $Fields::Time_values->{ $groupby }){
				# Sort these in ascending label order
				my @tmp;
				my $increment = $Fields::Time_values->{ $groupby };
				my $use_gmt = $increment >= 86400 ? 1 : 0;
				#my $gmt_offset = timegm(localtime)-timelocal(localtime); 
				foreach my $key (sort { $a <=> $b } keys %agg){
					$total_records += $agg{$key};
					#my $unixtime = timelocal(gmtime(($key * $increment) - $increment)); # convert from GMT 
					my $unixtime = $key * $increment;
					#my $unixtime = ($key * $increment) - $increment; # MySQL rounds up during cast to int, we want rounded down
					#if ($increment > $gmt_offset){
					#	$unixtime -= $gmt_offset; # remove GMT offset
					#}
										
					$self->log->trace('key: ' . $key . ', tv: ' . $increment . 
						', unixtime: ' . $unixtime . ', localtime: ' . (scalar localtime($unixtime)));
					push @tmp, { 
						intval => $unixtime, 
						'_groupby' => epoch2iso($unixtime, $use_gmt), #$self->resolve_value(0, $key, $groupby), 
						'_count' => $agg{$key}
					};
				}
				
				# Fill in zeroes for missing data so the graph looks right
				my @zero_filled;
				
				$self->log->trace('using increment ' . $increment . ' for time value ' . $groupby);
				OUTER: for (my $i = 0; $i < @tmp; $i++){
					push @zero_filled, $tmp[$i];
					if (exists $tmp[$i+1]){
						for (my $j = $tmp[$i]->{intval} + $increment; $j < $tmp[$i+1]->{intval}; $j += $increment){
							#$self->log->trace('i: ' . $tmp[$i]->{intval} . ', j: ' . ($tmp[$i]->{intval} + $increment) . ', next: ' . $tmp[$i+1]->{intval});
							push @zero_filled, { 
								'_groupby' => epoch2iso($j, $use_gmt),
								intval => $j,
								'_count' => 0
							};
							last OUTER if scalar @zero_filled >= $q->limit;
						}
					}
				}
				$results{$groupby} = [ @zero_filled ];
			}
			else { 
				# Sort these in descending value order
				my @tmp;
				foreach my $key (sort { $agg{$b} <=> $agg{$a} } keys %agg){
					$total_records += $agg{$key};
					push @tmp, { intval => $agg{$key}, '_groupby' => $key, '_count' => $agg{$key} };
					last if scalar @tmp >= $q->limit;
				}
				$results{$groupby} = [ @tmp ];
			}
			$records_returned += scalar keys %agg;
		}
		$q->results(Results::Groupby->new(conf => $self->conf, results => \%results, total_records => $total_records));
	}
	else {
		my @tmp;
		foreach my $node (keys %$ret){
			$total_records += $ret->{$node}->{meta}->{total_found};
			foreach my $id (sort { $a <=> $b } keys %{ $ret->{$node}->{results} }){
				my $row = $ret->{$node}->{results}->{$id};
				$row->{datasource} = 'Sphinx';
				$row->{_fields} = [
						{ field => 'host', value => $row->{host}, class => 'any' },
						{ field => 'program', value => $row->{program}, class => 'any' },
						{ field => 'class', value => $row->{class}, class => 'any' },
					];
				my $is_import = 0;
				foreach my $import_col (@{ $Fields::Import_fields }){
					if (exists $row->{$import_col}){
						$is_import++;
						push @{ $row->{_fields} }, { field => $import_col, value => $row->{$import_col}, class => 'any' };
					}
				}
				if ($is_import){
					# Remove host
					shift(@{ $row->{_fields} });
					
					# Add node
					push @{ $row->{_fields} }, { field => 'node', value => $row->{node}, class => 'any' };
				}
				# Resolve column names for fields
				foreach my $col (qw(i0 i1 i2 i3 i4 i5 s0 s1 s2 s3 s4 s5)){
					my $value = delete $row->{$col};
					# Swap the generic name with the specific field name for this class
					my $field = $self->node_info->{fields_by_order}->{ $row->{class_id} }->{ $Fields::Field_to_order->{$col} }->{value};
					if (defined $value and $field){
						# See if we need to apply a conversion
						$value = $self->resolve_value($row->{class_id}, $value, $col);
						push @{ $row->{_fields} }, { 'field' => $field, 'value' => $value, 'class' => $self->node_info->{classes_by_id}->{ $row->{class_id} } };
					}
				}
				push @tmp, $row;
			}
		}
		
		# Now that we've got our results, order by our given order by
		if ($q->orderby_dir eq 'DESC'){
			foreach my $row (sort { $b->{_orderby} <=> $a->{_orderby} } @tmp){
				$q->results->add_result($row);
				last if not $q->analytics and $q->results->records_returned >= $q->limit;
			}
		}
		else {
			foreach my $row (sort { $a->{_orderby} <=> $b->{_orderby} } @tmp){
				$q->results->add_result($row);
				last if not $q->analytics and $q->results->records_returned >= $q->limit;
			}
		}
		
#		# Trim to just the limit asked for unless we're doing analytics
#		foreach my $row (sort { $a->{timestamp} <=> $b->{timestamp} } @tmp){
#			$q->results->add_result($row);
#			last if not $q->analytics and $q->results->records_returned >= $q->limit;
#		}
		$q->results->total_records($total_records);
	}
	
	$self->log->debug('completed query in ' . (time() - $overall_start) . ' with ' . $q->results->records_returned . ' rows');
	
	my $total_docs = 0;
	foreach my $node (keys %$ret){
		foreach my $key (keys %{ $ret->{$node}->{meta} }){
			if ($key =~ /^docs\[/){
				$total_docs += $ret->{$node}->{meta}->{$key};
			}
		}
	}
	
	my %keywords;
	foreach my $node (keys %$ret){
		foreach my $key (keys %{ $ret->{$node}->{meta} }){
			if ($key =~ /^keyword\[(\d+)\]/){
				$keywords{$1} = $ret->{$node}->{meta}->{$key};
			}
		}
	}
	my %keyword_stats;
	foreach my $node (keys %$ret){
		foreach my $key (keys %{ $ret->{$node}->{meta} }){
			$key =~ /^([\w\_\.\@]+)\[(\d+)\]/;
			next unless defined $1 and defined $2;
			$keyword_stats{ $keywords{$2} } ||= {};
			$keyword_stats{ $keywords{$2} }->{$1} += $ret->{$node}->{meta}->{$key} unless $1 eq 'keyword';
		}
	}
	
	foreach my $keyword_id (keys %keyword_stats){
		next unless $total_docs;
		$keyword_stats{$keyword_id}->{percentage} = $keyword_stats{$keyword_id}->{docs} / $total_docs * 100;
	}  
	
	$q->stats({ 
		keywords => \%keyword_stats, 
		total_docs => $total_docs, 
		total_time => (time() - $overall_start),
		docs_filtered_per_sec => ($total_docs / (time() - $overall_start))
	});
	
	return 1;
}

sub _unlimited_sphinx_query {
	my ($self, $q) = @_;

	# Keep running the sphinx query until we have less than $Max_limit results (no-limit query)
	
	my $overall_start = time();
	my $overall_limit = $q->limit ? $q->limit : 100_000_000;
	$q->limit($Max_limit);
	#$q->cutoff($Max_limit);
	my %last_ids;
	foreach my $node (keys %{ $self->node_info->{nodes} }){
		$last_ids{ $node } ||= {};
	}
	my @results;
	my $latest_time = 0;
	my $total = 0;
	my $initial_total = 0;
	
	while ($total < $overall_limit){
		$self->log->trace('total: ' . $total . ', overall_limit: ' . $overall_limit);
		# Check if the query was cancelled
		return if $q->check_cancelled;
		
		my $batch_q = $q->clone;
		$batch_q->results(Results->new(results => []));
		
		# Turn off verbose logging for the search
		my $old_log_level = $self->log->level;
		$self->log->level($ERROR);
		
		# Execute search
		$self->_sphinx_query($batch_q);
		
		# Turn logging back on to normal
		$self->log->level($old_log_level);
		
		unless ($initial_total){
			$initial_total = $batch_q->results->total_records;
			$self->log->trace('found $initial_total of '. $initial_total);
			$q->cutoff($Max_limit);
		}
		$self->log->trace('query got ' . $batch_q->results->records_returned . ' of ' . $batch_q->results->total_records. ' results');
		last if $batch_q->results->records_returned == 0;
		
		# Find latest result and count returned
		my @batch_results;
		if ($batch_q->has_groupby){
			$q->results($batch_q->results);
			return 1;
		}
		else {
			foreach my $record ($batch_q->results->all_results){
				# Check for duplicates (this can happen because of the overlapping timestamp for start/end between batch runs)
				if (exists $last_ids{ $record->{node} }->{ $record->{id} }){
					next;
				}
				$last_ids{ $record->{node} }->{ $record->{id} } = 1;
				
				push @batch_results, $record;
				
				if ($record->{timestamp} > $latest_time){
					$latest_time = $record->{timestamp};
				}
				
				$total++;
				last if $total >= $overall_limit;
			}
		}
		$self->log->debug('found latest time: ' . $latest_time . ' ' .  (scalar localtime($latest_time)));
#		$self->log->debug('$args->{min_ids}: ' . Dumper($args->{min_ids}));
		
		$q->start($latest_time);
		
		# Safety in case there are more than $Max_limit results in one second
		if (scalar @batch_results == 0){ # all were duplicates
			$q->offset($q->offset + $Max_limit);
			next;
		}
		else {
			$q->offset(0);
		}			
		
		$q->results->add_results(\@batch_results);
		$self->log->debug('received: ' . $total .' of ' . $initial_total . ' with overall limit ' . $overall_limit);
		last if $total >= $initial_total;
	}
	
	$q->results->close;
	
	$q->time_taken(int((Time::HiRes::time() - $overall_start) * 1000));		
	$self->log->debug('completed unlimited query in ' . (time() - $overall_start) . ' with ' . $total . ' rows');
	
	return 1;
}

sub get_bulk_file {
	my ($self, $args) = @_;
	
	if ( $args and ref($args) eq 'HASH' and $args->{qid} and $args->{name} ) {
		my ($query, $sth);
		$query = 'SELECT qid FROM query_log WHERE qid=? AND uid=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{qid}, $args->{user}->uid);
		my $row = $sth->fetchrow_hashref;
		unless ($row){
			$self->log->error('No qid ' . $args->{qid} . ' for user ' . Dumper($args->{user}->username));
			return 'No query found for that id for this user';
		}
		
		my $file = Results::get_bulk_file($args->{name});
		die('File ' . $file . ' not found') unless -f $file;
		open($args->{bulk_file_handle}, $file) or die($!);
		
		return { 
			ret => $args->{bulk_file_handle}, 
			mime_type => 'text/plain',
			filename => $args->{name},
		};
	}
	else {
		$self->log->error('Invalid args: ' . Dumper($args));
		return 'Unable to build results object from args';
	}
}

sub _build_sphinx_match_str {
	my ($self, $q) = @_;

	# Create the Sphinx Extended2 matching mode query string to be placed in MATCH()
	
	# No-field match str
	my $match_str = '';
	my (%and, %or, %not);
	foreach my $term (keys %{ $q->terms->{any_field_terms}->{and} }){
		$and{$term} = 1;
	}
		
	my @or = ();
	foreach my $term (keys %{ $q->terms->{any_field_terms}->{or} }){
		$or{$term} = 1;
	}
	
	my @not = ();
	foreach my $term (keys %{ $q->terms->{any_field_terms}->{not} }){
		$not{$term} = 1;
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
	
	my @class_match_strs;
	
	# Merge distinct and partially_permitted
	my %classes;
	foreach my $class_id (keys %{ $q->classes->{distinct} }){
		$classes{$class_id} = 1;
	}
	foreach my $class_id (keys %{ $q->classes->{partially_permitted} }){
		$classes{$class_id} = 1;
	}
	#foreach my $class_id (sort keys %{ $q->classes->{distinct} }, sort keys %{ $q->classes->{partially_permitted} }){
	foreach my $class_id (sort keys %classes){
		(%and, %or, %not) = ();
		my $class_match_str = '';
		# First, the ANDs
		foreach my $field (sort keys %{ $q->terms->{field_terms}->{and}->{$class_id} }){
			foreach my $value (@{ $q->terms->{field_terms}->{and}->{$class_id}->{$field} }){
				$and{'(@' . $field . ' ' . $value . ')'} = 1;
			}
		}
				
		# Then, the NOTs
		foreach my $field (sort keys %{ $q->terms->{field_terms}->{not}->{$class_id} }){
			foreach my $value (@{ $q->terms->{field_terms}->{not}->{$class_id}->{$field} }){
				$not{'(@' . $field . ' ' . $value . ')'} = 1;
			}
		}
		
		# Then, the ORs
		foreach my $field (sort keys %{ $q->terms->{field_terms}->{or}->{$class_id} }){
			foreach my $value (@{ $q->terms->{field_terms}->{or}->{$class_id}->{$field} }){
				$or{'(@' . $field . ' ' . $value . ')'} = 1;
			}
		}
		
		if (scalar keys %and){
			$class_match_str .= ' (' . join(' ', sort keys %and) . ')';
		}
		if (scalar keys %or){
			$class_match_str .= ' (' . join('|', sort keys %or) . ')';
		}
		if (scalar keys %not){
			$class_match_str .= ' !(' . join('|', sort keys %not) . ')';
		}
		push @class_match_strs, $class_match_str if $class_match_str;
	}
	
	if (@class_match_strs){
		$match_str .= ' (' . join('|', @class_match_strs) . ')';
	}	
	
	$self->log->trace('match str: ' . $match_str);		
	
	return $match_str;
}

sub _build_archive_match_str {
	my ($self, $q) = @_;

	# Create the SQL LIKE clause
	
	# No-field match str
	my $match_str = '';
	my (%and, %or, %not);
	foreach my $term (keys %{ $q->terms->{any_field_terms}->{and} }){
		if ($term =~ /^\((\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\|(\d+)\)$/){
			$and{'(msg LIKE "%' . $1 . '%" OR host_id=' . $2 . ')'} = 1;
		}
		else {
			$and{'msg LIKE "%' . $term . '%"'} = 1;
		}
	}
		
	my @or = ();
	foreach my $term (keys %{ $q->terms->{any_field_terms}->{or} }){
		if ($term =~ /^\((\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\|(\d+)\)$/){
			$or{'(msg LIKE "%' . $1 . '%" OR host_id=' . $2 . ')'} = 1;
		}
		else {
			$or{'msg LIKE "%' . $term . '%"'} = 1;
		}
	}
	
	my @not = ();
	foreach my $term (keys %{ $q->terms->{any_field_terms}->{not} }){
		if ($term =~ /^\((\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\|(\d+)\)$/){
			$not{'(msg LIKE "%' . $1 . '%" OR host_id=' . $2 . ')'} = 1;
		}
		else {
			$not{'msg LIKE "%' . $term . '%"'} = 1;
		}
	}
	
	foreach my $class_id (sort keys %{ $q->classes->{distinct} }, sort keys %{ $q->classes->{partially_permitted} }){
		# First, the ANDs
		foreach my $field (sort keys %{ $q->terms->{field_terms}->{and}->{$class_id} }){
			foreach my $value (@{ $q->terms->{field_terms}->{and}->{$class_id}->{$field} }){
				$and{$field . ' LIKE "%' . $value . '%"'} = 1;
			}
		}
				
		# Then, the NOTs
		foreach my $field (sort keys %{ $q->terms->{field_terms}->{not}->{$class_id} }){
			foreach my $value (@{ $q->terms->{field_terms}->{not}->{$class_id}->{$field} }){
				$not{$field . ' LIKE "%' . $value . '%"'} = 1;
			}
		}
		
		# Then, the ORs
		foreach my $field (sort keys %{ $q->terms->{field_terms}->{or}->{$class_id} }){
			foreach my $value (@{ $q->terms->{field_terms}->{or}->{$class_id}->{$field} }){
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

sub _build_query {
	my $self = shift;
	my $q = shift;
	
	my @queries;
	my %clauses = ( 
		classes => { clauses => [], vals => [] }, 
		and => { clauses => [], vals => [] }, 
		or => { clauses => [], vals => [] }, 
		not => { clauses => [], vals => [] },
		permissions =>  { clauses => [], vals => [] },
	);
	
	# Create permissions clauses
	foreach my $attr (qw(class_id host_id program_id node_id)){
		my @clause;
		foreach my $id (keys %{ $q->user->permissions->{$attr} }){
			next unless $id;
			$self->log->trace("Adding id $id to $attr based on permissions");
			if ($Fields::IP_fields->{$attr} and $id =~ /^(\d+)\-(\d+)$/){
				my ($min, $max) = ($1, $2);
				push @clause, '(' . $attr . '>=? AND ' . $attr . '<=?)';
				push @{ $clauses{permissions}->{vals} }, $min, $max;
			}
			else {
				push @clause, $attr . '=?';
				push @{ $clauses{permissions}->{vals} }, $id;
			}
		}
		push @{ $clauses{permissions}->{clauses} }, [ @clause ] if scalar @clause;
	}
	
	my @perm_fields_clause;
	foreach my $class_id (keys %{ $q->user->permissions->{fields} }){
		foreach my $perm_hash (@{ $q->user->permissions->{fields}->{$class_id} }){
			my ($name, $value) = @{ $perm_hash->{attr} };
			if ($value =~ /^(\d+)\-(\d+)$/){
				my ($min, $max) = ($1, $2);
				if ($class_id){
					push @perm_fields_clause, '(class_id=? AND ' . $name . '>=? AND ' . $name . '<=?)';
					push @{ $clauses{permissions}->{vals} }, $class_id, $min, $max;
				}
				else {
					push @perm_fields_clause, '(' . $name . '>=? AND ' . $name . '<=?)';
					push @{ $clauses{permissions}->{vals} }, $min, $max;
				}
			}
			else {
				if ($class_id){
					push @perm_fields_clause, '(class_id=? AND ' . $name . '=?)';
					push @{ $clauses{permissions}->{vals} }, $class_id, $value;
				}
				else {
					push @perm_fields_clause, $name . '=?';
					push @{ $clauses{permissions}->{vals} }, $value;
				}
			}
		}
	}
	
	# Add in any blanket class allow statements for classes which didn't have a specific value restriction 
	#  to make sure the class remains present in the clause.  Otherwise, there won't be any reason for the AND
	#  to succeed when specific values are given in one class but not another since the general one won't be represented
	#  in this specific clause.  It will be redundant with the class clause, but that should not affect performance.
	if (scalar @perm_fields_clause){
		foreach my $class_id (keys %{ $q->classes->{distinct} }){
			unless (exists $q->user->permissions->{fields}->{$class_id}){
				push @perm_fields_clause, 'class_id=?';
				push @{ $clauses{permissions}->{vals} }, $class_id;
			}
		}
	}
	
	push @{ $clauses{permissions}->{clauses} }, [ @perm_fields_clause ] if scalar @perm_fields_clause;

	foreach my $class_id (keys %{ $q->classes->{distinct} }){
		push @{ $clauses{classes}->{clauses} }, [ 'class_id=?' ];
		push @{ $clauses{classes}->{vals} }, $class_id;
	}

	foreach my $class_id (keys %{ $q->classes->{excluded} }){
		push @{ $clauses{not}->{clauses} }, [ 'class_id=?' ];
		push @{ $clauses{not}->{vals} }, $class_id;
	}
	
	# Handle our basic equalities
	foreach my $boolean (qw(and or not)){
		foreach my $field (sort keys %{ $q->terms->{attr_terms}->{$boolean}->{'='} }){
			my @clause;
			foreach my $class_id (sort keys %{ $q->terms->{attr_terms}->{$boolean}->{'='}->{$field} }){
				next unless $q->classes->{distinct}->{$class_id} or $class_id eq 0
					or exists $q->classes->{partially_permitted}->{$class_id};
				foreach my $attr (sort keys %{ $q->terms->{attr_terms}->{$boolean}->{'='}->{$field}->{$class_id} }){
					foreach my $value (@{ $q->terms->{attr_terms}->{$boolean}->{'='}->{$field}->{$class_id}->{$attr} }){
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
			push @{ $clauses{$boolean}->{clauses} }, [ @clause ] if @clause;
		}
	}
	
	# Ranges are tougher: First sort by field name so we can group the ranges for the same field together in an OR
	my %ranges;
	foreach my $boolean (qw(and or not)){
		foreach my $op (sort keys %{ $q->terms->{attr_terms}->{$boolean} }){
			next unless $op =~ /\<|\>/;
			foreach my $field (sort keys %{ $q->terms->{attr_terms}->{$boolean}->{$op} }){
				foreach my $class_id (sort keys %{ $q->terms->{attr_terms}->{$boolean}->{$op}->{$field} }){		
					next unless $q->classes->{distinct}->{$class_id} or $class_id eq 0;
					foreach my $attr (sort keys %{ $q->terms->{attr_terms}->{$boolean}->{$op}->{$field}->{$class_id} }){
						$ranges{$boolean} ||= {};
						$ranges{$boolean}->{$field} ||= {};
						$ranges{$boolean}->{$field}->{$attr} ||= {};
						$ranges{$boolean}->{$field}->{$attr}->{$class_id} ||= {};
						$ranges{$boolean}->{$field}->{$attr}->{$class_id}->{$op} ||= [];
						foreach my $value (sort { $a <=> $b } @{ $q->terms->{attr_terms}->{$boolean}->{$op}->{$field}->{$class_id}->{$attr} }){
							push @{ $ranges{$boolean}->{$field}->{$attr}->{$class_id}->{$op} }, $value;
							# resort in case this is added on
							$ranges{$boolean}->{$field}->{$attr}->{$class_id}->{$op} = 
								[ sort { $a <=> $b } @{ $ranges{$boolean}->{$field}->{$attr}->{$class_id}->{$op} } ];
						}					
					}
				}				
			}
		}
	}
	
	# Then divine which range operators go together by sorting them and dequeuing the appropriate operator until there are none left
	foreach my $boolean (qw(and or not)){
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
						if ($max < $min){
							die('max was less than min');
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
	if (@{ $clauses{classes}->{clauses} }){
		my @clauses;
		foreach my $clause_arr (@{ $clauses{classes}->{clauses} }){
			push @clauses, '(' . join(' OR ', @$clause_arr) . ')';
		}
		$positive_qualifier = '(' . join(" " . ' OR ', @clauses) . ')';
	}
	if (@{ $clauses{and}->{clauses} }){
		my @clauses;
		foreach my $clause_arr (@{ $clauses{and}->{clauses} }){
			push @clauses, '(' . join(' OR ', @$clause_arr) . ')';
		}
		$positive_qualifier .= ' AND ' . join(" " . ' AND ', @clauses);
	}
	if (@{ $clauses{or}->{clauses} }){
		my @clauses;
		foreach my $clause_arr (@{ $clauses{or}->{clauses} }){
			push @clauses, '(' . join(' OR ', @$clause_arr) . ')';
		}
		$positive_qualifier .= " " . ' AND (' . join(' OR ', @clauses) . ')';
	}
	
	my $negative_qualifier = 0;
	if (@{ $clauses{not}->{clauses} }){
		my @clauses;
		foreach my $clause_arr (@{ $clauses{not}->{clauses} }){
			push @clauses, '(' . join(' OR ', @$clause_arr) . ')';
		}
		$negative_qualifier = '(' . join(" " . ' OR ', @clauses) . ')';
	}
	
	my $permissions_qualifier = 1;
	if (@{ $clauses{permissions}->{clauses} }){
		my @clauses;
		foreach my $clause_arr (@{ $clauses{permissions}->{clauses} }){
			push @clauses, '(' . join(' OR ', @$clause_arr) . ')';
		}
		$permissions_qualifier = '(' . join(" " . ' AND ', @clauses) . ')';
	}
	
	my $select = "$positive_qualifier AS positive_qualifier, $negative_qualifier AS negative_qualifier, $permissions_qualifier AS permissions_qualifier";
	my $where;
	if ($q->archive){
		my $match_str = $self->_build_archive_match_str($q);
		$match_str = '1=1' unless $match_str;
		$where = $match_str . ' AND ' . $positive_qualifier . ' AND NOT ' . $negative_qualifier . ' AND ' . $permissions_qualifier;
	}
	else {
		$where = 'MATCH(\'' . $self->_build_sphinx_match_str($q) .'\')';
		$where .=  ' AND positive_qualifier=1 AND negative_qualifier=0 AND permissions_qualifier=1';
	}
	
	my @values = (@{ $clauses{classes}->{vals} }, @{ $clauses{and}->{vals} }, @{ $clauses{or}->{vals} }, @{ $clauses{not}->{vals} }, @{ $clauses{permissions}->{vals} });
	
	# Check for time given
	if ($q->start and $q->end){
		$where .= ' AND timestamp BETWEEN ? AND ?';
		push @values, $q->start, $q->end;
	}
	
	# Add a groupby query if necessary
	my $groupby;	
	if ($q->has_groupby){
		foreach my $field ($q->all_groupbys){
			if ($field eq 'node'){ # special case for node
				# We'll do a normal query
				push @queries, {
					select => $select,
					where => $where,
					values => [ @values ],
				};
				next;
			}
			
			my $field_infos = $self->get_field($field);
			#$self->log->trace('field_infos: ' . Dumper($field_infos));
			foreach my $class_id (keys %{$field_infos}){
				next unless $q->classes->{distinct}->{$class_id} or $class_id == 0;
				my $orderby = undef;
				if ($q->orderby){
					$orderby = $Fields::Field_order_to_attr->{ $self->get_field($q->orderby)->{$class_id}->{field_order} };
					$select .= ', ' . $orderby . ' AS _orderby';
				}
				else {
					$select .= ', timestamp AS _orderby';
				}
				push @queries, {
					select => $select,
					where => $where . ($class_id ? ' AND class_id=?' : ''),
					values => [ @values, $class_id ? $class_id : () ],
					groupby => $Fields::Field_order_to_attr->{ $field_infos->{$class_id}->{field_order} },
					groupby_field => $field,
					orderby => $orderby,
					orderby_dir => $q->orderby_dir,
				};
			}
		}
	}
	elsif ($q->orderby){
		my $field_infos = $self->get_field($q->orderby);
		foreach my $class_id (keys %{$field_infos}){
			my $orderby = $Fields::Field_order_to_attr->{ $self->get_field($q->orderby)->{$class_id}->{field_order} };
			$select .= ', ' . $orderby . ' AS _orderby';
			push @queries, {
				select => $select,
				where => $where,
				values => [ @values ],
				orderby => $orderby,
				orderby_dir => $q->orderby_dir,
			};
		}
	}
	else {
		# We can get away with a single query
		push @queries, {
			select => $select,
			where => $where,
			values => [ @values ]
		};
	}	
		
	return \@queries;
}

sub format_results {
	my ($self, $args) = @_;
	
	my $ret = '';
	if ($args->{format} eq 'tsv'){
		if ($args->{groupby}){
			foreach my $groupby (@{ $args->{groupby} }){
				foreach my $row (@{ $args->{results}->{$groupby} }){
					print join("\t", $row->{'_groupby'}, $row->{'_count'}) . "\n";
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
	elsif ($args->{format} eq 'flat_json'){
		my $json = [];
		if ($args->{groupby}){
			$json = $args->{results};
		}
		else {
			foreach my $row (@{ $args->{results} }){
				foreach my $field (@{ $row->{_fields} }){
					next if $field->{class} eq 'any';
					$row->{ $field->{class} . '.' . $field->{field} } = $field->{value};
				}
				delete $row->{_fields};
				push @$json, $row;
			}
		}
		$ret = $self->json->encode($json);
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
			if (ref($decode) eq 'HASH' and $decode->{qid}){
				$decode->{user} = $args->{user};
				$decode = $self->get_saved_result($decode)->{results};
			}
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

sub transform {
	my ($self, $q) = @_;
	
	my $transform_args = { transforms => $q->transforms, results => [] };
	if ($q->has_groupby){
		$transform_args->{groupby} = $q->groupby;
		foreach my $groupby ($q->results->all_groupbys){
			foreach my $datum (@{ $q->results->groupby($groupby) }){
				push @{ $transform_args->{results} }, { $groupby => $datum->{'_groupby'}, count => $datum->{'_count'} };
			}
		}
		$self->log->trace('new results: ' . Dumper($transform_args->{results}));
	}
	else {
		# Check to see if we are processing bulk results and break work into batches
		if ($q->results->is_bulk){
			$q->results->close();
			my $ret_results = new Results();
			while (my $subset = $q->results->get_results(0,$Max_limit)){
				last unless scalar @$subset;
				# Recursively transform this batch
				my $subq = $q->clone;
				$self->log->debug('subq archive: ' . Dumper($subq->archive));
				$subq->results( new Results(results => $subset) );
				$self->transform($subq);
				if ($subq->has_groupby and not $ret_results->isa('Results::Groupby')){
					$self->log->trace('Switching results from normal to groupby');
					$ret_results = $subq->results;
					$q->groupby($subq->groupby);
				}
				else {
					$ret_results->add_results($subq->results->results);
				}
				$self->log->debug('retresults num results: ' . $ret_results->total_records); 
			}
			$ret_results->close();
			$q->results($ret_results);
			$self->log->debug('got num bulk results after transform: ' . $q->results->total_records);
			return 1;
		}
		else {
			foreach my $row ($q->results->all_results){
				my $condensed_hash = { timestamp => $row->{timestamp} };
				foreach my $base_col (qw(id msg)){
					$condensed_hash->{$base_col} = $row->{$base_col};
				}
				foreach my $field_hash (@{ $row->{_fields} }){
					$condensed_hash->{ $field_hash->{field} } = $field_hash->{value};
				}
				push @{ $transform_args->{results} }, $condensed_hash;
			}
			$self->log->trace('ready to transform ' . (scalar @{ $transform_args->{results} }) . ' rows');
		}
		$self->log->debug('$transform_args->{transforms}' . Dumper($transform_args->{transforms}));
	}
	
	my $num_found = 0;
	my $cache;
	eval {
		$cache = CHI->new(
			driver => 'DBI', 
			dbh => $self->db, 
			create_table => 1,
			table_prefix => 'cache_',
			namespace => 'transforms',
		);
	};
	if (@$ or not $cache){
		$self->log->warn('Falling back to RawMemory for cache, consider installing CHI::Driver::DBI');
		$cache = CHI->new(driver => 'RawMemory', datastore => {});
	}
	#$self->log->debug('using cache ' . Dumper($cache));
	for (my $transform_counter = 0; $transform_counter < @{ $transform_args->{transforms} }; $transform_counter++){
		my $raw_transform = $transform_args->{transforms}->[$transform_counter];
		$raw_transform =~ /(\w+)\(?([^\)]+)?\)?/;
		my $transform = lc($1);
		my @given_transform_args = $2 ? split(/\,/, $2) : ();
		# Remove any args which are all whitespace
		for (my $i = 0; $i < @given_transform_args; $i++){
			if ($given_transform_args[$i] =~ /^\s+$/){
				splice(@given_transform_args, $i, 1);
			}
		}
		
		if ($transform eq 'subsearch'){
			unless ($transform_args->{groupby}){
				my $warning = 'cannot subsearch without a report or groupby field';
				$self->log->error($warning);
				$self->add_warning($warning);
				next;
			}
			
			my $query_string = $given_transform_args[0];
			
			# Add optional field
			my $field = '';
			my $negate = 0;
			if ($#given_transform_args >= 1){
				$field = $given_transform_args[1];
				$negate = $given_transform_args[2] ? 1 : 0;
			}
			my @values;
			$self->log->debug('attempting to subsearch result: ' . Dumper($transform_args->{results}));
			
			if (ref($transform_args->{results}) eq 'HASH'){
				foreach my $groupby (keys %{ $transform_args->{results} }){
					foreach my $datum (@{ $transform_args->{results}->{$groupby} }){
						if (exists $datum->{_groupby}){
							if ($field){
								push @values, $field . ':' . $datum->{_groupby};
							}
							else {
								push @values, $datum->{_groupby};
							}
						}
						else {
							# Find our field to use since it wasn't given explicitly
							foreach my $key (keys %$datum){
								my $value = $datum->{$key};
								next if ref($value);
								if ($field){
									next if $key eq 'count' and $field ne 'count';
									push @values, $field . ':' . $value;
								}
								else {
									next if $key eq 'count';
									push @values, $value;
								}
							}
						}
					}
				}
			}
			else {
				foreach my $datum (@{ $transform_args->{results} }){
					if (exists $datum->{_groupby}){
						if ($field){
							push @values, $field . ':' . $datum->{_groupby};
						}
						else {
							push @values, $datum->{_groupby};
						}
					}
					else {
						# Find our field to use since it wasn't given explicitly
						foreach my $key (keys %$datum){
							my $value = $datum->{$key};
							next if ref($value);
							if ($field){
								next if $key eq 'count' and $field ne 'count';
								push @values, $field . ':' . $value;
							}
							else {
								next if $key eq 'count';
								push @values, $value;
							}
						}
					}
				}
			}				
			
			$self->log->debug('values: ' . Dumper(\@values));
			unless (scalar @values){
				$self->log->error('no values from transform_args: ' . Dumper($transform_args));
				$self->add_warning('Transform ' . $transform_args->{transforms}->[$transform_counter - 1] . ' eliminated all values');
				last;
			}
			
			for (my $values_counter = 0; $values_counter < scalar @values; $values_counter += $Max_query_terms){
				my $end = $values_counter + $Max_query_terms;
				if ($end > scalar @values){
					$end = (scalar @values) - 1; # -1 doesn't work here
				}
				my @subvalues = @values[$values_counter..$end];
				$self->log->debug('values_counter: ' . $values_counter . ', end: ' . $end . ', num subvalues: ' . (scalar @subvalues));
				last unless scalar @subvalues;
				
				my $sub_query_string;
				if ($negate){
					$sub_query_string = $given_transform_args[0] . ' -(' . join(' ', @subvalues) . ')';
				}
				else {
					$sub_query_string = $given_transform_args[0] . ' +(' . join(' ', @subvalues) . ')';
				}
				my $subq = new Query(
					conf => $self->conf, 
					user => $q->user, 
					query_string => $sub_query_string, 
					meta_params => $q->meta_params,
					node_info => $q->node_info,
					archive => $q->archive,
					analytics => $q->analytics,
					system => 1,
				);
				$subq->cutoff($subq->limit);

				$self->query($subq);
				
				if ($subq->has_groupby){
					$transform_args->{groupby} = $subq->groupby;
				}
				else {
					delete $transform_args->{groupby};
				}
				$self->log->debug('groupby ' . Dumper($transform_args->{groupby}));
				$self->log->debug('got subsearch results: ' . Dumper($subq->results->results));
				$num_found++;
				
				if (not $subq->has_groupby and $transform_counter != ((scalar @{ $transform_args->{transforms} }) - 1)){
					# If there's no groupby but more transforms
					$transform_args->{results} = [];
					foreach my $row ($subq->results->all_results){
						my $condensed_hash = { id => $row->{id}, msg => $row->{msg}, timestamp => $row->{timestamp} };
						foreach my $field_hash (@{ $row->{_fields} }){
							$condensed_hash->{ $field_hash->{field} } = $field_hash->{value};
						}
						push @{ $transform_args->{results} }, $condensed_hash;
					}
					$q->results($subq->results);
					$q->groupby($subq->groupby);
				}
				elsif ($transform_counter == ((scalar @{ $transform_args->{transforms} }) - 1) and $subq->has_groupby){
					$self->log->trace('subsearch is final transform, returning hash results instead of array results');
					$transform_args->{results} = $subq->results->results;
					$transform_args->{groupby} = $subq->groupby;
				}
				elsif ($subq->has_groupby){
					$transform_args->{results} = [];
					$transform_args->{groupby} = $subq->groupby;
					foreach my $groupby ($subq->results->all_groupbys){
						foreach my $datum (@{ $subq->results->groupby($groupby) }){
							push @{ $transform_args->{results} }, { $groupby => $datum->{'_groupby'} };
						}
					}
				}
				else {
					$transform_args->{results} = $subq->results->results;
					$q->results($subq->results);
					$q->groupby($subq->groupby);
				}
				$q->stats($subq->stats);
			}
			$self->log->trace('new results after subsearch: ' . Dumper($transform_args->{results}));
		}
		else {
			my $plugin_fqdn = 'Transform::' . $transform;
			foreach my $plugin ($self->plugins()){
				if (lc($plugin) eq lc($plugin_fqdn)){
					$self->log->debug('loading plugin ' . $plugin);
					eval {
						my %compiled_transform_args = (
							query_string => $q->query_string,
							query_meta_params => $q->meta_params,
							conf => $self->conf,
							log => $self->log,
							user => $q->user,
							cache => $cache,
							data => $transform_args->{results}, #$transform_args->{results}, 
							args => [ @given_transform_args ]);
						if ($transform_args->{groupby} and ref($transform_args->{results}) eq 'HASH'){
							foreach my $groupby (@{ $transform_args->{groupby} }){
								$compiled_transform_args{data} = $transform_args->{results}->{$groupby};
								$compiled_transform_args{groupby} = $groupby;
							}
						}
						elsif ($transform_args->{groupby}){
							$compiled_transform_args{groupby} = $transform_args->{groupby}->[0];
						}
						my $plugin_object = $plugin->new(%compiled_transform_args);
						$transform_args->{results} = $plugin_object->data;
						
						$self->log->debug('transform_args groupby: ' . Dumper($transform_args->{groupby}));
						if ($plugin_object->groupby){
							$transform_args->{groupby} = [ $plugin_object->groupby ];
							$transform_args->{results} = { $plugin_object->groupby => $plugin_object->data };
							$self->log->debug('set new groupby: ' . Dumper($transform_args->{groupby}));
						}
						elsif ($transform_args->{groupby}){
							$transform_args->{results} = {};
							foreach my $groupby (@{ $transform_args->{groupby} }){
								 $transform_args->{results}->{$groupby} = $plugin_object->data;
							}
						}
						
						$num_found++;
					};
					if ($@){
						$self->log->error('Error creating plugin ' . $plugin . ' with data ' 
							. Dumper($transform_args->{results}) . ' and args ' . Dumper(\@given_transform_args) . ': ' . $@);
						$self->add_warning($@);
					}
					last;
				}
			}
			unless ($num_found){
				$self->log->error("failed to find transform $plugin_fqdn" . ', only have transforms ' .
					join(', ', $self->plugins()));
				return 0;
			}
		}
	}
	
	if (ref($transform_args->{results}) eq 'ARRAY' and $q->has_groupby){
		$self->log->trace('removing groupby due to transform');
		$q->groupby([]);
	}
	
	if ($transform_args->{groupby} or $q->has_groupby){
		my $results;
		my $groupbys;
		if ($transform_args->{groupby}){
			$results = $transform_args->{results};
			$groupbys = $transform_args->{groupby};
		}
		else {
			$results = $q->results;
			$groupbys = $q->results->all_groupbys;
		}
		
		foreach my $groupby (@$groupbys){
			my @groupby_results;
			if (ref($results) eq 'HASH'){
				for (my $i = 0; $i < scalar @{ $results->{$groupby} }; $i++){
					my $row = $results->{$groupby}->[$i];
					#$self->log->debug('row: ' . Dumper($row));
					if (exists $row->{transforms} and scalar keys %{ $row->{transforms} }){
						foreach my $transform (sort keys %{ $row->{transforms} }){
							next unless ref($row->{transforms}->{$transform}) eq 'HASH';
							my $arr_add_on_str = '';
							foreach my $field (sort keys %{ $row->{transforms}->{$transform} }){
								if (ref($row->{transforms}->{$transform}->{$field}) eq 'HASH'){
									my $add_on_str = '';
									foreach my $data_attr (keys %{ $row->{transforms}->{$transform}->{$field} }){
										if (ref($row->{transforms}->{$transform}->{$field}->{$data_attr}) eq 'ARRAY'){
											$add_on_str .= ' ' . $data_attr . '=' . join(',', @{ $row->{transforms}->{$transform}->{$field}->{$data_attr} }); 
										}
										else {
											$add_on_str .= ' ' . $data_attr . '=' .  $row->{transforms}->{$transform}->{$field}->{$data_attr};
										}
									}
									push @groupby_results, { '_count' => $row->{count}, '_groupby' => ($row->{$groupby} . ' ' . $add_on_str) };
								}
								# If it's an array, we want to concatenate all fields together.
								elsif (ref($row->{transforms}->{$transform}->{$field}) eq 'ARRAY'){
									foreach my $value (@{ $row->{transforms}->{$transform}->{$field} }){
										$arr_add_on_str .= ' ' . $field . '=' .  $value;
									}
								}
							}
							if ($arr_add_on_str ne ''){
								push @groupby_results, { '_count' => $row->{count}, '_groupby' => ($row->{$groupby} . ' ' . $arr_add_on_str) };
							}
						}
					}
					else {
						push @groupby_results, $row;
					}
				}
			}
			else {
				$self->log->error('results for groupby must be HASH');
				next;
			}
			#$self->log->debug('args results ' . Dumper($args->{results}));
			if (ref($results) eq 'ARRAY'){
				#$self->log->trace('converting results from ARRAY to HASH because we have a groupby');
				$results = {};
			}
			$results->{$groupby} = [ @groupby_results ];
			$q->results(Results::Groupby->new(results => $results));
			$q->groupby($groupbys);
		}

		$self->log->trace('transform_args->{groupby} to ' . Dumper($transform_args->{groupby}));
		
	}
	else {
		# Now go back and insert the transforms
		my @final;
		#$self->log->debug('$transform_args->{results}: ' . Dumper($transform_args->{results}));
		#$self->log->debug('results: ' . Dumper($q->results->results));
		for (my $i = 0; $i < scalar @{ $transform_args->{results} }; $i++){
			my $transform_row = $transform_args->{results}->[$i];
			unless (exists $transform_row->{id}){
				push @final, $transform_row;
				next;
			}
			for (my $j = 0; $j < $q->results->records_returned; $j++){
				my $results_row = $q->results->idx($j);
				$self->log->debug('results_row: ' . Dumper($results_row));
				$self->log->debug('transform_row: ' . Dumper($transform_row));
				# If id's match or there is no id (like from an external datasource) and the msg matches or no id/msg
				if ((exists $results_row->{id} and exists $transform_row->{id} and $results_row->{id} eq $transform_row->{id})
					or (not exists $results_row->{id} and exists $results_row->{msg} and exists $transform_row->{msg} and $results_row->{msg} eq $transform_row->{msg})
					or (not defined $transform_row->{msg} and not defined $results_row->{id})){
					foreach my $transform (sort keys %{ $transform_row->{transforms} }){
						next unless ref($transform_row->{transforms}->{$transform}) eq 'HASH';
						foreach my $transform_field (sort keys %{ $transform_row->{transforms}->{$transform} }){
							if ($transform_field eq '__REPLACE__'){
								foreach my $transform_key (sort keys %{ $transform_row->{transforms}->{$transform}->{$transform_field} }){
									my $value = $transform_row->{$transform_key};
									# Perform replacement on fields
									foreach my $row_field_hash (@{ $results_row->{_fields} }){
										if ($row_field_hash->{field} eq $transform_key){
											$row_field_hash->{value} = $value;
										}
									}
									# Perform replacement on attrs
									foreach my $row_key (keys %{ $results_row }){
										next if ref($results_row->{$row_key});
										if ($row_key eq $transform_key){
											$results_row->{$row_key} = $value;
										}
									}
								}
							}
							elsif (ref($transform_row->{transforms}->{$transform}->{$transform_field}) eq 'HASH'){
								foreach my $transform_key (sort keys %{ $transform_row->{transforms}->{$transform}->{$transform_field} }){
									my $value = $transform_row->{transforms}->{$transform}->{$transform_field}->{$transform_key};
									if (ref($value) eq 'ARRAY'){
										foreach my $value_str (@$value){
											push @{ $results_row->{_fields} }, { 
												field => $transform_field . '.' . $transform_key, 
												value => $value_str, 
												class => 'Transform.' . $transform,
											};
										}
									}
									else {			
										push @{ $results_row->{_fields} }, { 
											field => $transform_field . '.' . $transform_key, 
											value => $value,
											class => 'Transform.' . $transform,
										};
									}
								}
							}
							elsif (ref($transform_row->{transforms}->{$transform}->{$transform_field}) eq 'ARRAY'){
								foreach my $value (@{ $transform_row->{transforms}->{$transform}->{$transform_field} }){
									push @{ $results_row->{_fields} }, { 
										field => $transform . '.' . $transform_field, 
										value => $value,
										class => 'Transform.' . $transform,
									};
								}
							}
						}
					}
					push @final, $results_row;
					last;
				}
			}
		}
		$self->log->debug('final: ' . Dumper(\@final));
		$q->results(Results->new(results => [ @final ]));
		$q->groupby([]);
	}
	return 1;
}

sub send_to {
	my ($self, $args) = @_;
	
	my $q;
	if (ref($args) eq 'Query'){
		$q = $args;
	}
	else {
		# Get our node info
		if (not $self->node_info->{updated_at} 
			or (time() - $self->node_info->{updated_at} >= $self->conf->get('node_info_cache_timeout'))
			or not $args->{user}->is_admin){
			$self->node_info($self->_get_node_info($args->{user}));
		}
		if($args->{query}){
			$self->log->debug('args: ' . Dumper($args));
			$q = new Query(conf => $self->conf, user => $args->{user}, query_string => $args->{query}->{query_string}, 
				node_info => $self->node_info, connectors => $args->{connectors}, 
				meta_params => $args->{query}->{query_meta_params}, qid => $args->{qid} ? $args->{qid} : 0);
			$q->results(new Results(results => ( ref($args->{results}) eq 'ARRAY' ? $args->{results} : $args->{results}->{results})));
		}
		else {
			$self->_error('Invalid args, no Query, args->{query}');
			return;
		}
	}
		
	#$self->log->debug('q: ' . Dumper($q));
	
	return unless $q->has_connectors;

	for (my $i = 0; $i < $q->num_connectors; $i++){
		my $raw = $q->connector_idx($i);
		my ($connector, @connector_args);
		if ($raw =~ /(\w+)\(([^\)]+)?\)/){
			$connector = $1;
			if ($2){
				@connector_args = split(/\,/, $2);
			}
			elsif ($q->connector_params_idx($i)){
				@connector_args = $q->connector_params_idx($i);
				$self->log->debug('connector_params: ' . Dumper($q->connector_params));
				$self->log->debug('set @connector_args to ' . Dumper(\@connector_args)); 
			}
		}
		else {
			$connector = $raw;
			my $cargs = $q->connector_params_idx($i);
			@connector_args = @{ $cargs } if $cargs;
		}
		
		my $plugin_fqdn = 'Connector::' . $connector;
		my $num_found = 0;
		foreach my $plugin ($self->plugins()){
			if (lc($plugin) eq lc($plugin_fqdn)){
				$self->log->debug('loading plugin ' . $plugin);
				eval {
					# Check to see if we are processing bulk results
					if ($q->results->is_bulk){
						$q->results->close();
						my $ret_results = new Results();
						while (my $results = $q->results->get_results(0,$Max_limit)){
							last unless scalar @$results;
							my $plugin_object = $plugin->new(
								api => $self,
								user => $q->user,
								results => { results => $results },
								args => [ @connector_args ],
								query_schedule_id => $q->schedule_id,
								comments => $q->comments,
							);
							$ret_results->add_results($plugin_object->results->{results});
							# for returnable amount
							if (ref($plugin_object->results) eq 'HASH'){
								#$self->log->trace('returnable results: ' . scalar @{ $plugin_object->results->{results} });
								foreach (@{ $plugin_object->results->{results} }){
									last if ($q->limit and $ret_results->total_records > $q->limit) 
										or ($ret_results->total_records > $Max_limit);
									$ret_results->add_result($_);
								}
							}
							else {
								#$self->log->trace('returnable results: ' . scalar @{ $plugin_object->results });
								foreach (@{ $plugin_object->results }){
									last if ($q->limit and $ret_results->total_records > $q->limit) 
										or ($ret_results->total_records > $Max_limit);
									$ret_results->add_result($_);
								}
							}
						}
						$ret_results->close();
						$q->results($ret_results);
					}
					else {
						my $plugin_object = $plugin->new(
							api => $self,
							user => $q->user,
							results => { results => $q->results->results },
							args => [ @connector_args ],
							query => $q,
						);
						if ($q->has_groupby and ref($plugin_object->results) eq 'HASH' and scalar keys %{ $plugin_object->results }){
							$q->results(Results::Groupby->new(results => $plugin_object->results->{results}));
						}
						elsif (ref($plugin_object->results) eq 'HASH' and scalar keys %{ $plugin_object->results }){
							$q->results(Results->new(results => $plugin_object->results->{results}));
							#$self->log->debug('$plugin_object->results->{results}: ' . Dumper($plugin_object->results->{results}));
							#$self->log->debug('$q->results: ' . Dumper($q->results));
							#$self->log->debug('$q->results->all_results: ' . Dumper($q->results->all_results));
						}
						elsif (ref($plugin_object->results) eq 'ARRAY'){
							$q->results(Results->new(results => $plugin_object->results));
						}
					}
					$num_found++;
				};
				if ($@){
					$self->log->error('Error creating plugin ' . $plugin . ' with data ' 
						. Dumper($q->results) . ' and args ' . Dumper(\@connector_args) . ': ' . $@);
					return [ 'Error: ' . $@ ];
				}
			}
		}
		unless ($num_found){
			$self->log->error("failed to find connectors " . Dumper($q->connectors) . ', only have connectors ' .
				join(', ', $self->plugins()));
			return 0;
		}
	}
	#$self->log->debug('$q->results->all_results: ' . Dumper($q->results->all_results));
	
	return [ $q->results->all_results ];
}


sub run_schedule {
	my ($self, $args) = @_;
	
	unless ($args->{user} and $args->{user}->username eq 'system'){
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
	
	my $form_params = $self->get_form_params($args->{user});
	
	# Expire schedule entries
	$query = 'SELECT id, query, username FROM query_schedule JOIN users ON (query_schedule.uid=users.uid) WHERE end < UNIX_TIMESTAMP() AND enabled=1';
	$sth = $self->db->prepare($query);
	$sth->execute();
	my @ids;
	my $counter = 0;
	while (my $row = $sth->fetchrow_hashref){
		push @ids, $row->{id};
		my $user = $self->get_user($row->{username});
		my $decode = $self->json->decode($row->{query});
		
		my $headers = {
			To => $user->email,
			From => $self->conf->get('email/display_address') ? $self->conf->get('email/display_address') : 'system',
			Subject => 'ELSA alert has expired for query ' . $decode->{query_string},
		};
		my $body = 'The alert set for query ' . $decode->{query_string} . ' has expired and has been disabled.  ' .
			'If you wish to continue receiving this query, please log into ELSA, enable the query, and set a new expiration date.';
		
		$self->send_email({headers => $headers, body => $body});
	}
	if (scalar @ids){
		$self->log->info('Expiring query schedule for ids ' . join(',', @ids));
		$query = 'UPDATE query_schedule SET enabled=0 WHERE id IN (' . join(',', @ids) . ')';
		$sth = $self->db->prepare($query);
		$sth->execute;
	}
	
	# Run schedule	
	$query = 'SELECT t1.id AS query_schedule_id, username, t1.uid, query, frequency, start, end, connector, params' . "\n" .
		'FROM query_schedule t1' . "\n" .
		'JOIN users ON (t1.uid=users.uid)' . "\n" .
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
				# Keep doubling the distance we'll go back to find the last date
				$farthest_back_to_check -= $how_far_back;
				$self->log->trace('how_far_back: ' . $how_far_back);
				$how_far_back *= 2;
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
			ParseDate("jan 1"), # base time to use for the recurrence period 
			ParseDate(scalar localtime($cur_time)),
			ParseDate(scalar localtime($cur_time + $self->conf->get('schedule_interval')))
		);
		$self->log->trace('dates: ' . Dumper(\@dates) . ' row: ' . Dumper($row));
		if (scalar @dates){
			# Adjust the query time to avoid time that is potentially unindexed by offsetting by the schedule interval
			my $query_params = $self->json->decode($row->{query});
			$query_params->{meta_params} = delete $query_params->{query_meta_params};
			$query_params->{meta_params}->{start} = ($last_run - $self->conf->get('schedule_interval'));
			$query_params->{meta_params}->{end} = ($cur_time - $self->conf->get('schedule_interval'));
			$query_params->{schedule_id} = $row->{query_schedule_id};
			$query_params->{connectors} = [ $row->{connector} ];
			$query_params->{system} = 1; # since the user did not init this, it's a system query
			$self->log->debug('query_params: ' . Dumper($query_params));
			
			if (!$user_info_cache->{ $row->{uid} }){
				$user_info_cache->{ $row->{uid} } = $self->get_user($row->{username});
				#$self->log->trace('Got user info: ' . Dumper($user_info_cache->{ $row->{uid} }));
			}
			else {
				$self->log->trace('Using existing user info');
			}
			$query_params->{user} = $user_info_cache->{ $row->{uid} };
			
			# Perform query
			eval {
				$self->query($query_params);
			};
			if ($@){
				$self->log->error('Problem running query: ' . Dumper($query_params) . "\n" . $@);
			}
			$counter++;
		}
	}
	
	# Verify we've received logs from hosts specified in the config file
	if ($self->conf->get('host_checks')){
		my $admin_email = $self->conf->get('admin_email_address');
		if ($admin_email){
			my %intervals;
			foreach my $host (keys %{ $self->conf->get('host_checks') }){
				my $interval = $self->conf->get('host_checks')->{$host};
				$intervals{$interval} ||= [];
				push @{ $intervals{$interval} }, $host;
			}
			
			# For each unique interval, run all the hosts in a batch via groupby:host
			foreach my $interval (keys %intervals){
				my $query_params = { 
					query_string => join(' ', map { 'host:' . $_ } @{ $intervals{$interval} }) . ' groupby:host', 
					meta_params => { 
						start => (time() - $interval - 60), # 60 second grace period for batch load
						limit => scalar @{ $intervals{$interval} },
					},
					system => 1,
					user => $args->{user},
				};
				$self->log->debug('query_params: ' . Dumper($query_params));
				my $result = $self->query($query_params);
				my %not_found = map { $_ => 1 } @{ $intervals{$interval} };
				foreach my $row (@{ $result->results->all_results }){
					$self->log->trace('Found needed results for ' . $row->{_groupby} . ' in interval ' . $interval);
					delete $not_found{ $row->{_groupby} };
				}
				foreach my $host (keys %not_found){
					my $errmsg = 'Did not find entries for host ' . $host . ' within interval ' . $interval;
					$self->log->error($errmsg);
					my $headers = {
						To => $self->conf->get('admin_email_address'),
						From => $self->conf->get('email/display_address') ? $self->conf->get('email/display_address') : 'system',
						Subject => sprintf('Host inactivity alert: %s', $host),
					};
					$self->send_email({ headers => $headers, body => $errmsg });
				}
			}
		}
		else {
			$self->log->error('Configured to do host checks via host_checks but no admin_email_address found in config file');
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
	
	$self->expire_livetails($args);
	
	return $counter;
}

sub send_email {
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
		($ret) = Email::LocalDelivery->deliver($email->as_string, $self->conf->get('logdir') . '/' . $self->conf->get('email/to'));
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

sub _batch_notify {
	my ($self, $q) = @_;
	#$self->log->trace('got results for batch: ' . Dumper($args));
	
	my $num_records = $q->results->total_records ? $q->results->total_records : $q->results->records_returned;
	my $headers = {
		To => $q->user->email,
		From => $self->conf->get('email/display_address') ? $self->conf->get('email/display_address') : 'system',
		Subject => sprintf('ELSA archive query %d complete with %d results', $q->qid, $num_records),
	};
	my $body;
	
	if ($q->results->is_bulk){
		$body = sprintf('%d results for query %s', $num_records, $q->query_string) .
			"\r\n" . sprintf('%s/Query/get_bulk_file?qid=%d&name=%s', 
				$self->conf->get('email/base_url') ? $self->conf->get('email/base_url') : 'http://localhost',
				$q->qid, $q->results->bulk_file->{name});
	}
	else {
		$body = sprintf('%d results for query %s', $num_records, $q->query_string) .
			"\r\n" . sprintf('%s/get_results?qid=%d&hash=%s', 
				$self->conf->get('email/base_url') ? $self->conf->get('email/base_url') : 'http://localhost',
				$q->qid, $q->hash);
	}
	
	$self->send_email({ headers => $headers, body => $body});
}

sub run_archive_queries {
	my ($self, $args) = @_;
	
	unless ($args->{user} and $args->{user}->username eq 'system'){
		die('Only system can run the schedule');
	}
	
	my ($query, $sth);
	$query = 'SELECT qid, username, query FROM query_log t1 JOIN users t2 ON (t1.uid=t2.uid) WHERE ISNULL(num_results) AND archive=1';
	$sth = $self->db->prepare($query);
	$sth->execute;
	
	while (my $row = $sth->fetchrow_hashref){
		my $user = $self->get_user($row->{username});
		
		# Get our node info
		if (not $self->node_info->{updated_at} 
			or ($self->conf->get('node_info_cache_timeout') 
				and (time() - $self->node_info->{updated_at} >= $self->conf->get('node_info_cache_timeout')))
			or not $user->is_admin){
			$self->node_info($self->_get_node_info($user));
		}
		
		my $q = new Query(conf => $self->conf, user => $user, q => $row->{query}, qid => $row->{qid}, 
			node_info => $self->node_info);
		$q->mark_batch_start();
		
		
		if ($q->archive){
			$self->_archive_query($q);
			next if $q->cancelled;
		}
		else {
			$q->analytics(1);
			$self->_unlimited_sphinx_query($q);
			next if $q->cancelled;
		}
		
		my $total_time = int(
			(Time::HiRes::time() - $q->start_time) * 1000
		);
		
		# Apply transforms
		if ($q->has_transforms){	
			$self->transform($q);
		}
		
		# Send to connectors
		if ($q->has_connectors){
			$self->send_to($q);
		}
		
		# Record the results
		$self->log->trace('got archive results: ' . Dumper($q->results) . ' ' . $q->results->total_records);
		my $sth2 = $self->db->prepare('UPDATE query_log SET num_results=?, milliseconds=? WHERE qid=?');
		$sth2->execute($q->results->records_returned, (1000 * $total_time), $q->qid);
		$sth2->finish;
		
		my $meta = {};
		if ($q->has_groupby){
			$meta->{groupby} = $q->groupby;
		}
		if ($q->analytics){
			$meta->{analytics} = 1;
		}
		if ($q->archive){
			$meta->{archive} = 1;
		}
		
		$q->comments(($q->archive ? 'archive' : 'analytics') . ' query');
		$self->_save_results($q->TO_JSON);
		$self->_batch_notify($q);
	} 
}	

sub _archive_query {
	my ($self, $q) = @_;
	#$self->log->trace('running archive query with args: ' . Dumper($args));
	
	my $overall_start = time();
	my $limit = $q->limit ? $q->limit : 2**32;
	
	my $ret = {};
	my %queries; # per-node hash
	foreach my $node (keys %{ $q->node_info->{nodes} }){
		$ret->{$node} = { rows => [] };
		$queries{$node} = [];
		my $node_info = $q->node_info->{nodes}->{$node};
		# Prune tables
		my @table_arr;
		foreach my $table (@{ $node_info->{tables}->{tables} }){
			if ($q->start and $q->end){
				if ($table->{table_type} eq 'archive' and
					(($q->start >= $table->{start_int} and $q->start <= $table->{end_int})
					or ($q->end >= $table->{start_int} and $q->end <= $table->{end_int})
					or ($q->start <= $table->{start_int} and $q->end >= $table->{end_int})
					or ($table->{start_int} <= $q->start and $table->{end_int} >= $q->end))
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
		
		my $time_select_conversions = {
			day => 'CAST(timestamp/86400 AS unsigned) AS day',
			hour => 'CAST(timestamp/3600 AS unsigned) AS hour',
			minute => 'CAST(timestamp/60 AS unsigned) AS minute',
		};
		
		my $queries = $self->_build_query($q);
		foreach my $table (@table_arr){
			my $start = time();
			foreach my $query (@$queries){
				# strip sphinx-specific attr_ prefix
				$query->{where} =~ s/attr\_((?:i|s)\d)([<>=]{1,2})\?/$1$2\?/g; 
				$query->{orderby} =~ s/attr\_((?:i|s)\d)/$1/g;
				my $search_query;
				if ($query->{groupby}){
					$query->{groupby} =~ s/attr\_((?:i|s)\d)/$1/g;
					if ($time_select_conversions->{ $query->{groupby_field} }){
						my $groupby = $time_select_conversions->{ $query->{groupby_field} };
						$search_query = "SELECT COUNT(*) AS count, class_id, $groupby\n";
					}
					else {
						if ($query->{groupby} eq 'program_id' or $query->{groupby} eq 'class_id'){
							$search_query = "SELECT COUNT(*) AS count, class_id, $query->{groupby}, $query->{groupby_field}\n";
						}
						elsif ($query->{groupby} eq 'host_id'){
							$search_query = "SELECT COUNT(*) AS count, class_id, $query->{groupby}, INET_NTOA($query->{groupby}) AS $query->{groupby_field}\n";
						}
						else {
							$search_query = "SELECT COUNT(*) AS count, class_id, $query->{groupby} AS \"$query->{groupby_field}\"\n";
						}
					}
					$search_query .= "FROM $table main\n";
					if ($query->{groupby} eq 'program_id' or $query->{groupby} eq 'class_id'){
						$search_query .= "LEFT JOIN " . $node_info->{db} . ".programs ON main.program_id=programs.id\n" .
						"LEFT JOIN " . $node_info->{db} . ".classes ON main.class_id=classes.id\n";
					}
					$search_query .= 'WHERE ' . $query->{where} . "\nGROUP BY $query->{groupby}\n" . 'ORDER BY 1 DESC LIMIT ?,?';
				}
				else {
					$search_query = "SELECT main.id,\n" .
						"\"" . $node . "\" AS node,\n" .
						#"DATE_FORMAT(FROM_UNIXTIME(timestamp), \"%Y/%m/%d %H:%i:%s\") AS timestamp,\n" .
						"timestamp,\n" .
						($query->{orderby} ? $query->{orderby} : 'timestamp') . ' AS _orderby,' . "\n" .
						"INET_NTOA(host_id) AS host, program, class_id, class, msg,\n" .
						"i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5\n" .
						"FROM $table main\n" .
						"LEFT JOIN " . $node_info->{db} . ".programs ON main.program_id=programs.id\n" .
						"LEFT JOIN " . $node_info->{db} . ".classes ON main.class_id=classes.id\n" .
						'WHERE ' . $query->{where} . ' ORDER BY _orderby ' . $query->{orderby_dir} . "\n" . 'LIMIT ?,?';
				}
				#$self->log->debug('archive_query: ' . $search_query . ', values: ' . 
				#	Dumper($query->{values}, $args->{offset}, $args->{limit}));
				push @{ $queries{$node} }, 
					{ query => $search_query, values => [ @{ $query->{values} }, $q->offset, $limit ] };
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
		last unless scalar keys %queries;
		my $cv = AnyEvent->condvar;
		$cv->begin(sub {
			$cv->send;
		});
		
		foreach my $node (keys %queries){
			my $query_hash = shift @{ $queries{$node} };
			next unless $query_hash;
			
			# Check if the query was cancelled
			return if $q->check_cancelled;
			
			eval {
				my $start = time();
				foreach my $key (keys %$query_hash){
					$self->log->debug('node: ' . $node . ', key: ' . $key . ', val: ' . Dumper($query_hash->{$key}));
				}
				$self->log->debug('running query ' . $query_hash->{query});
				$self->log->debug(' with values ' . join(',', @{ $query_hash->{values} }));
				$cv->begin;
				$q->node_info->{nodes}->{$node}->{dbh}->query($query_hash->{query}, sub { 
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
	
	#my $total_records = 0;
	if ($q->has_groupby){
		foreach my $groupby ($q->all_groupbys){
			my %agg;
			foreach my $node (sort keys %$ret){
				# One-off for grouping by node
				if ($groupby eq 'node'){
					$agg{$node} = scalar @{ $ret->{$node}->{rows} };
					next;
				}
				
				foreach my $row (@{ $ret->{$node}->{rows} }){
					my $field_infos = $q->resolve($groupby, $row->{$groupby}, '=');					
					my $key;
					my $attr = (keys %{ $field_infos->{attrs}->{ $row->{class_id} } })[0];
					if ($attr){
						$attr =~ s/attr\_//;
						if (exists $Fields::Time_values->{ $groupby }){
							# We will resolve later
							$key = (values %{ $field_infos->{attrs}->{0} })[0];
						}
						elsif ($groupby eq 'program'){
							$key = $row->{program};
						}
						elsif ($groupby eq 'class'){
							$key = $row->{class};
						}
						elsif (exists $Fields::Field_to_order->{ $attr }){
							# Resolve normally
							$key = $self->resolve_value($row->{class_id}, $row->{$groupby}, $attr);
						}
					}
					else {
						my $field_order = $self->get_field($groupby)->{ $row->{class_id} }->{field_order};
						$key = $self->resolve_value($row->{class_id}, $row->{$groupby}, $Fields::Field_order_to_field->{$field_order});
					}
					$agg{ $key } += $row->{count};	
				}
			}
			$self->log->trace('got agg ' . Dumper(\%agg) . ' for groupby ' . $groupby);
			if (exists $Fields::Time_values->{ $groupby }){
				# Sort these in ascending label order
				my @tmp;
				foreach my $key (sort { $a <=> $b } keys %agg){
					my $unixtime = ($key * $Fields::Time_values->{ $groupby });
					push @tmp, { 
						intval => $unixtime, 
						'_groupby' => $self->resolve_value(0, $key, $groupby), 
						'_count' => $agg{$key}
					};
				}
				
				# Fill in zeroes for missing data so the graph looks right
				my @zero_filled;
				my $increment = $Fields::Time_values->{ $groupby };
				$self->log->trace('using increment ' . $increment . ' for time value ' . $groupby);
				OUTER: for (my $i = 0; $i < @tmp; $i++){
					push @zero_filled, $tmp[$i];
					if (exists $tmp[$i+1]){
						for (my $j = $tmp[$i]->{intval} + $increment; $j < $tmp[$i+1]->{intval}; $j += $increment){
							$self->log->trace('i: ' . $tmp[$i]->{intval} . ', j: ' . ($tmp[$i]->{intval} + $increment) . ', next: ' . $tmp[$i+1]->{intval});
							push @zero_filled, { 
								'_groupby' => epoch2iso($j), 
								intval => $j,
								'_count' => 0
							};
							last OUTER if scalar @zero_filled >= $limit;
						}
					}
				}
				foreach (@zero_filled){
					$q->results->add_result($groupby, $_);
				}
			}
			else { 
				# Sort these in descending value order
				my @tmp;
				foreach my $key (sort { $agg{$b} <=> $agg{$a} } keys %agg){
					push @tmp, { intval => $agg{$key}, '_groupby' => $key, '_count' => $agg{$key} };
					last if scalar @tmp >= $limit;
				}
				foreach (@tmp){
					$q->results->add_result($groupby, $_);
				}
				#$self->log->debug('archive groupby results: ' . Dumper($q->results));
			}
		}
	}
	else {
		my @tmp; # we need to sort chronologically
		NODE_LOOP: foreach my $node (keys %$ret){
			#$total_records += scalar @{ $ret->{$node}->{rows} };
			foreach my $row (@{ $ret->{$node}->{rows} }){
				$row->{datasource} = 'Archive';
				$row->{_fields} = [
						{ field => 'host', value => $row->{host}, class => 'any' },
						{ field => 'program', value => $row->{program}, class => 'any' },
						{ field => 'class', value => $row->{class}, class => 'any' },
					];
				# Resolve column names for fields
				foreach my $col (qw(i0 i1 i2 i3 i4 i5 s0 s1 s2 s3 s4 s5)){
					my $value = delete $row->{$col};
					# Swap the generic name with the specific field name for this class
					my $field = $self->node_info->{fields_by_order}->{ $row->{class_id} }->{ $Fields::Field_to_order->{$col} }->{value};
					if (defined $value and $field){
						# See if we need to apply a conversion
						$value = $self->resolve_value($row->{class_id}, $value, $col);
						push @{ $row->{_fields} }, { 'field' => $field, 'value' => $value, 'class' => $self->node_info->{classes_by_id}->{ $row->{class_id} } };
					}
				}
				push @tmp, $row;
			}
		}
		foreach my $row (sort { $a->{timestamp} <=> $b->{timestamp} } @tmp){
			$q->results->add_result($row);
			last if scalar $q->results->total_records >= $limit;
		}
	}
	
	$q->time_taken(time() - $overall_start);
	
	$self->log->debug('completed query in ' . $q->time_taken . ' with ' . $q->results->total_records . ' rows');
	
	return 1;
}

sub _external_query {
	my ($self, $q) = @_;
	#$self->log->trace('running external query with args: ' . Dumper($q));
	
	my $cache;
	eval {
		$cache = CHI->new(
			driver => 'DBI', 
			dbh => $self->db, 
			create_table => 1,
			table_prefix => 'cache_',
			namespace => 'transforms',
		);
	};
	if (@$ or not $cache){
		$self->log->warn('Falling back to RawMemory for cache, consider installing CHI::Driver::DBI');
		$cache = CHI->new(driver => 'RawMemory', datastore => {});
	}
	
	DATASOURCES_LOOP: foreach my $datasource_arg (sort keys %{ $q->datasources }){
		$datasource_arg =~ /(\w+)\(?([^\)]+)?\)?/;
		my $datasource = lc($1);
		my @given_args = $2 ? split(/\,/, $2) : ();
		
		# Check to see if this is a system group
		if ($self->system_datasources->{$datasource} and ref($self->system_datasources->{$datasource})){
			delete $q->datasources->{$datasource};
			foreach my $alias (@{ $self->system_datasources->{$datasource} } ){
				$q->datasources->{$alias} = 1;
			}
			return $self->_external_query($q);
		}
		
		# Check to see if this is a group (kind of a datasource macro)
		if ($self->conf->get('datasource_groups')){
			foreach my $datasource_group_name (keys %{ $self->conf->get('datasource_groups') }){
				if ($datasource_group_name eq $datasource){
					delete $q->datasources->{$datasource_group_name};
					foreach my $datasource_config_reference (@{ $self->conf->get('datasource_groups/' . $datasource_group_name . '/datasources') }){
						$q->datasources->{$datasource_config_reference} = 1;
					}
					# Now that we've resolved the group into its subcomponents, we recurse to run those 
					return $self->_external_query($q);
				}
			}
		}
		
		my $plugin_fqdn = 'Datasource::' . $datasource;
		foreach my $plugin ($self->plugins()){
			$self->log->debug('checking ' . $plugin_fqdn . ' against ' . $plugin);
			if (lc($plugin) eq lc($plugin_fqdn)){
				$self->log->debug('loading plugin ' . $plugin);
				my %compiled_args;
				eval {
					%compiled_args = (
						conf => $self->conf,
						log => $self->log, 
						cache => $cache,
						args => [ @given_args ]
					);
					my $plugin_object = $plugin->new(%compiled_args);
					$plugin_object->query($q);
				};
				if ($@){
					delete $compiled_args{user};
					delete $compiled_args{cache};
					delete $compiled_args{conf};
					delete $compiled_args{log};
					$self->log->error('Error creating plugin ' . $plugin . ' with args ' . Dumper(\%compiled_args) . ': ' . $@);
					$self->add_warning($@);
				}
				next DATASOURCES_LOOP;
			}
		}
		die('datasource ' . $plugin_fqdn . ' not found');
	}
	return $q;
}

sub _livetail_query {
	my ($self, $q) = @_;
	
	my ($query, $sth);
	
	# First check to see if we already have a livetail going (this is a poll)
	$query = 'SELECT qid, query FROM query_log WHERE uid=? AND num_results=-3 AND archive=1';
	$sth = $self->db->prepare($query);
	$sth->execute($q->user->uid);
	my $row = $sth->fetchrow_hashref;
	if ($row){
		$self->log->trace('Found running live tail: ' . Dumper($row));
		# Is this a new query?
		my $query_hash = $self->json->decode($row->{query});
		if ($query_hash->{query_string} ne $q->query_string){
			$self->log->info('Starting new query, cancelling old one');
			$self->cancel_livetail({ qid => $row->{qid}, user => $q->user});
		}
		else {
			$self->log->debug('my qid: ' . $q->qid . ', running qid: ' . $row->{qid});
			# Drop our new one in favor of the already-running one
			if ($q->qid ne $row->{qid}){
				$self->cancel_livetail({ qid => $q->qid, user => $q->user});
				$q->qid($row->{qid});
			}
			# Get latest results
			$q->start(int(time() - $Livetail_poll_interval));
			$self->_get_livetail_results($q);
			# Mark that we're still running
			$query = 'UPDATE query_log SET milliseconds = (UNIX_TIMESTAMP() - UNIX_TIMESTAMP(timestamp))*1000 WHERE qid=? AND uid=?';
			$sth = $self->db->prepare($query);
			$sth->execute($q->qid, $q->user->uid);
			return $q;
		}
	}
	else {
		$self->log->debug('no livetail for uid ' . $q->user->uid);
	}
	
	# Set batch/livetail mode
	$q->batch(1);
	$q->mark_livetail_start();
	
	# Take archive query terms and turn into livetail query terms
	my $eval_str = '';
	my $queries = $self->_build_livetail_query($q);
	
	$query = 'INSERT INTO livetail (qid, query) VALUES (?,?)';
	foreach my $node (keys %{ $q->node_info->{nodes} }){
		my $dbh = DBI->connect_cached(@{ $q->node_info->{nodes}->{$node}->{dbh}->db_args }) or die($DBI::errstr);
		$sth = $dbh->prepare($query);
		$sth->execute($q->qid, $queries);
		$sth->finish;
		$self->log->debug('added livetail ' . $q->qid . ' to node ' . $node);
	}
	return $q;
}

sub _get_livetail_results {
	my ($self, $q) = @_;
	my ($query,$sth);
	my $ret = {};
	foreach my $node (keys %{ $q->node_info->{nodes} }){
		my $node_info = $q->node_info->{nodes}->{$node};
		$query = "SELECT main.id,\n" .
			"\"" . $node . "\" AS node,\n" .
			#"DATE_FORMAT(FROM_UNIXTIME(timestamp), \"%Y/%m/%d %H:%i:%s\") AS timestamp,\n" .
			"timestamp,\n" .
			"INET_NTOA(host_id) AS host, program, class_id, class, msg,\n" .
			"i0, i1, i2, i3, i4, i5, s0, s1, s2, s3, s4, s5\n" .
			"FROM livetail_results main\n" .
			"LEFT JOIN " . $node_info->{db} . ".programs ON main.program_id=programs.id\n" .
			"LEFT JOIN " . $node_info->{db} . ".classes ON main.class_id=classes.id\n" .
			'WHERE qid=? AND timestamp >= ? ORDER BY timestamp DESC';
		
		$ret->{$node} = [];
		my $dbh = DBI->connect_cached(@{ $q->node_info->{nodes}->{$node}->{dbh}->db_args }) or die($DBI::errstr);
		$sth = $dbh->prepare($query);
		$sth->execute($q->qid, $q->start);
		$self->log->debug('query: ' . $query . "\nargs: " . $q->qid . " " . $q->start);
		while (my $row = $sth->fetchrow_hashref){
			push @{ $ret->{$node} }, $row;
		}
		$sth->finish;
	}
	
	my @tmp; # we need to sort chronologically
	NODE_LOOP: foreach my $node (keys %$ret){
		foreach my $row (@{ $ret->{$node} }){
			$row->{datasource} = 'Livetail';
			$row->{_fields} = [
					{ field => 'host', value => $row->{host}, class => 'any' },
					{ field => 'program', value => $row->{program}, class => 'any' },
					{ field => 'class', value => $row->{class}, class => 'any' },
				];
			# Resolve column names for fields
			foreach my $col (qw(i0 i1 i2 i3 i4 i5 s0 s1 s2 s3 s4 s5)){
				my $value = delete $row->{$col};
				# Swap the generic name with the specific field name for this class
				my $field = $self->node_info->{fields_by_order}->{ $row->{class_id} }->{ $Fields::Field_to_order->{$col} }->{value};
				if (defined $value and $field){
					# See if we need to apply a conversion
					$value = $self->resolve_value($row->{class_id}, $value, $col);
					push @{ $row->{_fields} }, { 'field' => $field, 'value' => $value, 'class' => $self->node_info->{classes_by_id}->{ $row->{class_id} } };
				}
			}
			push @tmp, $row;
		}
	}
	foreach my $row (sort { $a->{timestamp} cmp $b->{timestamp} } @tmp){
		$q->results->add_result($row);
	}
}

sub _build_livetail_query {
	my $self = shift;
	my $q = shift;
	
	my @queries;
	my %clauses = ( 
		classes => [], 
		and => [], 
		or => [], 
		not => [],
		permissions => [],
	);
	
	# Create permissions clauses
	foreach my $attr (qw(class_id host_id program_id node_id)){
		# Get field name
		my $line_pos;
		foreach my $idx (keys %{ $Fields::Field_order_to_attr }){
			if ($Fields::Field_order_to_attr->{$idx} eq $attr){
				$line_pos = $idx;
				last;
			}
		}
		
		foreach my $id (keys %{ $q->user->permissions->{$attr} }){
			next unless $id;
			$self->log->trace("Adding id $id to $attr based on permissions");
			$clauses{permissions}->[$line_pos] ||= [];
			if ($Fields::IP_fields->{$attr} and $id =~ /^(\d+)\-(\d+)$/){
				my ($min, $max) = ($1, $2);
				push @{ $clauses{permissions}->[$line_pos] }, 'sub { $_[1] >= ' . $min . ' and $_[1] <= ' . $max . ' }';
			}
			else {
				push @{ $clauses{permissions}->[$line_pos] }, 'sub { $_[1] == ' . $id . ' }';
			}
		}
	}
	
	foreach my $class_id (keys %{ $q->user->permissions->{fields} }){
		foreach my $perm_hash (@{ $q->user->permissions->{fields}->{$class_id} }){
			my ($name, $value) = @{ $perm_hash->{attr} };
			if ($value =~ /^(\d+)\-(\d+)$/){
				my ($min, $max) = ($1, $2);
				push @{ $clauses{permissions}->[ $Fields::Field_to_order->{$name} ] }, 'sub { $_[0] == ' . $class_id . ' and $_[1] >= ' . $min . ' $_[1] <= ' . $max . ' }';
			}
			else {
				push @{ $clauses{permissions}->[ $Fields::Field_to_order->{$name} ] }, 'sub { $_[0] == ' . $class_id . ' and $_[1] eq ' . $value . ' }';
			}			
		}
	}

	my @terms;
	foreach my $class_id (keys %{ $q->classes->{distinct} }){
		push @terms, '$_[0] == ' . $class_id;
	}
	push @{ $clauses{classes} }, 'sub { ' . join(' or ', @terms) . ' }'; 
	
	if (scalar keys %{ $q->classes->{excluded} }){
		@terms = ();
		foreach my $class_id (keys %{ $q->classes->{excluded} }){
			push @terms, '$_[0] != ' . $class_id;
		}
		push @{ $clauses{classes} }, 'sub { ' . join(' and ', @terms) . ' }';
	}
	
	# Handle our basic equalities
	foreach my $boolean (qw(and or not)){
		foreach my $field (sort keys %{ $q->terms->{attr_terms}->{$boolean}->{'='} }){
			my @clause;
			foreach my $class_id (sort keys %{ $q->terms->{attr_terms}->{$boolean}->{'='}->{$field} }){
				next unless $q->classes->{distinct}->{$class_id} or $class_id eq 0
					or exists $q->classes->{partially_permitted}->{$class_id};
				foreach my $attr (sort keys %{ $q->terms->{attr_terms}->{$boolean}->{'='}->{$field}->{$class_id} }){
					foreach my $value (@{ $q->terms->{attr_terms}->{$boolean}->{'='}->{$field}->{$class_id}->{$attr} }){
						if ($class_id){
							my $field = $attr;
							$field =~ s/^attr\_//; 
							push @{ $clauses{$boolean}->[ $Fields::Field_to_order->{$field} ] }, 'sub { $_[0] == ' . $class_id . ' and $_[1] eq ' . $value . ' }';
						}
						else {
							push @{ $clauses{$boolean}->[ $Fields::Field_to_order->{$field} ] }, 'sub { $_[1] == ' . $value . ' }';
						}
					}
				}
			}
		}
	}
	
	# Ranges are tougher: First sort by field name so we can group the ranges for the same field together in an OR
	my %ranges;
	foreach my $boolean (qw(and or not)){
		foreach my $op (sort keys %{ $q->terms->{attr_terms}->{$boolean} }){
			next unless $op =~ /\<|\>/;
			foreach my $field (sort keys %{ $q->terms->{attr_terms}->{$boolean}->{$op} }){
				foreach my $class_id (sort keys %{ $q->terms->{attr_terms}->{$boolean}->{$op}->{$field} }){		
					next unless $q->classes->{distinct}->{$class_id} or $class_id eq 0;
					foreach my $attr (sort keys %{ $q->terms->{attr_terms}->{$boolean}->{$op}->{$field}->{$class_id} }){
						$ranges{$boolean} ||= {};
						$ranges{$boolean}->{$field} ||= {};
						$ranges{$boolean}->{$field}->{$attr} ||= {};
						$ranges{$boolean}->{$field}->{$attr}->{$class_id} ||= {};
						$ranges{$boolean}->{$field}->{$attr}->{$class_id}->{$op} ||= [];
						foreach my $value (sort { $a <=> $b } @{ $q->terms->{attr_terms}->{$boolean}->{$op}->{$field}->{$class_id}->{$attr} }){
							push @{ $ranges{$boolean}->{$field}->{$attr}->{$class_id}->{$op} }, $value;
							# resort in case this is added on
							$ranges{$boolean}->{$field}->{$attr}->{$class_id}->{$op} = 
								[ sort { $a <=> $b } @{ $ranges{$boolean}->{$field}->{$attr}->{$class_id}->{$op} } ];
						}					
					}
				}				
			}
		}
	}
	
	# Then divine which range operators go together by sorting them and dequeuing the appropriate operator until there are none left
	foreach my $boolean (qw(and or not)){
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
							push @{ $clauses{$boolean}->[ $Fields::Field_to_order->{$attr} ] }, 'sub { $_[0] == ' . $class_id . ' and $_[1] ' . $min . ' and $_[1] <= ' . $max . ' }';
						}
						else {
							push @{ $clauses{$boolean}->[ $Fields::Field_to_order->{$attr} ] }, 'sub { $_[1] >= ' . $min . ' and $_[1] <= ' . $max . ' }';
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
		}
	}
	
	foreach my $boolean (keys %{ $q->terms->{any_field_terms} }){
		$clauses{'any_field_terms_' . $boolean} ||= [];
		foreach my $term (keys %{ $q->terms->{any_field_terms}->{$boolean} }){
			push @{ $clauses{'any_field_terms_' . $boolean} }, 'sub { $_[1] =~ qr/' . $term . '/io }';
		}
	}
	
	
	foreach my $class_id (sort keys %{ $q->classes->{distinct} }, sort keys %{ $q->classes->{partially_permitted} }){
		# First, the ANDs
		foreach my $field (sort keys %{ $q->terms->{field_terms}->{and}->{$class_id} }){
			foreach my $value (@{ $q->terms->{field_terms}->{and}->{$class_id}->{$field} }){
				push @{ $clauses{and}->[ $Fields::Field_to_order->{$field} ] }, 'sub { $_[0] == ' . $class_id . ' and $_[1] =~ qr/' . $value . '/io }';
			}
		}
				
		# Then, the NOTs
		foreach my $field (sort keys %{ $q->terms->{field_terms}->{not}->{$class_id} }){
			foreach my $value (@{ $q->terms->{field_terms}->{not}->{$class_id}->{$field} }){
				push @{ $clauses{not}->[ $Fields::Field_to_order->{$field} ] }, 'sub { $_[0] != ' . $class_id . ' or $_[1] !~ qr/' . $value . '/io }';
			}
		}
		
		# Then, the ORs
		foreach my $field (sort keys %{ $q->terms->{field_terms}->{or}->{$class_id} }){
			foreach my $value (@{ $q->terms->{field_terms}->{or}->{$class_id}->{$field} }){
				push @{ $clauses{or}->[ $Fields::Field_to_order->{$field} ] }, 'sub { $_[0] == ' . $class_id . ' and $_[1]  =~ qr/' . $value . '/io }';
			}
		}
	}
		
	return $self->json->encode(\%clauses);
#	$Storable::Deparse = 1;
#	
#	my $ret = freeze(\%clauses);
#	
#	$Storable::Deparse = 0;
#	
#	return $ret;
}

sub cancel_query {
	my ($self, $args) = @_;
	
	my ($query, $sth);
	$query = 'UPDATE query_log SET num_results=-2 WHERE qid=? AND uid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{qid}, $args->{user}->uid);
	return { ok => 1 };
}

sub cancel_livetail {
	my ($self, $args) = @_;
	
	my ($query, $sth);
	
	if ($args->{user}->is_admin){
		$query = 'UPDATE query_log SET num_results=-4 WHERE qid=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{qid});
		die('Invalid qid') unless $sth->rows;
	}
	else {
		$query = 'UPDATE query_log SET num_results=-4 WHERE qid=? AND uid=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{qid}, $args->{user}->uid);
		die('Invalid qid/uid') unless $sth->rows;
	}
	
	# Get our node info
	if (not $self->node_info->{updated_at} 
		or ($self->conf->get('node_info_cache_timeout') and ((time() - $self->node_info->{updated_at}) >= $self->conf->get('node_info_cache_timeout')))
		or not $args->{user}->is_admin){
		$self->node_info($self->_get_node_info($args->{user}));
	}
	
	$query = 'DELETE FROM livetail WHERE qid=?';
	foreach my $node (keys %{ $self->node_info->{nodes} }){
		$self->log->debug('cancelling livetail for qid ' . $args->{qid} . ' on node ' . $node);
		my $dbh = DBI->connect_cached(@{ $self->node_info->{nodes}->{$node}->{dbh}->db_args }) or die($DBI::errstr);
		$sth = $dbh->prepare($query);
		$sth->execute($args->{qid});
		$sth->finish;
	}
		
	return { ok => 1 };
}

sub cancel_all_livetails {
	my ($self, $args) = @_;
	
	die('Insufficient permission') unless $args->{user}->is_admin;
	
	my ($query, $sth);
	$query = 'SELECT qid, uid FROM query_log WHERE archive=1 AND num_results=-3';
	$sth = $self->db->prepare($query);
	$sth->execute();
	
	my $cancelled = 0;
	while (my $row = $sth->fetchrow_hashref){
		my $ret = $self->cancel_livetail({user => $args->{user}, qid => $row->{qid}});
		$cancelled++ if $ret and $ret->{ok};
	}
	return { ok => $cancelled };
}

sub expire_livetails {
	my ($self, $args) = @_;
	
	die('Insufficient permission') unless $args->{user}->is_admin;
	
	my ($query, $sth);
	$query = 'SELECT qid, uid FROM query_log WHERE archive=1 AND num_results=-3 AND milliseconds > (? * 1000)';
	$sth = $self->db->prepare($query);
	$sth->execute($self->conf->get('livetail/time_limit') ? $self->conf->get('livetail/time_limit') : 3600);
	
	my $cancelled = 0;
	while (my $row = $sth->fetchrow_hashref){
		my $ret = $self->cancel_livetail({user => $args->{user}, qid => $row->{qid}});
		$cancelled++ if $ret and $ret->{ok};
	}
	return { ok => $cancelled };
}

sub get_livetails {
	my ($self, $args) = @_;
	
	die('Insufficient permission') unless $args->{user}->is_admin;
	
	my ($query, $sth);
	$query = 'SELECT * FROM query_log WHERE archive=1 AND num_results=-3';
	$sth = $self->db->prepare($query);
	$sth->execute();
	
	my $ret = { results => [] };
	while (my $row = $sth->fetchrow_hashref){
		$row->{query} = $self->json->decode($row->{query});
		$row->{query_string} = $row->{query}->{query_string};
		$row->{query_meta_params} = $row->{query}->{query_meta_params};
		push @{ $ret->{results} }, $row;
	}
	return $ret;
}

sub preference {
	my ($self, $args) = @_;
	
	die('No user') unless $args->{user};
	
	my ($query, $sth);
	
	# Lower case these vars
	for my $var (qw(type name value)){
		if (exists $args->{$var}){
			$args->{$var} = lc($args->{$var});
		}
	}
	
	if ($args->{action} eq 'add'){
		$query = 'INSERT INTO preferences (uid, type, name, value) VALUES (?,?,?,?)';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{user}->uid, $args->{type}, $args->{name}, $args->{value});
	}
	elsif ($args->{action} eq 'remove'){
		$query = 'DELETE FROM preferences WHERE uid=? AND id=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{user}->uid, $args->{id});
	}
	elsif ($args->{action} eq 'update'){
		die('Need col/val') unless $args->{col} and defined $args->{val};
		if ($args->{col} eq 'name'){
			$query = 'UPDATE preferences SET name=? WHERE id=? AND uid=?';
		}
		elsif ($args->{col} eq 'value'){
			$query = 'UPDATE preferences SET value=? WHERE id=? AND uid=?';
		}	
		$sth = $self->db->prepare($query);
		$sth->execute($args->{val}, $args->{id}, $args->{user}->uid);
	}
	else {
		die('Invalid action');
	}
	
	return { ok => $sth->rows };
}

__PACKAGE__->meta->make_immutable;
1;