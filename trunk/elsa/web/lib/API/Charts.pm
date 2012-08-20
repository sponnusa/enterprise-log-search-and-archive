package API::Charts;
use Moose;
extends 'API';
use Data::Dumper;
use URI::Encode qw(uri_decode);

sub get_dashboards {
	my ($self, $args) = @_;
	my ($query, $sth);
	
	my $ret = [];
	$query = 'SELECT id, title, alias, auth_required, groupname FROM dashboards LEFT JOIN dashboard_auth ON (dashboards.id=dashboard_auth.dashboard_id) LEFT JOIN groups ON (dashboard_auth.gid=groups.gid) WHERE uid=? ORDER BY id DESC LIMIT ?,?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{user}->uid, $args->{startIndex} ? $args->{startIndex} : 0, $args->{results} ? $args->{results} : 10);
	
	while (my $row = $sth->fetchrow_hashref){
		push @$ret, $row;
	}
	
	return { 
		totalRecords => scalar @$ret,
		recordsReturned => scalar @$ret,
		results => $ret
	};
}

sub add_dashboard {
	my ($self, $args) = @_;
	
	my ($query, $sth);
	my $dashboard_id;
	
	eval {
		$args = $self->_import_dashboard($args) if $args->{data};
		
		if ($args->{groups}){
			$args->{auth_required} = 2;
		}
		defined $args->{auth_required} or $args->{auth_required} = 1; 
				
		die('Invalid alias, must be alphanumeric, hyphen, or underscore') unless $args->{alias} =~ /^[a-zA-Z0-9\_\-]+$/;
		$self->db->begin_work;
		$query = 'INSERT INTO dashboards (uid, title, alias, auth_required) VALUES(?,?,?,?)';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{user}->uid, $args->{title}, $args->{alias}, $args->{auth_required});
		
		$dashboard_id = $self->db->{mysql_insertid};
		
		if ($args->{groups}){
			my @groups = split(/\,/, $args->{groups});
			$query = 'INSERT INTO dashboard_auth (dashboard_id, gid) VALUES (?,(SELECT gid FROM groups WHERE groupname=?))';
			$sth = $self->db->prepare($query);
			foreach my $group (@groups){
				$sth->execute($dashboard_id, $group);
			}
		}
		
		if ($args->{charts}){
			foreach my $chart (@{ $args->{charts} }){
				$chart->{user} = $args->{user};
				$chart->{chart_type} = delete $chart->{type};
				$chart->{dashboard_id} = $dashboard_id;
				my $ret = $self->add($chart, 1);
			}
		}
		
		$self->db->commit;
	};
	if ($@){
		my $errstr = 'Error creating dashboard: ' . $@;
		$self->log->error($errstr);
		$self->db->rollback;
		die($errstr);
	}
	
	return { dashboard_id => $dashboard_id };
}

sub _import_dashboard {
	my ($self, $args) = @_;
	my ($query, $sth);
	
	$args->{data} = $self->json->decode(uri_decode($args->{data}));
	$self->log->info('Importing dashboard ' . Dumper($args->{data}));
	
	foreach my $var (qw(auth_required title alias charts)){
		$args->{$var} = $args->{data}->{$var} unless $args->{$var};
	}
	$self->log->debug('args now: ' . Dumper($args));
	
	return $args;
}

sub del_dashboard {
	my ($self, $args) = @_;
	my ($query, $sth);
	
	my $ok;
	eval {
		$self->db->begin_work;
		
		$self->log->info('Deleting dashboard ' . $args->{id});
		
		# Verify this dashboard belongs to this user
		$query = 'SELECT id FROM dashboards WHERE uid=? AND id=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{user}->uid, $args->{id});
		my $row = $sth->fetchrow_hashref;
		die('Dashboard does not belong to this user') unless $row;
		
		$query = 'DELETE FROM dashboards WHERE id=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{id});
		$ok = $sth->rows;
		
		$self->db->commit;
	};
	if ($@){
		my $errstr = 'Error deleting dashboard: ' . $@;
		$self->log->error($errstr);
		die($errstr);
		$self->db->rollback;
	}
	return { ok => $ok };
}

sub update_dashboard {
	my ($self, $args) = @_;
	
	$self->log->debug('args', Dumper($args));
	my ($query, $sth);
	my $query_id;
	eval {
		$self->db->begin_work;
		
		# Verify this dashboard belongs to this user
		$query = 'SELECT id FROM dashboards WHERE uid=? AND id=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{user}->uid, $args->{id});
		my $row = $sth->fetchrow_hashref;
		die('Dashboard does not belong to this user') unless $row;
		
		my $valid_cols = {
			title => 1,
			alias => 1,
			auth_required => 1,
			groupname => 1,
		};
		
		die('Invalid col') unless $valid_cols->{ $args->{col} };
		
		if ($args->{col} eq 'groupname'){
			$query = 'SELECT gid FROM groups WHERE groupname=?';
			$sth = $self->db->prepare($query);
			$sth->execute($args->{val});
			my $row = $sth->fetchrow_hashref;
			die('Invalid groupname ' . $args->{val}) unless $row;
			$query = 'DELETE FROM dashboard_auth WHERE dashboard_id=?';
			$sth = $self->db->prepare($query);
			$sth->execute($args->{id});
			$query = 'INSERT INTO dashboard_auth (dashboard_id, gid) VALUES (?,?)';
			$sth = $self->db->prepare($query);
			$sth->execute($args->{id}, $row->{gid});
		}
		else {
			$query = 'UPDATE dashboards SET ' . $args->{col} . '=? WHERE id=?'; # $col sanitized above
			$sth = $self->db->prepare($query);
			$sth->execute($args->{val}, $args->{id});
		}
		$self->log->debug('rows: ' . $sth->rows . ' updated col ' . $args->{col} . ' to ' . $args->{val});
		$self->log->debug('query: ' . $query . ', id=' . $args->{id});
		
		$self->db->commit;
	};
	if ($@){
		my $errstr = 'Error updating dashboard: ' . $@;
		$self->log->error($errstr);
		die($errstr);
		$self->db->rollback;
	}
	
	return { ok => $sth->rows };
}

sub export_dashboard {
	my ($self, $args) = @_;
	my ($query, $sth);
	
	$args->{id} = delete $args->{data};
	my $copy = { %$args };
	delete $copy->{user};
	$self->log->debug('args: ' . Dumper($copy));
	my $ret;
	eval {
		$self->db->begin_work;
		
		$self->log->info('Exporting dashboard ' . $args->{id});
		
		# Verify this dashboard belongs to this user
		$query = 'SELECT id FROM dashboards WHERE uid=? AND id=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{user}->uid, $args->{id});
		my $row = $sth->fetchrow_hashref;
		die('Dashboard does not belong to this user') unless $row;
		
		$query = 'SELECT title, alias, auth_required FROM dashboards WHERE id=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{id});
		$ret = $sth->fetchrow_hashref;
		$ret->{charts} = [];
		
		$query = 'SELECT id, type, options, x, y FROM charts JOIN dashboards_charts_map ON (charts.id=dashboards_charts_map.chart_id) WHERE dashboards_charts_map.dashboard_id=? ORDER BY x,y ASC';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{id});
		while ($row = $sth->fetchrow_hashref){
			$row->{queries} = [];
			$row->{options} = $self->json->decode($row->{options});
			
			$query = 'SELECT label, query FROM chart_queries WHERE chart_id=? ORDER BY id ASC';
			my $sth2 = $self->db->prepare($query);
			$sth2->execute($row->{id});
			while (my $row2 = $sth2->fetchrow_hashref){
				push @{ $row->{queries} }, $row2;
			}
			delete $row->{id};
			push @{ $ret->{charts} }, $row; 
		}
		
		$self->db->commit;
	};
	if ($@){
		my $errstr = 'Error exporting dashboard: ' . $@;
		$self->log->error($errstr);
		die($errstr);
		$self->db->rollback;
	}
	return $ret;
}

sub add {
	my ($self, $args, $no_xa) = @_;
	$self->log->debug('args ' . Dumper($args));
	
	my ($query, $sth);
	my $chart_id;
	my $ret = { %$args };
	delete $ret->{user};
	
	eval {
		$self->db->begin_work unless $no_xa;
		
		# Verify this dashboard belongs to this user
		$query = 'SELECT id FROM dashboards WHERE uid=? AND id=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{user}->uid, $args->{dashboard_id});
		my $row = $sth->fetchrow_hashref;
		die('Dashboard does not belong to this user: ' . $args->{user}->uid . ' ' . $args->{dashboard_id}) unless $row;
		
		my $options = $self->json->encode({ title => $args->{chart_title} });
		if ($args->{options}){
			if (ref($args->{options})){
				$options = $self->json->encode($args->{options});
			}
			else {
				$options = $args->{options};
			}
		}
		
		$query = 'INSERT INTO charts (uid, options, type) VALUES (?,?,?)';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{user}->uid, $options, $args->{chart_type});
		
		$chart_id = $self->db->{mysql_insertid};
		$ret->{chart_id} = $chart_id;
		$self->log->info('new chart ' . $chart_id);
		
		$query = 'INSERT INTO chart_queries (chart_id, label, query) VALUES (?,?,?)';
		$sth = $self->db->prepare($query);
		$args->{queries} = $self->json->decode($args->{queries}) unless ref($args->{queries});
		foreach my $query_hash (@{ $args->{queries} }){
			#$sth->execute($chart_id, $query_hash->{label}, $self->json->encode($query_hash->{query}));
			$self->log->debug($chart_id . ' ' . $query_hash->{label} . ' ' . $query_hash->{query});
			$sth->execute($chart_id, $query_hash->{label}, $query_hash->{query});
			$ret->{query_id} = $self->db->{mysql_insertid};
		}
		
		# Find next open slot for y
		if (not defined $args->{y}){
			$query = 'SELECT COUNT(*) AS count, MAX(y) AS y FROM v_dashboards WHERE dashboard_id=?';
			$sth = $self->db->prepare($query);
			$sth->execute($args->{dashboard_id});
			$row = $sth->fetchrow_hashref;
			$args->{y} = $row->{y} + 1 if $row->{count};
		}
		# Find next slot for x
		elsif (not defined $args->{x}){
			$query = 'SELECT COUNT(*) AS count, MAX(x) AS x FROM v_dashboards WHERE dashboard_id=? AND y=?';
			$sth = $self->db->prepare($query);
			$sth->execute($args->{dashboard_id}, $args->{y});
			$row = $sth->fetchrow_hashref;
			$args->{x} = $row->{x} + 1 if $row->{count};
		}
		
		# Sanity check
		if ($args->{x} > 2){
			die('Cannot have more than 3 charts on one line: ' . $args->{x});
		}
		if ($args->{y} > 100){
			die('Cannot have more than 100 chart lines');
		}
		
		$query = 'INSERT INTO dashboards_charts_map (dashboard_id, chart_id, x, y) VALUES (?,?,?,?)';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{dashboard_id}, $chart_id, $args->{x} ? $args->{x} : 0, $args->{y} ? $args->{y} : 0);
		
		$self->db->commit unless $no_xa;
	};
	if ($@){
		my $errstr = 'Error creating chart: ' . $@;
		$self->log->error($errstr);
		die($errstr);
		$self->db->rollback unless $no_xa;
	}
	
	return $ret;
}

sub del {
	my ($self, $args) = @_;
	
	my ($query, $sth);
	my $rows;
	eval {
		$self->db->begin_work;
		
		# Verify this chart belongs to this user
		$query = 'SELECT id FROM charts WHERE uid=? AND id=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{user}->uid, $args->{chart_id});
		my $row = $sth->fetchrow_hashref;
		die('Chart does not belong to this user') unless $row;
		
		if ($args->{dashboard_id}){
			# Verify this dashboard belongs to this user
			$query = 'SELECT id FROM dashboards WHERE uid=? AND id=?';
			$sth = $self->db->prepare($query);
			$sth->execute($args->{user}->uid, $args->{dashboard_id});
			my $row = $sth->fetchrow_hashref;
			die('Dashboard does not belong to this user') unless $row;
			
			# Find current coordinates
			$query = 'SELECT x, y FROM dashboards_charts_map WHERE dashboard_id=? AND chart_id=?';
			$sth = $self->db->prepare($query);
			$sth->execute($args->{dashboard_id}, $args->{chart_id});
			$row = $sth->fetchrow_hashref;
			
			# Disassociate chart from dashboard
			$query = 'DELETE FROM dashboards_charts_map WHERE dashboard_id=? AND chart_id=?';
			$sth = $self->db->prepare($query);
			$sth->execute($args->{dashboard_id}, $args->{chart_id});
			$rows = $sth->rows;
			
			# Shift other charts
			$query = 'UPDATE dashboards_charts_map SET x=(x-1) WHERE dashboard_id=? AND y=? AND x > ?';
			$sth = $self->db->prepare($query);
			$sth->execute($args->{dashboard_id}, $row->{y}, $row->{x});
			
			# Remove an empty row
			$query = 'SELECT COUNT(*) AS count FROM dashboards_charts_map WHERE dashboard_id=? AND y=?';
			$sth = $self->db->prepare($query);
			$sth->execute($args->{dashboard_id}, $row->{y});
			my $count_row = $sth->fetchrow_hashref;
			if ($count_row->{count} == 0){
				# Shift all higher rows
				$query = 'UPDATE dashboards_charts_map SET y=(y-1) WHERE dashboard_id=? AND y>=?';
				$sth = $self->db->prepare($query);
				$sth->execute($args->{dashboard_id}, $row->{y});
			}
			
			# Check to see if this was the only dashboard using the query
			$query = 'SELECT COUNT(*) AS count FROM dashboards_charts_map WHERE chart_id=?';
			$sth = $self->db->prepare($query);
			$sth->execute($args->{chart_id});
			$row = $sth->fetchrow_hashref;
			
			if ($row->{count} == 0){
				# Delete chart
				$query = 'DELETE FROM charts WHERE id=?';
				$sth = $self->db->prepare($query);
				$sth->execute($args->{chart_id});
				$rows = $sth->rows;
				$self->log->trace('deleted chart ' . $args->{chart_id});
			}
		}
		else {
			# Find current coordinates
			$query = 'SELECT dashboard_id, x, y FROM dashboards_charts_map WHERE chart_id=?';
			$sth = $self->db->prepare($query);
			$sth->execute($args->{chart_id});
			my @coordinates;
			while (my $row = $sth->fetchrow_hashref){
				push @coordinates, $row;
			}
			
			# Delete chart
			$query = 'DELETE FROM charts WHERE id=?';
			$sth = $self->db->prepare($query);
			$sth->execute($args->{chart_id});
			$rows = $sth->rows;
			
			# Shift other charts
			foreach my $row (@coordinates){
				$query = 'UPDATE dashboards_charts_map SET x=(x-1) WHERE dashboard_id=? AND y=? AND x > ?';
				$sth = $self->db->prepare($query);
				$sth->execute($row->{dashboard_id}, $row->{y}, $row->{x});
				$query = 'UPDATE dashboards_charts_map SET y=(y-1) WHERE dashboard_id=? AND y > ?';
				$sth = $self->db->prepare($query);
				$sth->execute($row->{dashboard_id}, $row->{y});
			}
		}
		$self->db->commit;
	};
	if ($@){
		my $errstr = 'Error deleting chart: ' . $@;
		$self->log->error($errstr);
		die($errstr);
		$self->db->rollback;
	}
	
	return { ok => $rows };
}

sub move {
	my ($self, $args) = @_;
	
	$self->log->trace('move args: ' . Dumper($args));
	my ($query, $sth);
	my $rows;
	eval {
		$self->db->begin_work;
		# Find current coordinates
		$query = 'SELECT chart_id, x, y FROM dashboards_charts_map WHERE dashboard_id=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{dashboard_id});
		my $coordinates = {};
		my $by_id = {};
		while (my $row = $sth->fetchrow_hashref){
			$coordinates->{ $row->{y} } ||= [];
			push @{ $coordinates->{ $row->{y} } }, $row;
			$by_id->{ $row->{chart_id} } = $row;
		}
		
		my $target = $by_id->{ $args->{chart_id} };
		die('chart not found') unless $target;
		$self->log->debug('target ' . Dumper($target));
		$self->log->debug('coordinates ' . Dumper($coordinates));
		
		if ($args->{direction} eq 'up'){
			if ($target->{y} > 0){
				# Move up
				$query = 'UPDATE dashboards_charts_map SET y=(y-1), x=? WHERE dashboard_id=? AND chart_id=?';
				$sth = $self->db->prepare($query);
				$sth->execute((scalar @{ $coordinates->{ ($target->{y} - 1) } }), $args->{dashboard_id}, $target->{chart_id});
				
				# Shift other charts
				$query = 'UPDATE dashboards_charts_map SET x=(x-1) WHERE dashboard_id=? AND y=? AND x > ?';
				$sth = $self->db->prepare($query);
				$sth->execute($args->{dashboard_id}, $target->{y}, $target->{x});
				$rows = $sth->rows;
			}
			else {
				die('Cannot move up');
			}
		}
		elsif ($args->{direction} eq 'down'){
			# Verify that we're not going to empty the row by moving down if this is the bottom
			if ((($target->{y} == (scalar keys %$coordinates) - 1) and scalar @{ $coordinates->{ $target->{y} } } > 1)
				or $target->{y} != ((scalar keys %$coordinates) - 1)){
				# Move down
				$query = 'UPDATE dashboards_charts_map SET y=(y+1), x=? WHERE dashboard_id=? AND chart_id=?';
				$sth = $self->db->prepare($query);
				my $new_x = 0;
				if (exists $coordinates->{ ($target->{y} + 1) }){
					$new_x = scalar @{ $coordinates->{ ($target->{y} + 1) } };
				}
				$sth->execute($new_x, $args->{dashboard_id}, $target->{chart_id});
				
				# Shift other charts
				$query = 'UPDATE dashboards_charts_map SET x=(x-1) WHERE dashboard_id=? AND y=? AND x > ?';
				$sth = $self->db->prepare($query);
				$sth->execute($args->{dashboard_id}, $target->{y}, $target->{x});
				$rows = $sth->rows;
				
				# Remove an empty row
				$query = 'SELECT COUNT(*) AS count FROM dashboards_charts_map WHERE dashboard_id=? AND y=?';
				$sth = $self->db->prepare($query);
				$sth->execute($args->{dashboard_id}, $target->{y});
				my $row = $sth->fetchrow_hashref;
				if ($row->{count} == 0){
					# Shift all higher rows
					$query = 'UPDATE dashboards_charts_map SET y=(y-1) WHERE dashboard_id=? AND y>=?';
					$sth = $self->db->prepare($query);
					$sth->execute($args->{dashboard_id}, $target->{y});
				}
			}
			else {
				die('Cannot move down');
			}
		}
		elsif ($args->{direction} eq 'right'){
			if ($target->{x} < (scalar @{ $coordinates->{ $target->{y} } })){
				# Swap with coordinate replaced, if any
				$query = 'UPDATE dashboards_charts_map SET x=(x-1) WHERE dashboard_id=? AND x=? AND y=?';
				$sth = $self->db->prepare($query);
				$sth->execute($args->{dashboard_id}, ($target->{x} + 1), $target->{y});
				$rows = $sth->rows;
				
				# Move right
				$query = 'UPDATE dashboards_charts_map SET x=(x+1) WHERE dashboard_id=? AND chart_id=?';
				$sth = $self->db->prepare($query);
				$sth->execute($args->{dashboard_id}, $target->{chart_id});
			}
			else {
				die('Cannot move right');
			}
		}
		elsif ($args->{direction} eq 'left'){
			if ($target->{x} > 0){
				# Swap with coordinate replaced, if any
				$query = 'UPDATE dashboards_charts_map SET x=(x+1) WHERE dashboard_id=? AND x=? AND y=?';
				$sth = $self->db->prepare($query);
				$sth->execute($args->{dashboard_id}, ($target->{x} - 1), $target->{y});
				$rows = $sth->rows;
				
				# Move left
				$query = 'UPDATE dashboards_charts_map SET x=(x-1) WHERE dashboard_id=? AND chart_id=?';
				$sth = $self->db->prepare($query);
				$sth->execute($args->{dashboard_id}, $target->{chart_id});

			}
			else {
				die('Cannot move left');
			}
		}
		
		$self->db->commit;
	};
	if ($@){
		my $errstr = 'Error moving chart: ' . $@;
		$self->log->error($errstr);
		die($errstr);
		$self->db->rollback;
	}
	return $self->get_rows($args);
}

sub _is_permitted {
	my ($self, $args) = @_;
	my ($query, $sth);
	
	# Check authorization
	my $is_authorized = 0;
	$query = 'SELECT dashboard_id, dashboard_title, alias, auth_required FROM v_dashboards WHERE alias=? ORDER BY x,y';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{dashboard_name});
	my $row = $sth->fetchrow_hashref;
	if ($row->{auth_required}){
		if ($row->{auth_required} == 1 and $args->{user}){
			$self->log->trace('user authentication sufficient');
			$is_authorized = 1;
		}
		elsif ($row->{auth_required} == 2){
			$query = 'SELECT groupname FROM groups WHERE gid IN (SELECT gid FROM dashboard_auth WHERE dashboard_id=?)';
			$sth = $self->db->prepare($query);
			$sth->execute($args->{id});
			AUTH_LOOP: while (my $row = $sth->fetchrow_hashref){
				foreach my $groupname (@{ $args->{user}->groups }){
					if ($row->{groupname} eq $groupname){
						$self->log->trace('Authorizing based on membership in group ' . $groupname);
						$is_authorized = 1;
						last AUTH_LOOP;
					}
				}
			}
		}
	}
	else {
		$self->log->trace('no auth required');
		$is_authorized = 1;
	}
	
	return $is_authorized;
}

sub _get_auth_token {
	my ($self, $args) = @_;
	return $self->get_hash($args->{query} . $args->{label} . $args->{query_id});
}

sub _check_auth_token {
	my ($self, $args) = @_;
	return $args->{auth} eq $self->get_hash($args->{query_string} . $args->{label} . $args->{query_id}) ? 1 : 0;
}

sub _get_rows {
	my ($self, $args) = @_;
	my ($query, $sth, $ret);
	
	# Double-check auth so public API can't abuse this
	unless ($self->_is_permitted($args)){
		die('Unauthorized');
	}
	
	if ($args->{dashboard_name}){
		$query = 'SELECT * FROM v_dashboards WHERE alias=? ORDER BY x,y';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{dashboard_name});
	}
	else {
		$query = 'SELECT * FROM v_dashboards WHERE dashboard_id=? ORDER BY x,y';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{dashboard_id});
	}
	
	while (my $row = $sth->fetchrow_hashref){
		next unless defined $row->{chart_id};
		$ret->[ $row->{y} ] ||= { title => '', charts => [] };
		$ret->[ $row->{y} ]->{charts}->[ $row->{x} ] ||= { 
			title => $row->{chart_title}, 
			type => $row->{chart_type}, 
			queries => [], 
			chart_id => $row->{chart_id}, 
			chart_options => $row->{chart_options} ? $self->json->decode($row->{chart_options}) : undef,
			x => $row->{x},
			y => $row->{y}, 
		};
		$self->log->debug('query: ' . $row->{query});
		#push @{ $ret->[ $row->{y} ]->{charts}->[ $row->{x} ]->{queries} }, { query => $self->json->decode($row->{query}), label => $row->{label}, query_id => $row->{query_id} }; 
		push @{ $ret->[ $row->{y} ]->{charts}->[ $row->{x} ]->{queries} }, { 
			query_string => $row->{query}, 
			label => $row->{label}, 
			query_id => $row->{query_id},
			auth => $self->_get_auth_token($row),
		};
	}

	$self->log->debug('ret: ' . Dumper($ret));
	foreach my $chart_row (@$ret){
		foreach my $chart (@{ $chart_row->{charts} }){
			foreach my $query (@{ $chart->{queries} }){
				my $query_meta_params = {
					start => $args->{start_time},
					end => $args->{end_time},
					comment => $query->{label},
					type => $chart->{type},
				};
				$query_meta_params->{groupby} = [$args->{groupby}] unless $query->{query_string} =~ /\sgroupby[:=]/ or $query->{query_string} =~ /sum\([^\)]+\)$/;
				$query->{query_meta_params} = $query_meta_params;
			}
		}
	}
	return $ret;
}

sub add_query {
	my ($self, $args) = @_;
	
	my ($query, $sth);
	my $query_id;
	eval {
		$self->db->begin_work;
		
#		unless ($args->{query} =~ /groupby[\:\=]/i){
#			die('Report on field (groupby) not specified');
#		}
		
		$self->log->debug('$args->{chart_id} ' . Dumper($args->{chart_id}));
		if ($args->{chart_id} eq '__NEW__'){
			die('No dashboard_id defined') unless defined $args->{dashboard_id};
			$self->db->rollback();
			$args->{queries} = [ { label => $args->{label}, query => $args->{query} } ];
			$args->{chart_type} = 'ColumnChart';
			return $self->add($args);
		}
		else {
			# Verify this chart belongs to this user
			$query = 'SELECT id FROM charts WHERE uid=? AND id=?';
			$sth = $self->db->prepare($query);
			$sth->execute($args->{user}->uid, $args->{chart_id});
			my $row = $sth->fetchrow_hashref;
			die('Chart does not belong to this user') unless $row;
		}
		
		
				
#		unless ($args->{query} =~ /query_meta_params/){
#			$self->log->trace('Converting raw query text to query object');
#			$args->{query} = $self->json->encode({ query_string => $args->{query}, query_meta_params => {} });
#		}
		
		$query = 'INSERT INTO chart_queries (chart_id, label, query) VALUES (?,?,?)';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{chart_id}, $args->{label}, $args->{query});
		$query_id = $self->db->{mysql_insertid};
		
		$self->db->commit;
	};
	if ($@){
		my $errstr = 'Error creating query: ' . $@;
		$self->log->error($errstr);
		die($errstr);
		$self->db->rollback;
	}
	
	return { query_id => $query_id, label => $args->{label}, query => $args->{query} };
}

sub del_query {
	my ($self, $args) = @_;
	
	my ($query, $sth);
	my $ok;
	eval {
		$self->db->begin_work;
		
		# Verify this chart belongs to this user
		$query = 'SELECT query_id FROM v_dashboards WHERE uid=? AND query_id=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{user}->uid, $args->{query_id});
		my $row = $sth->fetchrow_hashref;
		die('Chart does not belong to this user') unless $row;
		
		$query = 'DELETE FROM chart_queries WHERE id=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{query_id});
		$ok = $sth->rows;
		
		$self->db->commit;
	};
	if ($@){
		my $errstr = 'Error deleting query: ' . $@;
		$self->log->error($errstr);
		die($errstr);
		$self->db->rollback;
	}
	
	return { ok => $ok };
}

sub update_query {
	my ($self, $args) = @_;
	
	my ($query, $sth);
	my $query_id;
	eval {
		$self->db->begin_work;
		
		# Verify this chart belongs to this user
		$query = 'SELECT id FROM charts WHERE uid=? AND id=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{user}->uid, $args->{chart_id});
		my $row = $sth->fetchrow_hashref;
		die('Chart does not belong to this user') unless $row;
		
		die('Invalid col') unless $args->{col} eq 'label' or $args->{col} eq 'query';
		
#		if ($args->{col} eq 'query' and $args->{val} !~ /query_meta_params/){
#			$self->log->trace('Converting raw query text to query object');
#			$args->{val} = $self->json->encode({ query_string => $args->{val}, query_meta_params => {} });
#		}
		
		$query = 'UPDATE chart_queries SET ' . $args->{col} . '=? WHERE id=?'; # args->col sanitized above
		$sth = $self->db->prepare($query);
		$sth->execute($args->{val}, $args->{query_id});
		$query_id = $self->db->{mysql_insertid};
		
		$self->db->commit;
	};
	if ($@){
		my $errstr = 'Error creating query: ' . $@;
		$self->log->error($errstr);
		die($errstr);
		$self->db->rollback;
	}
	
	return { ok => $sth->rows };
}

sub update {
	my ($self, $args) = @_;
	
	$self->log->debug('args', Dumper($args));
	my ($query, $sth);
	my $query_id;
	eval {
		$self->db->begin_work;
		
		# Verify this chart belongs to this user
		$query = 'SELECT id FROM charts WHERE uid=? AND id=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{user}->uid, $args->{chart_id});
		my $row = $sth->fetchrow_hashref;
		die('Chart does not belong to this user') unless $row;
		
		my $to_update = $self->json->decode($args->{to_update});
		my $valid_cols = {
			type => 1,
			options => 1,
		};
		foreach my $col (keys %$to_update){
			die('Invalid col ' . $col) unless $valid_cols->{$col};
		
			$query = 'UPDATE charts SET ' . $col . '=? WHERE id=?'; # $col sanitized above
			$sth = $self->db->prepare($query);
			my $value = $to_update->{$col};
			if (ref($to_update->{$col})){
				$value = $self->json->encode($to_update->{$col});
			}
			$sth->execute($value, $args->{chart_id});
			$self->log->debug('rows: ' . $sth->rows . ' updated col ' . $col . ' to ' . $value);
			$self->log->debug('query: ' . $query . ', id=' . $args->{chart_id});
		}
		
		$self->db->commit;
	};
	if ($@){
		my $errstr = 'Error creating query: ' . $@;
		$self->log->error($errstr);
		die($errstr);
		$self->db->rollback;
	}
	
	return { ok => $sth->rows };
}

sub get {
	my ($self, $args) = @_;
	my ($query, $sth);
	
	if ($args->{dashboard_id}){
		$query = 'SELECT DISTINCT chart_id, chart_type, x, y, chart_options FROM v_dashboards WHERE uid=? AND dashboard_id=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{user}->uid, $args->{dashboard_id});
	}
	elsif ($args->{chart_id}){
		$query = 'SELECT DISTINCT chart_id, chart_type, x, y, chart_options FROM v_dashboards WHERE uid=? AND chart_id=?';
		$sth = $self->db->prepare($query);
		$sth->execute($args->{user}->uid, $args->{chart_id});
	}
	
	my @charts;
	while (my $row = $sth->fetchrow_hashref){
		next unless $row->{chart_type};
		$row->{chart_options} = $self->json->decode($row->{chart_options}) if $row->{chart_options};
		push @charts, $row;
	}
	
	$query = 'SELECT chart_id, query_id, label, query FROM v_dashboards WHERE uid=? AND chart_id=?';
	$sth = $self->db->prepare($query);
	my @queries;
	foreach my $chart (@charts){
		$sth->execute($args->{user}->uid, $chart->{chart_id});
		while (my $row = $sth->fetchrow_hashref){
			#$row->{query} = $self->json->decode(delete $row->{query});
			#$row->{query_string} = delete $row->{query};
			push @queries, $row;
		}
	}
	return { 
		totalRecords => scalar @charts,
		recordsReturned => scalar @charts,
		charts => \@charts,
		queries => \@queries,
	};
}

sub get_all {
	my ($self, $args) = @_;
	my ($query, $sth);
	
	$query = 'SELECT DISTINCT dashboard_id, alias, chart_id, chart_type, x, y, chart_options FROM v_dashboards WHERE uid=?';
	$sth = $self->db->prepare($query);
	$sth->execute($args->{user}->uid);
	my @charts;
	while (my $row = $sth->fetchrow_hashref){
		$row->{chart_options} = $self->json->decode($row->{chart_options});
		push @charts, $row;
	}
		
	return { 
		totalRecords => scalar @charts,
		recordsReturned => scalar @charts,
		results => \@charts,
	};
}

__PACKAGE__->meta->make_immutable;


1;