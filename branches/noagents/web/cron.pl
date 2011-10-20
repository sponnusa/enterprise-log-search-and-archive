#!/usr/bin/perl
use strict;
use Getopt::Std;
use Net::Server::Daemonize qw(daemonize);
use Mail::Internet;
use FindBin;

use lib $FindBin::Bin . '/lib';

my %opts;
getopts('Ddc:', \%opts);

my $config_file_name = -f '/etc/elsa.conf' ? '/etc/elsa.conf' : '/usr/local/elsa/etc/elsa.conf';
if ($opts{c}){
	$config_file_name = $opts{c};
}


sub _run_schedule {
	my ( $self, $kernel, $session, $heap, $state ) = @_[ OBJECT, KERNEL, SESSION, HEAP, STATE ];
	
	$self->log->debug('Current number of events in queue: ' . $kernel->get_event_count());
	
	# Reset the schedule alarm
	$kernel->delay($state, $self->conf->get('schedule_interval'));
	
	my ($query, $sth);
	
	# Find the last run time from the bookmark table
	$query = 'SELECT UNIX_TIMESTAMP(last_run) FROM schedule_bookmark';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute();
	my $row = $sth->fetchrow_arrayref;
	my $last_run_bookmark = $self->conf->get('schedule_interval'); # init to interval here so we don't underflow if 0
	if ($row){
		$last_run_bookmark = $row->[0];
	}
	
	# Get form params from backend so we can get the latest available index time
	my $backend_msg = POE::Event::Message->package({});
	$backend_msg->setMode('call');
	$backend_msg->addRouteTo('post', 'agent', 'execute', 'web', 'get_form_params');
	$backend_msg->addRemoteRouteTo(
		$self->conf->get('cluster/server'),
		$self->conf->get('cluster/port'),
		'sync',
	);
	
	my $form_params_msg;
	eval {	
		local $SIG{ALRM} = sub { die 'alarm'; };
		alarm $self->conf->get('cluster/timeout');
		($form_params_msg) = $backend_msg->route();
		alarm 0;
	};
	if ($@){
		$self->log->error('Cluster connection timed out after ' . $self->conf->get('cluster/timeout') . ' seconds');
		$kernel->yield( '_error', 'Cluster connection timed out after ' . $self->conf->get('cluster/timeout') . ' seconds', $backend_msg);
		
		return;
	}
	my $form_params = $form_params_msg->body();
	
	# Expire schedule entries
	$query = 'SELECT id, query, username FROM query_schedule JOIN users ON (query_schedule.uid=users.uid) WHERE end < UNIX_TIMESTAMP() AND enabled=1';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute();
	my @ids;
	while (my $row = $sth->fetchrow_hashref){
		push @ids, $row->{id};
		
		my $user_info = $kernel->call( $session, '_get_user_info', $row->{username} );
		
		my $decode = decode_json($row->{query});
		
		my $headers = {
			To => $user_info->{email},
			From => $self->conf->get('email/display_address') ? $self->conf->get('email/display_address') : 'system',
			Subject => 'ELSA alert has expired for query ' . $decode->{query_params},
		};
		my $body = 'The alert set for query ' . $decode->{query_params} . ' has expired and has been disabled.  ' .
			'If you wish to continue receiving this query, please log into ELSA, enable the query, and set a new expiration date.';
		
		$kernel->yield('_send_email', $headers, $body);
	}
	if (scalar @ids){
		$self->log->info('Expiring query schedule for ids ' . join(',', @ids));
		$query = 'UPDATE query_schedule SET enabled=0 WHERE id IN (' . join(',', @ids) . ')';
		$sth = $heap->{dbh}->prepare($query);
		$sth->execute;
	}
	
	# Run schedule	
	$query = 'SELECT t1.id AS query_schedule_id, username, t1.uid, query, frequency, start, end, action_subroutine, action_params' . "\n" .
		'FROM query_schedule t1' . "\n" .
		'JOIN users ON (t1.uid=users.uid)' . "\n" .
		'JOIN query_schedule_actions t2 ON (t1.action_id=t2.action_id)' . "\n" .
		'WHERE start <= ? AND end >= ? AND enabled=1' . "\n" .
		'AND UNIX_TIMESTAMP(NOW()) - UNIX_TIMESTAMP(last_alert) > alert_threshold';  # we won't even run queries we know we won't alert on
	$sth = $heap->{dbh}->prepare($query);
	
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
				$self->log->debug('prev: ' . Dumper(\@prev_dates));
				$last_run = UnixDate($prev_dates[$#prev_dates], '%s');
				$self->log->debug('last_run:' . $prev_dates[$#prev_dates]);
			}
			else {
				# Keep squaring the distance we'll go back to find the last date
				$farthest_back_to_check -= $how_far_back;
				$self->log->debug('how_far_back: ' . $how_far_back);
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
		$self->log->debug('dates: ' . Dumper(\@dates) . ' row: ' . Dumper($row));
		if (scalar @dates){
			# Adjust the query time to avoid time that is potentially unindexed by offsetting by the schedule interval
			my $query = decode_json($row->{query});
			$query->{query_meta_params}->{start} = ($last_run - $self->conf->get('schedule_interval'));
			$query->{query_meta_params}->{end} = ($cur_time - $self->conf->get('schedule_interval'));
			
			my $args = { 
				q => encode_json($query),
				session_id => 1,
			};
			my $msg = POE::Event::Message->package($args);
			if (!$user_info_cache->{ $row->{uid} }){
				$user_info_cache->{ $row->{uid} } = $kernel->call( $session, '_get_user_info', $row->{username} );
				$self->log->debug('Got user info: ' . Dumper($user_info_cache->{ $row->{uid} }));
			}
			else {
				$self->log->debug('Using existing user info');
			}
			
			$msg->param('_user', $user_info_cache->{ $row->{uid} });
			$msg->addRouteBack('post', undef, $row->{action_subroutine});
			#TODO find a better, non-duck-tape way of doing this
			my $action_params = { 
				comments => 'Scheduled Query ' . $row->{query_schedule_id}, 
				query_schedule_id => $row->{query_schedule_id},
				query => $query
			};
			if ($row->{action_params}){
				my $stored_params = decode_json($row->{action_params});
				foreach my $stored_param (keys %{ $stored_params }){
					$action_params->{$stored_param} = $stored_params->{$stored_param};
				}
			}
			$msg->param('_action_params', $action_params);
			$msg->addRouteTo('post', undef, 'query');
			$self->log->debug('routing query msg: ' . Dumper($msg));
			$msg->route();
		} 
	}
	
	# Update our bookmark to the current run
	$query = 'UPDATE schedule_bookmark SET last_run=FROM_UNIXTIME(?)';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($cur_time);
	unless ($sth->rows){
		$query = 'INSERT INTO schedule_bookmark (last_run) VALUES (FROM_UNIXTIME(?))';
		$sth = $heap->{dbh}->prepare($query);
		$sth->execute($cur_time);
	}
}


sub _send_email {
	my ( $self, $kernel, $session, $heap, $headers, $body ) = @_[ OBJECT, KERNEL, SESSION, HEAP, ARG0..ARG1 ];
	
	$self->log->debug('headers: ' . Dumper($headers));
	
	# Send the email
	my $email_headers = new Mail::Header();
	$email_headers->header_hashref($headers);
	my $email = new Mail::Internet( Header => $email_headers, Body => [ split(/\n/, $body) ] );
	
	$self->log->debug('email: ' . $email->as_string());
	$email->smtpsend(
		Host => $self->conf->get('email/smtp_server'), 
		Debug => 1, 
		MailFrom => $self->conf->get('email/display_address')
	);
	$self->log->debug('done sending email');
}

sub _open_ticket {
	my ( $self, $kernel, $session, $heap ) = @_[ OBJECT, KERNEL, SESSION, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	$self->log->debug('got results to create ticket on: ' . Dumper($msg));
	unless (ref($msg->body()) eq 'HASH' 
		and $msg->body()->{results} 
		and ref($msg->body()->{results}) eq 'ARRAY'
		and scalar @{ $msg->body()->{results} }){
		$self->log->info('No results for query');
		$msg->setErr(ERROR, 'No results for query');
		$msg->route();
		return 0;
	}
	
	my $meta_info = { %{ $msg->body() } };
	if ($msg->param('_action_params') and ref($msg->param('_action_params')) eq 'HASH'){
		foreach my $key (keys %{ $msg->param('_action_params') }){
			$meta_info->{$key} = $msg->param('_action_params')->{$key};
		}
	}
	
	$self->log->debug('meta_info: ' . Dumper($meta_info));
	
	unless ($self->conf->get('ticketing/email')){
		$self->log->error('No ticketing config setup.');
		return;
	}
	
	my $headers = {
		To => $self->conf->get('ticketing/email'),
		From => $self->conf->get('email/display_address') ? $self->conf->get('email/display_address') : 'system',
		Subject => $self->conf->get('email/subject') ? $self->conf->get('email/subject') : 'system',
	};
	my $body = sprintf($self->conf->get('ticketing/template'), $meta_info->{query}->{query_params},
		sprintf('%s/get_results?qid=%d&hash=%s', 
			$self->conf->get('email/base_url') ? $self->conf->get('email/base_url') : 'http://localhost',
			$meta_info->{qid},
			$meta_info->{hash},
		)
	);
	
	$kernel->yield('_send_email', $meta_info->{query_schedule_id}, $headers, $body, $msg->body());
}

sub _alert {
	my ( $self, $kernel, $session, $heap ) = @_[ OBJECT, KERNEL, SESSION, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	$self->log->debug('got results to alert on: ' . Dumper($msg));
	
	# Route back if something is wrong
	if ($msg->status()){
		$self->log->error('Got msg status: ' . $msg->status() . ' from msg ' . Dumper($msg));
		$msg->routeBack();
		return;
	}
	unless (ref($msg->body()) eq 'HASH' 
		and $msg->body()->{results} 
		and ref($msg->body()->{results}) eq 'ARRAY'
		and scalar @{ $msg->body()->{results} }){
		$self->log->info('No results for query');
		$msg->setErr(ERROR, 'No results for query');
		$msg->route();
		return 0;
	}
	
	my $meta_info = { %{ $msg->body() } };
	if ($msg->param('_action_params') and ref($msg->param('_action_params')) eq 'HASH'){
		foreach my $key (keys %{ $msg->param('_action_params') }){
			$meta_info->{$key} = $msg->param('_action_params')->{$key};
		}
	}
	
	$self->log->debug('meta_info: ' . Dumper($meta_info));
	
	my $headers = {
		To => $msg->param('_user')->{email},
		From => $self->conf->get('email/display_address') ? $self->conf->get('email/display_address') : 'system',
		Subject => $self->conf->get('email/subject') ? $self->conf->get('email/subject') : 'system',
	};
	my $body = sprintf('%d results for query %s', $meta_info->{recordsReturned}, $meta_info->{query}->{query_params}) .
		"\r\n" . sprintf('%s/get_results?qid=%d&hash=%s', 
			$self->conf->get('email/base_url') ? $self->conf->get('email/base_url') : 'http://localhost',
			$meta_info->{qid},
			$meta_info->{hash},
	);
	
	my ($query, $sth);
	$query = 'SELECT UNIX_TIMESTAMP(last_alert) AS last_alert, alert_threshold FROM query_schedule WHERE id=?';
	$sth = $heap->{dbh}->prepare($query);
	$sth->execute($meta_info->{query_schedule_id});
	my $row = $sth->fetchrow_hashref;
	if ((time() - $row->{last_alert}) < $row->{alert_threshold}){
		$self->log->warn('Not alerting because last alert was at ' . (scalar localtime($row->{last_alert})) 
			. ' and threshold is at ' . $row->{alert_threshold} . ' seconds.' );
		return;
	}
	else {
		$query = 'UPDATE query_schedule SET last_alert=NOW() WHERE id=?';
		$sth = $heap->{dbh}->prepare($query);
		$sth->execute($meta_info->{query_schedule_id});
	}
	
	$kernel->yield('_send_email', $headers, $body, $msg->body());
	
	# Save these results asynchronously
	my $save_msg = POE::Event::Message->package({results => $msg->body()->{results}, qid => $meta_info->{qid} });
	$save_msg->param('_action_params', { comments => 'Scheduled Query ' . $meta_info->{query_schedule_id} });
	$save_msg->addRouteTo('post', undef, '_save_results');
	$save_msg->route();
	
	
	# Make sure this continues to go wherever it needs to
	$msg->route();
}

sub _batch_notify {
	my ( $self, $kernel, $session, $heap ) = @_[ OBJECT, KERNEL, SESSION, HEAP ];
	my ($msg, $args) = _get_msg(@_);
	$self->log->debug('got results for batch: ' . Dumper($msg));
	
	# Route back if something is wrong
	if ($msg->status()){
		$self->log->error('Got msg status: ' . $msg->status() . ' from msg ' . Dumper($msg));
		$msg->routeBack();
		return;
	}
	unless (ref($msg->body()) eq 'HASH' 
		and $msg->body()->{results} 
		and ref($msg->body()->{results}) eq 'ARRAY'){
		my $errmsg = 'Did not get a valid msg body back from batched query';
		$self->log->error($errmsg);
		$msg->setErr(ERROR, $errmsg);
		$msg->route();
		return 0;
	}
	
	my $meta_info = { %{ $msg->body() } };
	if ($msg->param('_action_params') and ref($msg->param('_action_params')) eq 'HASH'){
		foreach my $key (keys %{ $msg->param('_action_params') }){
			$meta_info->{$key} = $msg->param('_action_params')->{$key};
		}
	}
	
	$self->log->debug('meta_info: ' . Dumper($meta_info));
	
	my $headers = {
		To => $msg->param('_user')->{email},
		From => $self->conf->get('email/display_address') ? $self->conf->get('email/display_address') : 'system',
		Subject => sprintf('ELSA batch query %d complete with %d results', $meta_info->{qid}, $meta_info->{recordsReturned}),
	};
	my $body = sprintf('%d results for query %s', $meta_info->{recordsReturned}, $meta_info->{query}->{query_params}) .
		"\r\n" . sprintf('%s/get_results?qid=%d&hash=%s', 
			$self->conf->get('email/base_url') ? $self->conf->get('email/base_url') : 'http://localhost',
			$meta_info->{qid},
			$meta_info->{hash},
	);
	
	$kernel->yield('_send_email', $headers, $body, $msg->body());
	
	# Save these results asynchronously
	my $save_msg = POE::Event::Message->package($meta_info);
	$save_msg->param('_action_params', { comments => 'Batch Query ' . $meta_info->{qid} });
	$save_msg->addRouteTo('post', undef, '_save_results');
	$save_msg->route();
	
	# Make sure this continues to go wherever it needs to
	$msg->route();
}