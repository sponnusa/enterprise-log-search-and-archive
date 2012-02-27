package Connector::Email;
use Moose;
use Data::Dumper;
use MIME::Base64;
extends 'Connector';

our $Description = 'Send email';
sub description { return $Description }

has 'query_schedule_id' => (is => 'rw', isa => 'Num', required => 1);
has 'qid' => (is => 'rw', isa => 'Num', required => 1);
has 'records_returned' => (is => 'rw', isa => 'Num', required => 1);

sub BUILDARGS {
	my $class = shift;
	my %params = @_;
	
	if (ref($params{results}) eq 'HASH' and $params{results}->{results}){
		$params{records_returned} = $params{results}->{recordsReturned};
		$params{results} = delete $params{results}->{results};
	}
	
	return \%params;
}

sub BUILD {
	my $self = shift;
	$self->api->log->debug('got results to alert on: ' . Dumper($self->results));
		
	unless (scalar @{ $self->results }){
		$self->api->log->info('No results for query');
		return 0;
	}
	
	my $headers = {
		To => $self->user_info->{email},
		From => $self->api->conf->get('email/display_address') ? $self->api->conf->get('email/display_address') : 'system',
		Subject => $self->api->conf->get('email/subject') ? $self->api->conf->get('email/subject') : 'system',
	};
	my $body = sprintf('%d results for query %s', $self->records_returned, $self->query->{query_string}) .
		"\r\n" . sprintf('%s/get_results?qid=%d&hash=%s', 
			$self->api->conf->get('email/base_url') ? $self->api->conf->get('email/base_url') : 'http://localhost',
			$self->qid,
			$self->api->_get_hash($self->qid),
	);
	
	my ($query, $sth);
	$query = 'SELECT UNIX_TIMESTAMP(last_alert) AS last_alert, alert_threshold FROM query_schedule WHERE id=?';
	$sth = $self->api->db->prepare($query);
	$sth->execute($self->query_schedule_id);
	my $row = $sth->fetchrow_hashref;
	if ((time() - $row->{last_alert}) < $row->{alert_threshold}){
		$self->api->log->warn('Not alerting because last alert was at ' . (scalar localtime($row->{last_alert})) 
			. ' and threshold is at ' . $row->{alert_threshold} . ' seconds.' );
		return;
	}
	else {
		$query = 'UPDATE query_schedule SET last_alert=NOW() WHERE id=?';
		$sth = $self->api->db->prepare($query);
		$sth->execute($self->query_schedule_id);
	}
	
	$self->api->send_email({ headers => $headers, body => $body});
	
	# Save the results
	$self->api->save_results({
		meta_info => { groupby => $self->query->{query_meta_params}->{groupby} },
		qid => $self->qid, 
		results => $self->results, 
		comments => 'Scheduled Query ' . $self->query_schedule_id
	});
}

1