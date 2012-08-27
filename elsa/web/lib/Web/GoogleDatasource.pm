package Web::GoogleDatasource;
use Moose;
extends 'Web';
use Data::Dumper;
use Plack::Request;
use Plack::Session;
use Encode;
use Scalar::Util;
use Data::Google::Visualization::DataSource;
use Data::Google::Visualization::DataTable;
use DateTime;

with 'Fields';

sub call {
	my ($self, $env) = @_;
    $self->session(Plack::Session->new($env));
	my $req = Plack::Request->new($env);
	my $res = $req->new_response(200); # new Plack::Response
	$res->content_type('text/plain');
	$res->header('Access-Control-Allow-Origin' => '*');
	
	$self->api->clear_warnings;
	
	my $args = $req->parameters->as_hashref;
	if ($self->session->get('user')){
		$args->{user} = $self->api->get_stored_user($self->session->get('user'));
	}
	else {
		$args->{user} = $self->api->get_user($req->user);
	}

	my $datasource = Data::Google::Visualization::DataSource->new({
	    tqx => $args->{tqx},
	    xda => ($req->header('X-DataSource-Auth') || undef)
	 });

	my $ret;
	eval {
		my $check_args = $self->api->json->decode($args->{q});
		my $query_args = $self->api->_get_query($check_args) or die('Query not found'); # this is now from the database, so we can trust the input
		$query_args->{auth} = $check_args->{auth};
		$query_args->{query_meta_params} = $check_args->{query_meta_params};
		$query_args->{user} = $args->{user};
		
		unless ($query_args->{uid} eq $args->{user}->uid){
			die('Invalid auth token') unless $self->api->_check_auth_token($query_args);
			$self->api->log->info('Running query created by ' . $query_args->{username} . ' on behalf of ' . $req->user);
			$query_args->{user} = $self->api->get_user(delete $query_args->{username});
		}
		
		$self->api->freshen_db;
		$ret = $self->api->query($query_args);
		unless ($ret){
			$ret = { error => $self->api->last_error };
		}
	};
	if ($@){
		my $e = $@;
		$self->api->log->error($e);
		$datasource->add_message({type => 'error', reason => 'access_denied', message => $e});
		my ($headers, $body) = $datasource->serialize;
		$res->headers(@$headers);
		$res->body([encode_utf8($body)]);
	}
	else {
		my $datatable = Data::Google::Visualization::DataTable->new();
	
		if ($ret->has_groupby){
			$self->api->log->debug('ret: ' . Dumper($ret));
			foreach my $groupby ($ret->all_groupbys){
				my $label = $ret->meta_params->{comment} ? $ret->meta_params->{comment} : 'count'; 
				if ($Fields::Time_values->{$groupby}){
					$datatable->add_columns({id => 'key', label => $groupby, type => 'datetime'}, {id => 'value', label => $label, type => 'number'});
					my $tz = DateTime::TimeZone->new( name => "local");
					foreach my $row (@{ $ret->results->results->{$groupby} }){
						$self->api->log->debug('row: ' . Dumper($row));
						$datatable->add_rows([ { v => DateTime->from_epoch(epoch => $row->{'intval'}, time_zone => $tz) }, { v => $row->{'@count'} } ]);
					}
				}
				else {
					$datatable->add_columns({id => 'key', label => $groupby, type => 'string'}, {id => 'value', label => $label, type => 'number'});
					foreach my $row (@{ $ret->results->results->{$groupby} }){
						$self->api->log->debug('row: ' . Dumper($row));
						$datatable->add_rows([ { v => $row->{'@groupby'} }, { v => $row->{'@count'} } ]);
					}
				}
			}
		}
		else {
			die('groupby required');
		}
		$datasource->datatable($datatable);
		
		if (ref($ret) and ref($ret) eq 'HASH'){
			if ($self->api->has_warnings){
				$self->api->log->debug('warnings: ' . Dumper($self->api->warnings));
				$datasource->add_message({type => 'warning', reason => 'data_truncated', message => join(' ', @{ $self->api->warnings })});
			}
		}
		elsif (ref($ret) and blessed($ret) and $ret->can('add_warning') and $self->api->has_warnings){
			$self->api->log->debug('warnings: ' . Dumper($self->api->warnings));
			$datasource->add_message({type => 'warning', reason => 'data_truncated', message => join(' ', @{ $self->api->warnings })});
		}
		my ($headers, $body) = $datasource->serialize;
		$self->api->log->debug('headers: ' . Dumper(@$headers));
		$self->api->log->debug('body: ' . Dumper($body));
		$res->headers(@$headers);
		$res->body([encode_utf8($body)]);
	}
	$res->finalize();
}

1;