package Web::GoogleDashboard;
use Moose;
extends 'Web';
use Data::Dumper;
use Plack::Request;
use Plack::Session;
use Encode;
use Module::Pluggable require => 1, search_path => [qw(Dashboard)];
use JSON;
use Plack::Middleware::Auth::Basic;
use Date::Manip;

sub call {
	my ($self, $env) = @_;
    $self->session(Plack::Session->new($env));
	my $req = Plack::Request->new($env);
	my $res = $req->new_response(200); # new Plack::Response
	$res->content_type('text/html');
	$res->header('Access-Control-Allow-Origin' => '*');
	$self->path_to_inc('../');
	
	my $dashboard_name = $self->_extract_method($req->request_uri);
	$self->api->log->debug('method: ' . $dashboard_name);
	
	my $config = $self->api->conf->get('dashboards/' . $dashboard_name);
	unless ($config){
		$res->status(404);
		$res->body('not found');
		return $res->finalize();
	}
	my $user = $self->api->get_user($req->user);
	if ($user){
		$self->session->set('user', $user->freeze);
		$self->session->set('user_info', $user->TO_JSON);
	}
	else {
		$res->status(401);
		die('Unauthorized');
	}
	
	my $args = $req->parameters->as_hashref;
	my $time_units = {
		seconds => { groupby => 'timestamp', multiplier => 1 },
		minutes => { groupby => 'minute', multiplier => 60 },
		hours => { groupby => 'hour', multiplier => 3600 },
		days => { groupby => 'day', multiplier => 86400 },
		months => { groupby => 'month', multiplier => 2592000 },
		years => { groupby => 'year', multiplier => 946080000 },
	};
	
	foreach my $arg (keys %$args){
		if (exists $time_units->{ $arg }){
			$args->{groupby} = $time_units->{ $arg }->{groupby};
			$args->{start_time} = (time() - ($time_units->{ $arg }->{multiplier} * int($args->{$arg})));
			$self->api->log->trace('set start_time to ' . (scalar localtime($args->{start_time})));
			last;
		}
	}
	
	if (exists $args->{start}){
		$args->{start_time} = UnixDate(ParseDate(delete $args->{start}), '%s');
		$self->api->log->trace('set start_time to ' . (scalar localtime($args->{start_time})));
	}
	if (exists $args->{end}){
		$args->{end_time} = UnixDate(ParseDate(delete $args->{end}), '%s');
		$self->api->log->trace('set end_time to ' . (scalar localtime($args->{end_time})));
	}
	else {
		$args->{end_time} = time;
	}
	
	foreach my $key (keys %$config){
		$args->{$key} = $config->{$key};
	}
	
	$args->{api} = $self->api;
	if ($config->{auth} eq 'none'){
		$args->{user} = $self->api->get_user('system');
	}
	else {
		$args->{user} = $self->api->get_user($req->user);
	}
	
	my $dashboard;
	eval {
		$self->api->freshen_db;
	
		$self->plugins();
		#$self->api->log->trace('creating dashboard from args: ' . Dumper($args));
		my $start_time = time();
		$dashboard = $config->{package}->new($args);
		$self->api->log->trace('created dashboard in ' . (time() - $start_time) . ' seconds');
		
		unless ($dashboard->queries){
			die('no queries: ' . $self->api->last_error);
		}
	};
	if ($@){
		my $e = $@;
		$self->api->log->error($e);
		$res->body([encode_utf8($self->api->json->encode({error => $e}))]);
	}
	else {
		$self->api->log->debug('queries: ' . Dumper($dashboard->queries));
		$res->body([$self->index($req, $dashboard->queries)]);
	}
		
	$res->finalize();
}

sub index {
	my $self = shift;
	my $req = shift;
	my $queries = shift;
	return $self->_get_headers() . $self->_get_index_body($queries);
}

sub _get_index_body {
	my $self = shift;
	my $queries = shift;
	
	
	foreach my $query (@$queries){
		delete $query->{user};
	}	
	my $json = $self->api->json->encode($queries);
	my $dir = $self->path_to_inc;

	my $HTML =<<"EOHTML"
<!--Load the AJAX API-->
<script type="text/javascript" src="https://www.google.com/jsapi"></script>
<script type="text/javascript" src="$dir/inc/dashboard.js" ></script>
<script>
YAHOO.ELSA.viewMode = 'dev';
YAHOO.ELSA.dashboardGroups = {};
YAHOO.ELSA.charts = {};
YAHOO.ELSA.dashboardRows = $json;

// Load the Visualization API and the piechart package.
google.load('visualization', '1.0', {'packages':['corechart', 'charteditor', 'controls']});

YAHOO.util.Event.addListener(window, "load", function(){
	YAHOO.ELSA.initLogger();
	// Set a callback to run when the Google Visualization API is loaded.
	//google.setOnLoadCallback(loadCharts);
	loadCharts();
	
});
</script>
</head>

  <body>
    <div id="google_charts"></div>
  </body>
</html>
EOHTML
;
	return $HTML;
}

1;