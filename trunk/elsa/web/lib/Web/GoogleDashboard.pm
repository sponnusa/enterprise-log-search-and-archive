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

our $Default_width = 1000;

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
	
	my $user = $self->api->get_user($req->user);
	if ($user){
		$self->session->set('user', $user->freeze);
		$self->session->set('user_info', $user->TO_JSON);
	}
	
	my $args = $req->parameters->as_hashref;
	$args->{alias} = $dashboard_name;
	
	if ($req->request_uri =~ /[\?\&]edit[=]?/){
		$args->{edit} = 1;
		$self->api->log->trace('edit mode');
	}
	
	#$self->api->log->debug('dashboard args: ' . Dumper($args));
	if (exists $args->{start}){
		$args->{start_time} = UnixDate(ParseDate($args->{start}), '%s');
		$self->api->log->trace('set start_time to ' . (scalar localtime($args->{start_time})));
	}
	else {
		$args->{start_time} = (time() - (86400*7));
	}
	if (exists $args->{end}){
		$args->{end_time} = UnixDate(ParseDate(delete $args->{end}), '%s');
		$self->api->log->trace('set end_time to ' . (scalar localtime($args->{end_time})));
	}
	else {
		$args->{end_time} = time;
	}
	
	my $time_units = {
		seconds => { groupby => 'timestamp', multiplier => 1 },
		minutes => { groupby => 'minute', multiplier => 60 },
		hours => { groupby => 'hour', multiplier => 3600 },
		days => { groupby => 'day', multiplier => 86400 },
		months => { groupby => 'month', multiplier => 2592000 },
		years => { groupby => 'year', multiplier => 946080000 },
	};
	
	$args->{groupby} = 'hour';
	
	foreach my $arg (keys %$args){
		if (exists $time_units->{ $arg }){
			$args->{groupby} = $time_units->{ $arg }->{groupby};
			if ($args->{$arg}){
				if ($args->{start}){
					$args->{end_time} = ($args->{start_time} + ($time_units->{ $arg }->{multiplier} * int($args->{$arg})));
					$self->api->log->trace('set end_time to ' . (scalar localtime($args->{end_time})));
				}
				else {
					$args->{start_time} = ($args->{end_time} - ($time_units->{ $arg }->{multiplier} * int($args->{$arg})));
					$self->api->log->trace('set start_time to ' . (scalar localtime($args->{start_time})));
				}
			}
			last;
		}
	}
	foreach my $plural_unit (keys %$time_units){
		if ($time_units->{$plural_unit}->{groupby} eq $args->{groupby}){
			$args->{limit} = ($args->{end_time} - $args->{start_time}) / $time_units->{$plural_unit}->{multiplier};
		}
	}
	
	my $ret = [];
	eval {
		$self->api->freshen_db;
		my ($query, $sth);
		
		$query = 'SELECT dashboard_id, dashboard_title, alias, auth_required FROM v_dashboards WHERE alias=? ORDER BY x,y';
		$sth = $self->api->db->prepare($query);
		$sth->execute($dashboard_name);
		my $row = $sth->fetchrow_hashref;
		die('dashboard ' . $dashboard_name . ' not found or not authorized') unless $row;
		$args->{id} = $row->{dashboard_id};
		$args->{title} = $row->{dashboard_title};
		$self->title($args->{title});
		$args->{alias} = $row->{alias};
		$args->{auth_required} = $row->{auth_required};
		if ($self->api->conf->get('dashboard_width')){
			$args->{width} = $self->api->conf->get('dashboard_width');
		}
		else {
			$args->{width} = $Default_width;
		}
		
		$args->{user} = $user;
		$args->{dashboard_name} = $dashboard_name;
		unless ($self->api->_is_permitted($args)){
			$res->status(401);
			die('Unauthorized');
		}
		
		$ret = $self->api->_get_rows($args);
		delete $args->{user};
	};
	if ($@){
		my $e = $@;
		$self->api->log->error($e);
		$res->body([encode_utf8($self->api->json->encode({error => $e}))]);
	}
	else {
		$self->api->log->debug('data: ' . Dumper($ret));
		$res->body([$self->index($req, $args, $ret)]);
	}
		
	$res->finalize();
}

sub index {
	my $self = shift;
	my $req = shift;
	my $args = shift;
	my $queries = shift;
	return $self->_get_headers() . $self->_get_index_body($args, $queries);
}

sub _get_index_body {
	my $self = shift;
	my $args = shift;
	my $queries = shift;
		
	my $edit = '';
	if ($args->{edit}){
		$edit = 'YAHOO.ELSA.editCharts = true;';
	}
		
	my $json = $self->api->json->encode($queries);
	my $dir = $self->path_to_inc;
	my $defaults = $self->api->json->encode({
		groupby => [$args->{groupby}],
		start => $args->{start_time},
		end => $args->{end_time}
	});
		
	my $yui = new YUI(%{ $self->api->conf->get('yui') });
	my $yui_css = $yui->css($dir);
	my $yui_js = $yui->js($dir);

	my $HTML =<<"EOHTML"
<!--Load the AJAX API-->
<script type="text/javascript" src="https://www.google.com/jsapi"></script>
<script type="text/javascript" src="$dir/inc/elsa.js" ></script>
<script type="text/javascript" src="$dir/inc/dashboard.js" ></script>
$yui_css
$yui_js
<link rel="stylesheet" type="text/css" href="$dir/inc/custom.css" />
<script>
$edit
//YAHOO.ELSA.viewMode = 'dev';
YAHOO.ELSA.queryMetaParamsDefaults = $defaults;
YAHOO.ELSA.dashboardParams = {
	id: $args->{id},
	title: '$args->{title}',
	alias: '$args->{alias}',
	container: 'google_charts',
	rows: $json,
	width: $args->{width}
};
			 
// Load the Visualization API and the piechart package.
google.load('visualization', '1.0', {'packages':['corechart', 'charteditor', 'controls']});

YAHOO.util.Event.addListener(window, "load", function(){
	YAHOO.ELSA.initLogger();
	// Set a callback to run when the Google Visualization API is loaded.
	//google.setOnLoadCallback(loadCharts);
	//YAHOO.ELSA.Chart.loadCharts();
	oDashboard = new YAHOO.ELSA.Dashboard($args->{id}, '$args->{title}', '$args->{alias}', $json, 'google_charts');
	
});
</script>
</head>

  <body class=" yui-skin-sam">
   <div id="panel_root"></div>
    <div id="google_charts"></div>
  </body>
</html>
EOHTML
;
	return $HTML;
}

1;