package Web;
use Moose;
use base 'Plack::Component';
use Data::Dumper;
use Plack::Request;
use Plack::Session;
use JSON -convert_blessed_universally;
use URI::Escape qw(uri_unescape);
use Encode;
use MIME::Base64;
use YUI;
use Query;

use API;

has 'mode' => (is => 'rw', isa => 'Str', required => 1, default => sub { return 'index' });
has 'session' => (is => 'rw', isa => 'Object', required => 0);
has 'api' => (is => 'rw', isa => 'Object', required => 1);
has 'title' => (is => 'rw', isa => 'Str', required => 1, default => 'ELSA');
has 'path_to_inc' => (is => 'rw', isa => 'Str', required => 1, default => '');

our %Modes = (
	index => 1,
	chart => 1,
	stats => 1,
	get_results => 1,
	admin => 1,
	transform => 2,
	send_to => 1,
);

sub call {
	my ($self, $env) = @_;
    $self->session(Plack::Session->new($env));
	my $req = Plack::Request->new($env);
	my $res = $req->new_response(200); # new Plack::Response
	$res->content_type('text/html');
	$res->header('Access-Control-Allow-Origin' => '*');
	
	$self->api->clear_warnings;
	
	my $body;
	my $method = $self->_extract_method($req->request_uri);
	$method ||= 'index';
	$self->api->log->debug('method: ' . $method);
	if (exists $Modes{ $method }){
		if ($Modes{ $method } == 1){
			my $user = $self->api->get_user($req->user);
			if ($user){
				$self->session->set('user', $user->freeze);
				$self->session->set('user_info', $user->TO_JSON);
				$body = $self->$method($req);
				if (ref($body) and ref($body) eq 'HASH'){
					if ($self->api->has_warnings){
						$body->{warnings} = $self->api->warnings;
					}
					$body = [encode_utf8($self->api->json->encode($body))];
					$self->api->log->trace('returning body: ' . Dumper($body));
				}
			}
			else {
				$res->status(401);
				$body = 'Unauthorized';
			}
		}
		elsif ($Modes{ $method } == 2){
			my $ret;
			eval {
				$ret = $self->$method($req);
				unless ($ret){
					$ret = { error => $self->api->last_error };
				}
			};
			if ($@){
				my $e = $@;
				$self->api->log->error($e);
				$body = [encode_utf8($self->api->json->encode({error => $e}))];
			}
			elsif (ref($ret) and $ret->{mime_type}){
				$res->content_type($ret->{mime_type});
				$body = $ret->{ret};
				if ($ret->{filename}){
					$res->header('Content-disposition', 'attachment; filename=' . $ret->{filename});
				}
			}
			else {
				$body = [encode_utf8($self->api->json->encode($ret))];
			}
			$body = [encode_utf8($self->api->json->encode($ret))];
		}
	}
	
	unless ($body){
		$body = { error => $self->api->last_error };
	}
	$res->body($body);
	$res->finalize();
}

sub _extract_method {
	my $self = shift;
	my $uri = shift;
	$self->api->log->debug('uri: ' . $uri);
	
	$uri =~ /\/([^\/\?]+)\??([^\/]*)$/;
	return $1;
}


sub index {
	my $self = shift;
	my $req = shift;
	
	$self->title('ELSA');
	
	return $self->_get_headers() . $self->_get_index_body();
}

sub _get_headers {
	my $self = shift;
#	my $dir = $self->api->conf->get('email/base_url');
#	$dir =~ s/^https?\:\/\/[^\/]+\//\//; # strip off the URL to make $dir the URI
#	$dir = '';
	my $dir = $self->path_to_inc;
	my $HTML = <<'EOHTML'
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
EOHTML
;

#	my $yui_css = YUI::css_link();
#	my $yui_js = YUI::js_link();
	my $yui = new YUI(%{ $self->api->conf->get('yui') });
	$HTML .= $yui->css($dir);
	$HTML .= $yui->js($dir);

	my $template = <<'EOHTML'
<style type="text/css">
/*margin and padding on body element
  can introduce errors in determining
  element position and are not recommended;
  we turn them off as a foundation for YUI
  CSS treatments. */
body {
	margin:0;
	padding:0;
}
</style>
<link rel="stylesheet" type="text/css" href=%3$s/inc/custom.css />
<script type="text/javascript" src="%3$s/inc/swfobject.js"></script>
<script type="text/javascript" src="%3$s/inc/chart.js" ></script>
<script type="text/javascript" src="%3$s/inc/utilities.js" ></script>
<script type="text/javascript" src="%3$s/inc/elsa.js" ></script>
<script type="text/javascript" src="%3$s/inc/main.js" ></script>
EOHTML
;

	if ($self->api->conf->get('custom_javascript_includes')){
		foreach my $include (@{ $self->api->conf->get('custom_javascript_includes') }){
			$template .= '<script type="text/javascript" src="' . $include . '" ></script>';
		}
	}

	$template .= '<script type="text/Javascript">';	

	if ($self->api->conf->get('javascript_debug_mode')){
		$template .= 'YAHOO.ELSA.viewMode = \'' . $self->api->conf->get('javascript_debug_mode') . '\';' . "\n";
	}
	$HTML .= sprintf($template, undef, undef, $dir);
	
	# Set the javascript var for admin here if necessary
	if ($self->session->get('user_info') and $self->session->get('user_info')->{is_admin}){
		$HTML .= 'YAHOO.ELSA.IsAdmin = true;' . "\n";
		
		# Set the URL for getPcap if applicable
		if ($self->api->conf->get('pcap_url')){
			$HTML .= 'YAHOO.ELSA.pcapUrl = "' . $self->api->conf->get('pcap_url') . '"' . "\n";
		}
		
		# Set the URL for Block if applicable
		if ($self->api->conf->get('block_url')){
			$HTML .= 'YAHOO.ELSA.blockUrl = "' . $self->api->conf->get('block_url') . '"' . "\n";
		}
	}
	
	# Check to see if we want to use the same tab for each query by default
	if ($self->api->conf->get('same_tab_for_queries_default')){
		$HTML .= 'YAHOO.ELSA.sameTabForQueries = 1;' . "\n";
	}
	
	# Check to see if we want grid results by default
	if ($self->api->conf->get('grid_view_default')){
		$HTML .= 'YAHOO.ELSA.gridDisplay = 1;' . "\n";
	}
	
	# Set form params
	my $user = $self->api->get_user($self->session->get('user_info')->{username});
	my $form_params = $self->api->get_form_params($user);
	if($form_params){
		$HTML .= 'var formParams = ' . $self->api->json->encode($form_params) . ';';
	}
	else {
		$HTML .= q/alert('Error contacting log server(s)');/;
	}
	
	$HTML .= <<'EOHTML'
YAHOO.util.Event.throwErrors = true; 
	/*
		Global object that should allow for the initial creation of the select dropdown
		If necessary, this might be devoured by 'classes' object
	*/
	
	classSelect = {
		values: {
			'0': 'New Class',
			'1': 'Test Class'
		},
		id: 'class',
		selected: ' ',
		onchange: 'loadClass(this.value);drawLabels();'
	};
	
	labelSearch = [];
	multipleInheritance = {};
	
</script>
EOHTML
;

	$HTML .= sprintf('<title>%s</title>', $self->title);
	return $HTML;

}


sub _get_index_body {
	my $HTML = <<'EOHTML'
<script>YAHOO.util.Event.addListener(window, "load", YAHOO.ELSA.main);</script>
</head>
<body class=" yui-skin-sam">
<div id="menu_bar"></div>
<div id="panel_root"></div>
<!--<h1>Enterprise Log Search and Archive</h1>-->
<div id="query_form"></div>
<div id="logs">
	<div id="tabView">
	<ul class="yui-nav">
    </ul>
	    <div class="yui-content"></div>
    </div>
</div>

</body>
</html>
EOHTML
;

	return $HTML;
}

sub get_results {
	my $self = shift;
	my $req = shift;
	
	my $HTML = $self->_get_headers('..');
	
	if ($self->session->get('user_info') and $self->session->get('user_info')->{uid}){
		my $args = $req->query_parameters->as_hashref;
		$args->{uid} = $self->session->get('user_info')->{uid};
		
		my $ret = $self->api->get_saved_result($args);
		if ($ret and ref($ret) eq 'HASH'){
			my $form_params = $self->api->get_form_params($self->api->get_user($self->session->get('user_info')->{username}));
			if($form_params){
				$HTML .= '<script>YAHOO.ELSA.formParams = ' . $self->api->json->encode($form_params) . ';</script>';
			}
			
			$HTML .= '<script>var oGivenResults = ' . $self->api->json->encode($ret) . '</script>';
			$HTML .= '<script>YAHOO.util.Event.addListener(window, "load", function(){YAHOO.ELSA.initLogger(); YAHOO.ELSA.Results.Given(oGivenResults)});</script>';
		}
		else {
			$self->api->_error('Unable to get results, got: ' . Dumper($ret));
			$HTML .= '<script>YAHOO.util.Event.addListener(window, "load", function(){YAHOO.ELSA.initLogger(); YAHOO.ELSA.Error("Unable to get results"); });</script>';
		}
	}
	else {
		$self->api->_error('Unauthorized');
		$HTML .= '<script>YAHOO.util.Event.addListener(window, "load", function(){YAHOO.ELSA.initLogger(); YAHOO.ELSA.Error("Unauthorized"); });</script>';
	}
	
	$HTML .= <<'EOHTML'
</head>
<body class=" yui-skin-sam">
<div id="panel_root"></div>
<div id="logs">
	<div id="tabView">
	<ul class="yui-nav">
    </ul>
	    <div class="yui-content">
	    </div>
    </div>
</div>
</body>
</html>
EOHTML
;

	return $HTML;
}

sub admin {
	my $self = shift;
	my $req = shift;
	
	$self->title('ELSA Permissions Management');
	
	my $args = $req->query_parameters->as_hashref;
	$args->{uid} = $self->session->get('user_info')->{uid};
	my $HTML = $self->_get_headers('..');
	
	$HTML .= <<'EOHTML'
<script type="text/javascript" src="inc/admin.js" ></script>
<script>YAHOO.util.Event.addListener(window, "load", YAHOO.ELSA.Admin.main)</script>
</head>
<body class=" yui-skin-sam">
<div id="panel_root"></div>
<div id="permissions"></div>
<div id="delete_exceptions_button_container"></div>
<div id="exceptions"></div>
<div id="logs">
	<div id="tabView">
	<ul class="yui-nav">
    </ul>
	    <div class="yui-content">
	    </div>
    </div>
</div>
</body>
</html>
EOHTML
;
	
	return $HTML;	
}

sub stats {
	my $self = shift;
	my $req = shift;
	
	$self->title('ELSA Stats');
	
	my $args = $req->query_parameters->as_hashref;
	$args->{uid} = $self->session->get('user_info')->{uid};
	my $HTML = $self->_get_headers('..');
	
	$HTML .= <<'EOHTML'
<script type="text/javascript" src="inc/stats.js" ></script>
<script>YAHOO.util.Event.addListener(window, "load", YAHOO.ELSA.Stats.main)</script>
</head>
<body class=" yui-skin-sam">
<div id="panel_root"></div>
<div id="query_stats"></div>
<div id="load_stats"></div>
<div id="logs">
	<div id="tabView">
	<ul class="yui-nav">
    </ul>
	    <div class="yui-content">
	    </div>
    </div>
</div>
</body>
</html>
EOHTML
;
	
	return $HTML;	
}

sub transform {
	my $self = shift;
	my $req = shift;
	my $args = $req->parameters->as_hashref;
	
	$self->title('ELSA Transform');
	
	if ( $args and ref($args) eq 'HASH' and $args->{data} and $args->{transforms} ) {
		eval {
			$self->api->log->trace('args: ' . Dumper($args));
			$args->{transforms} = $self->api->json->decode(uri_unescape($args->{transforms}));
			$self->api->log->trace('transforms: ' . Dumper($args->{transforms}));
			foreach my $transform (@{ $args->{transforms} }){
				die('subsearch not allowed') if $transform eq 'subsearch';
			}
			$args->{results} = $self->api->json->decode(uri_unescape(delete $args->{data}));
			$self->api->log->debug( "Decoded $args as : " . Dumper($args) );
		};
		if ($@){
			$self->api->log->error("invalid args, error: $@, args: " . Dumper($args));
			return { error => 'Unable to build results object from args' };
		}
		
		my $res = new Results(results => $args->{results}->{results});
		$self->api->log->debug('res: ' . Dumper($res));
		my $q = new Query(conf => $self->api->conf, results => $res, transforms => $args->{transforms});
		$self->api->transform($q);
		my $results = $q->results->results;
		
		$self->api->log->debug( "Got results: " . Dumper($results) );
		
		return { 
			ret => $results, 
			mime_type => 'application/javascript',
		};
	}
	else {
		$self->api->log->error('Invalid args: ' . Dumper($args));
		return { error => 'Unable to build results object from args' };
	}
}

sub send_to {
	my $self = shift;
	my $req = shift;
	my $args = $req->parameters->as_hashref;
	
	$self->title('ELSA Connector');
	
	if ( $args and ref($args) eq 'HASH' and $args->{data} ) {
		eval {
			my $json_args = $self->api->json->decode(uri_unescape(decode_base64($args->{data})));
			$args->{user} = $self->api->get_user($req->user);
			$args->{connectors} = $json_args->{connectors};
			$args->{results} = delete $json_args->{results};
			$args->{query} = delete $json_args->{query};
			$args->{qid} = delete $json_args->{qid};
			$self->api->log->debug( "Decoded $args as : " . Dumper($args) );
		};
		if ($@){
			$self->api->log->error("invalid args, error: $@, args: " . Dumper($args));
			return 'Unable to build results object from args';
		}
		
		my $results = $self->api->send_to($args);
		$results = $args->{results} unless $results;
		$self->api->log->debug( "Got results: " . Dumper($results) );
		
		return { 
			ret => $results, 
			mime_type => 'application/javascript',
		};
	}
	else {
		$self->api->log->error('Invalid args: ' . Dumper($args));
		return 'Unable to build results object from args';
	}
}

1;