package Request::Index;
use strict;
use warnings;
use Data::Dumper;
use base qw( CGI::Application::Dispatch CGI::Application Request);
use JSON -convert_blessed_universally;
use Apache2::Request;
use CGI::Application::Plugin::Apache;
use Apache2::Const qw(:http);
use APR::Request::Param;

use CGI::Application::Plugin::Session;
use CGI::Application::Plugin::Apache2::Request;
use CGI::Session::Driver::file;
use CGI qw(header);

use POE::Event::Message;
use POE::Filter::Reference;
use Data::Serializer;
BEGIN {
	$POE::Event::Message::Filter = new POE::Filter::Reference( 
		Data::Serializer->new(
			serializer => 'YAML::Syck',
			portable => 1,
		)
	);
}


use lib qw(../);
use Janus;
use YUI;

our $Json;
our $Message_class = 'POE::Event::Message';
our $Filter = new POE::Filter::Reference( 
		Data::Serializer->new(
			serializer => 'YAML::Syck',
			portable => 1,
		)
	);
my $Last_error = '';

sub setup {
	my $self = shift;
	$self->run_modes([qw(index get_results admin chart stats)]);
}

sub index {
	my $self = shift;
	my $r = $self->init_request();
	return $self->_get_headers() . $self->_get_index_body();
}

sub _get_headers {
	my $self = shift;
	my $dir = $self->conf->get('email/base_url');
	$dir =~ s/^https?\:\/\/[^\/]+\//\//; # strip off the URL to make $dir the URI
	$dir = '';
	my $HTML = <<'EOHTML'
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
EOHTML
;

	my $yui = new YUI(%{ $self->conf->get('yui') });
	$HTML .= $yui->css();
	$HTML .= $yui->js();

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
<script type="text/Javascript">
EOHTML
;

	if ($self->conf->get('javascript_debug_mode')){
		$template .= 'YAHOO.ELSA.viewMode = \'' . $self->conf->get('javascript_debug_mode') . '\';' . "\n";
	}
	$HTML .= sprintf($template, undef, undef, $dir);
	
	# Set the javascript var for admin here if necessary
	if ($self->session->param('user_info') and $self->session->param('user_info')->{is_admin}){
		$HTML .= 'YAHOO.ELSA.IsAdmin = true;' . "\n";
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
<title>ELSA</title>
EOHTML
;
	return $HTML;

}

sub _get_index_body {
	my $HTML = <<'EOHTML'
<script>YAHOO.util.Event.addListener(window, "load", YAHOO.ELSA.main);</script>
</head>
<body class=" yui-skin-sam">
<div id="menu_bar"></div>
<div id="panel_root"></div>
<h1>Enterprise Log Search and Archive</h1>
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
	my $r = $self->init_request();
	
	my $args = $self->{_QUERY_PARAMS};
	$args->{uid} = $self->session->param('user_info')->{uid};
	my $HTML = $self->_get_headers('..');
	
	my $ret = $self->rpc('get_saved_result', $args);
	if ($ret and ref($ret) eq 'HASH'){
		 $HTML .= '<script>var oGivenResults = ' . $self->json->encode($ret) . '</script>';
	}
	else {
		$self->log->error('Unable to get results, got: ' . Dumper($ret));
		$HTML .= '<script>YAHOO.ELSA.Error("Unable to get results");</script>';
	}
	
	$HTML .= <<'EOHTML'
<script>YAHOO.util.Event.addListener(window, "load", function(){YAHOO.ELSA.initLogger(); YAHOO.ELSA.Results.Given(oGivenResults)});</script>
</head>
<body class=" yui-skin-sam">
<div id="notification_area"></div>
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
	my $r = $self->init_request();
	
	unless ($self->session->param('user_info') and $self->session->param('user_info')->{is_admin}){
		my $errstr = 'Insufficient privileges';
		$self->log->error($errstr);
		$self->{_ERROR} = $errstr;
		$self->query->header(-status => HTTP_UNAUTHORIZED);
		return 'Unauthorized';
	}
	
	my $args = $self->{_QUERY_PARAMS};
	$args->{uid} = $self->session->param('user_info')->{uid};
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

sub chart {
	my $self = shift;
	my $r = $self->init_request();
	
	my $args = $self->{_QUERY_PARAMS};
	$args->{uid} = $self->session->param('user_info')->{uid};
	my $HTML = $self->_get_headers('..');
	
	$HTML .= <<'EOHTML'
<script type="text/javascript" src="inc/json2.js"></script>
<script type="text/javascript" src="inc/swfobject.js"></script>
<script type="text/javascript" src="inc/chart.js" ></script>
<script type="text/javascript" src="inc/graphAnything.js" ></script>
<script>YAHOO.util.Event.addListener(window, "load", graphAnything)</script>
</head>
<body class=" yui-skin-sam">
<div id="panel_root"></div>
<div id="form"></div>
<div id="chart"></div>
</body>
</html>
EOHTML
;
	
	return $HTML;	
}

sub stats {
	my $self = shift;
	my $r = $self->init_request();
	
	unless ($self->session->param('user_info') and $self->session->param('user_info')->{is_admin}){
		my $errstr = 'Insufficient privileges';
		$self->log->error($errstr);
		$self->{_ERROR} = $errstr;
		$self->query->header(-status => HTTP_UNAUTHORIZED);
		return 'Unauthorized';
	}
	
	my $args = $self->{_QUERY_PARAMS};
	$args->{uid} = $self->session->param('user_info')->{uid};
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

1;

__END__

