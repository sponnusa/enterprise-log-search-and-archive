package Request::Dispatch;
use strict;
use warnings;
use Data::Dumper;
use base qw( CGI::Application::Dispatch);

sub dispatch_args {
	return {
		debug => 1,
		prefix => 'Request',
		table => [
			':app/:method' => {},
			':rm' => { app => 'Index' },
			'' => { app => 'Index', rm => 'index' },
			
		],
	};
}

sub dispatch_path {
	# For some reason, CGI::App::Dispatch mangles the PATH_INFO variable, so we reset it
	my @ar = split(/\?/, $ENV{REQUEST_URI});
	return $ar[0];
}


1;