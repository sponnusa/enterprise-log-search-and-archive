#!/usr/bin/perl
use strict;
use Data::Dumper;
use Plack::Builder;
use Plack::App::File;
use FindBin;
use lib $FindBin::Bin;

use API;
use Web;
use Web::Query;

my $config_file = '/usr/local/elsa/etc/elsa.conf';
if ($ENV{ELSA_CONF}){
	$config_file = $ENV{ELSA_CONF};
}

my $api = API->new(config_file => $config_file) or die('Unable to start from given config file.');

my $auth;
if (lc($api->conf->get('auth/method')) eq 'ldap' and $api->conf->get('ldap')){
	require Authen::Simple::LDAP;
	$auth = Authen::Simple::LDAP->new(
		host        => $api->conf->get('ldap/host'),
		binddn      => $api->conf->get('ldap/bindDN'),
		bindpw      => $api->conf->get('ldap/bindpw'),
		basedn        => $api->conf->get('ldap/base'),
		filter => '(&(objectClass=organizationalPerson)(objectClass=user)(sAMAccountName=%s))',
		log => $api->log,
	);
}
elsif ($api->conf->get('auth/method') eq 'local'){
	require Authen::Simple::PAM;
	$auth = Authen::Simple::PAM->new(
		log => $api->log,
	);
}
elsif ($api->conf->get('auth/method') eq 'none'){
	# Inline a null authenticator
	package Authen::Simple::Null;
	use base qw(Authen::Simple::Adapter);
	Authen::Simple::Null->_options({log => {
		type     => Params::Validate::OBJECT,
		can      => [ qw[debug info error warn] ],
		default  => Authen::Simple::Log->new,
		optional => 1}});
	sub check { my $self = shift; $self->log->debug('Authenticating: ', join(', ', @_)); return 1; }
	package main;
	$auth = Authen::Simple::Null->new(log => $api->log);
}
elsif ($api->conf->get('auth/method') eq 'db'){
	require Authen::Simple::DBI;
	$auth = Authen::Simple::DBI->new(
		dsn => $api->conf->get('auth_db/dsn') ? $api->conf->get('auth_db/dsn') : $api->conf->get('meta_db/dsn'),
		username =>	$api->conf->get('auth_db/username') ? $api->conf->get('auth_db/username') : $api->conf->get('meta_db/username'),
		password => $api->conf->get('auth_db/password') ? $api->conf->get('auth_db/password') : $api->conf->get('meta_db/password'),
		log => $api->log,
		statement => $api->conf->get('auth_db/auth_statement') ? $api->conf->get('auth_db/auth_statement') : 'SELECT PASSWORD(password) FROM users WHERE username=?',
	);
}
else {
	die('No auth method, please configure one!');
}
warn 'serving from ' . $FindBin::Bin . '../';

builder {
	$ENV{PATH_INFO} = $ENV{REQUEST_URI}; #mod_rewrite will mangle PATH_INFO, so we'll set this manually here in case it's being used
	#enable 'ForwardedHeaders';
	enable 'Static', path => qr{^/?inc/}, root => $FindBin::Bin . '/../';
	enable 'CrossOrigin', origins => '*', methods => '*', headers => '*';
	enable 'Session', store => 'File';
	unless ($api->conf->get('auth/method') eq 'none'){
		enable 'Auth::Basic', authenticator => $auth, realm => $api->conf->get('auth/realm');
	}
	
	mount '/favicon.ico' => sub { return [ 200, [ 'Content-Type' => 'text/plain' ], [ '' ] ]; };
	mount '/Query' => Web::Query->new(api => $api)->to_app;
	mount '/' => Web->new(api => $api)->to_app;
};


