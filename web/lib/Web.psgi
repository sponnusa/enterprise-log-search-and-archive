#!/usr/bin/perl
use strict;
use Data::Dumper;
use Log::Log4perl;
use Config::JSON;
use JSON -convert_blessed_universally;
use Plack::Builder;
use DBI;
use FindBin;
use lib $FindBin::Bin;

use API;
use Web;
use Web::Query;

my $config_file = '/usr/local/elsa/etc/elsa.conf';
if ($ENV{ELSA_CONF}){
	$config_file = $ENV{ELSA_CONF};
}

my $conf = new Config::JSON ( $config_file ) or die("Unable to open config file");
my $log_level = 'DEBUG';
if ($conf->get('debug_level')){
	$log_level = $conf->get('debug_level');
}
my $logdir = $conf->get('logdir');
	my $log_conf = qq(
		log4perl.category.Web       = $log_level, File
		log4perl.appender.File			 = Log::Log4perl::Appender::File
		log4perl.appender.File.filename  = $logdir/web.log 
		log4perl.appender.File.layout = Log::Log4perl::Layout::PatternLayout
		log4perl.appender.File.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %x%n%m%n
		log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
		log4perl.appender.Screen.stderr  = 1
		log4perl.appender.Screen.layout = Log::Log4perl::Layout::PatternLayout
		log4perl.appender.Screen.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %x%n%m%n
		log4perl.appender.Syncer            = Log::Log4perl::Appender::Synchronized
		log4perl.appender.Syncer.appender   = File
	);
	
Log::Log4perl::init( \$log_conf ) or die("Unable to init logger\n");
my $logger = Log::Log4perl::get_logger('Web')
  or die("Unable to init logger\n");
my $json;
if ($log_level eq 'DEBUG' or $log_level eq 'TRACE'){
	$json = JSON->new->pretty->allow_nonref->allow_blessed;	
}
else {
	$json = JSON->new->allow_nonref->allow_blessed;
}

my $dbh = DBI->connect(
	$conf->get('meta_db/dsn'),
	$conf->get('meta_db/username'),
	$conf->get('meta_db/password'),
	{ 
		RaiseError => 1,
		HandleError => \&_dbh_error_handler,
		mysql_auto_reconnect => 1, # we will auto-reconnect on disconnect
	}
) or die($DBI::errstr);

my $auth;
if ($conf->get('auth/method') eq 'LDAP' and $conf->get('ldap')){
	require Authen::Simple::LDAP;
	$auth = Authen::Simple::LDAP->new(
		host        => $conf->get('ldap/host'),
		binddn      => $conf->get('ldap/bindDN'),
		bindpw      => $conf->get('ldap/bindpw'),
		basedn        => $conf->get('ldap/base'),
		filter => '(&(objectClass=organizationalPerson)(objectClass=user)(sAMAccountName=%s))',
		log => $logger,
	);
}
elsif ($conf->get('auth/method') eq 'local'){
	require Authen::Simple::PAM;
	$auth = Authen::Simple::PAM->new(
		log => $logger,
	);
}
elsif ($conf->get('auth/method') eq 'none'){
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
	$auth = Authen::Simple::Null->new(log => $logger);
}
else {
	die('No auth method, please configure one!');
}

my $api = API->new(conf => $conf, log => $logger, json => $json, db => $dbh);

builder {
	$ENV{PATH_INFO} = $ENV{REQUEST_URI}; #mod_rewrite will mangle PATH_INFO, so we'll set this manually here in case it's being used
	#enable 'ForwardedHeaders';
	enable 'Static', path => qr{^/?inc/}, root => $FindBin::Bin . '/../';
	enable 'CrossOrigin', origins => '*', methods => '*', headers => '*';
	enable 'Session', store => 'File';
	unless ($conf->get('auth/method') eq 'none'){
		enable 'Auth::Basic', authenticator => $auth, realm => $conf->get('auth/realm');
	}
	
	mount '/favicon.ico' => sub { return [ 200, [ 'Content-Type' => 'text/plain' ], [ '' ] ]; };
	mount '/Query' => Web::Query->new(conf => $conf, log => $logger, json => $json, api => $api)->to_app; 
	mount '/' => Web->new(conf => $conf, log => $logger, json => $json, api => $api)->to_app;
};

sub _dbh_error_handler {
	my $errstr = shift;
	my $dbh    = shift;
	my $query  = $dbh->{Statement};

	$errstr .= " QUERY: $query";
	Log::Log4perl::get_logger(__PACKAGE__)->error($errstr);
	foreach my $sth (grep { defined } @{$dbh->{ChildHandles}}){
		$sth->rollback; # in case there was an active transaction
	}
	
	return 0;
}
