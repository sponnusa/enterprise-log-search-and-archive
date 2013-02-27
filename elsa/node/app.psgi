#!/usr/bin/perl

# This file is used to receive import files sent by log forwarders via HTTP.  It will
# receive the upload and process it as if the file were generated locally, including
# running all plugins.

use strict;
use Data::Dumper;
use Plack::Builder;
use Plack::Request;
use Plack::App::File;
use Plack::Builder::Conditionals;
use FindBin;
use lib $FindBin::Bin;
use Getopt::Std;
use Log::Log4perl;

use Indexer;

my %Opts;
getopts('c:', \%Opts);

my $Conf_file = $Opts{c} ? $Opts{c} : '/etc/elsa_node.conf';
my $Config_json = Config::JSON->new( $Conf_file );
my $Conf = $Config_json->{config}; # native hash is 10x faster than using Config::JSON->get()

# Setup logger
my $logdir = $Conf->{logdir};
my $debug_level = $Conf->{debug_level};
my $l4pconf = qq(
	log4perl.category.ELSA       = $debug_level, File
	log4perl.appender.File			 = Log::Log4perl::Appender::File
	log4perl.appender.File.filename  = $logdir/node.log
	log4perl.appender.File.syswrite = 1
	log4perl.appender.File.recreate = 1
	log4perl.appender.File.layout = Log::Log4perl::Layout::PatternLayout
	log4perl.appender.File.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %m%n
	log4perl.filter.ScreenLevel               = Log::Log4perl::Filter::LevelRange
	log4perl.filter.ScreenLevel.LevelMin  = $debug_level
	log4perl.filter.ScreenLevel.LevelMax  = ERROR
	log4perl.filter.ScreenLevel.AcceptOnMatch = true
	log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
	log4perl.appender.Screen.Filter = ScreenLevel 
	log4perl.appender.Screen.stderr  = 1
	log4perl.appender.Screen.layout = Log::Log4perl::Layout::PatternLayout
	log4perl.appender.Screen.layout.ConversionPattern = * %p [%d] %F (%L) %M %P %m%n
);
Log::Log4perl::init( \$l4pconf ) or die("Unable to init logger\n");
my $Log = Log::Log4perl::get_logger("ELSA") or die("Unable to init logger\n");

my $Dbh = DBI->connect(($Conf->{database}->{dsn} or 'dbi:mysql:database=syslog;'), 
	$Conf->{database}->{username}, 
	$Conf->{database}->{password}, 
	{
		InactiveDestroy => 1, 
		PrintError => 0,
		mysql_auto_reconnect => 1, 
		HandleError => \&_sql_error_handler,
		mysql_local_infile => 1,
	}) 
	or die 'connection failed ' . $! . ' ' . $DBI::errstr;

sub _sql_error_handler {
	my $errstr = shift;
	my $dbh = shift;
	my $query = $dbh->{Statement};
	my $full_errstr = 'SQL_ERROR: ' . $errstr . ', query: ' . $query; 
	$Log->error($full_errstr);
	#return 1; # Stops RaiseError
	die($full_errstr);
}

my $static_root = $FindBin::Bin . '/../';
if (exists $ENV{DOCUMENT_ROOT}){
	$static_root = $ENV{DOCUMENT_ROOT} . '/../';
}

builder {
	$ENV{PATH_INFO} = $ENV{REQUEST_URI}; #mod_rewrite will mangle PATH_INFO, so we'll set this manually here in case it's being used
	#enable 'ForwardedHeaders';
	enable 'NoMultipleSlashes';
	enable 'CrossOrigin', origins => '*', methods => '*', headers => '*';
	
	mount '/favicon.ico' => sub { return [ 200, [ 'Content-Type' => 'text/plain' ], [ '' ] ]; };
	mount '/' => sub {
		my $env = shift;
		my $req = Plack::Request->new($env);
		my $uploaded_file = $req->uploads->{filename};
		# Hard link the file so that when the HTTP server deletes it, we still have it in the child process
		my $new_file_name = $Conf->{buffer_dir} . '/' . $req->address . '_' . $uploaded_file->basename;
		link($uploaded_file->path, $new_file_name) or (
			$Log->error('Unable to link ' . $uploaded_file->path . ' to ' . $new_file_name . ': ' . $!
			and return [ 500, [ 'Content-Type' => 'text/plain' ], [ 'error' ] ])
		);
		$Log->info('Received file ' . $uploaded_file->basename . ' with size ' . $uploaded_file->size . ' from client ' . $req->address);
		
		my ($query, $sth);
		if ($uploaded_file->basename =~ /programs/){
			$Log->info('Loading programs file ' . $new_file_name);
			$query = 'LOAD DATA LOCAL INFILE "' . $new_file_name . '" INTO TABLE programs';
			$Dbh->do($query);
			if ($Dbh->rows){
				return [ 200, [ 'Content-Type' => 'text/plain' ], [ 'ok' ] ];
			}
			else {
				return [ 500, [ 'Content-Type' => 'text/plain' ], [ 'error' ] ];
			}
		}
		
		# Record our received file in the database
		$query = 'INSERT INTO buffers (filename) VALUES (?)';
		$sth = $Dbh->prepare($query);
		$sth->execute($new_file_name);
		my $rows = $sth->rows;
		$sth->finish;
		
		# Fork and process
		my $pid = fork();
		if ($pid){
			if ($rows){
				return [ 200, [ 'Content-Type' => 'text/plain' ], [ 'ok' ] ];
			}
			else {
				return [ 500, [ 'Content-Type' => 'text/plain' ], [ 'error' ] ];
			}
		}
		else {
			# Child
			my $indexer = new Indexer(log => $Log, conf => $Config_json);
			$indexer->load_buffers();
			exit;
		}
	};
};

