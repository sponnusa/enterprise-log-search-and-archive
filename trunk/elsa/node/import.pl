#!/usr/bin/perl
use strict;
use Data::Dumper;
use Getopt::Std;
use DateTime;
use DateTime::Format::Strptime;
use Reader;
use File::Temp;
use Config::JSON;
use String::CRC32;
use Time::HiRes qw(time);
use IO::File;
use POSIX qw(strftime);

my %Opts;
my $Infile_name = pop(@ARGV);
die('No infile given ' . usage()) unless -f $Infile_name;
getopts('c:t:f:s:', \%Opts);
print "Working on $Infile_name\n";
my $Infile = new IO::File($Infile_name);
my $Lines_to_skip = defined $Opts{s} ? int($Opts{s}) : 0;
my $Format = defined $Opts{f} ? $Opts{f} : 'local_syslog';
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

my $Timezone = $Opts{t} ? $Opts{t} : DateTime::TimeZone->new( name => "local")->name;
my $parser = DateTime::Format::Strptime->new(pattern => '%m/%d/%Y%t%T', time_zone => $Timezone);
my $printer = DateTime::Format::Strptime->new(pattern => '%s', time_zone => $Timezone);

my $start = time();
my $Outfile = new IO::File('> /data/elsa/tmp/import') or die('Cannot open /data/elsa/tmp/import');

my $lines_imported = 0;
if ($Format eq 'local_syslog'){
	$lines_imported = _read_local_syslog();
}
elsif ($Format eq 'bro'){
	$lines_imported = _read_bro();
}
else {
	die('Invalid input type ' . $Format);
}

my $end_time = time() - $start;
print "Sent $lines_imported lines to ELSA in $end_time seconds\n";

# Local flat-file syslog is fine as-is
sub _read_local_syslog {
	my $counter = 0;
	while (<$Infile>){
		if ($. <= $Lines_to_skip){
			next;
		}
		$Outfile->print($_);
		$counter++;
	}
	return $counter;
}

# Read from a Bro file
sub _read_bro {
	my $counter = 0;
	$Infile_name =~ /([^\.]+)\./;
	my $type = $1;
	while (<$Infile>){
		eval {
			chomp;
			next if $_ =~ /^#/;
			my @fields = split(/\t/, $_);
			my $second = $fields[0];
			($second) = split(/\./, $second, 1);
			my $date = strftime('%b %d %H:%M:%s', localtime($second));
			$Outfile->print($date . " bro_$type: " . join('|', @fields) . "\n");
			$counter++;
		};
		if ($@){
			$Log->error($@ . "\nLine: " . $_);
		}
	}
	return $counter;
}

sub usage {
	print 'Usage: import.pl [ -c <conf file> ] [ -f <format> ] [ -s <lines to skip> ] <input file>' . "\n";
}