package Transform;
use Moose;
use Log::Log4perl;

# Base class for Transform plugins
has 'conf' => (is => 'rw', isa => 'Object', required => 1);
has 'log' => (is => 'rw', isa => 'Object', required => 1);
has 'data' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { {} });
# A transform may be a "meta" tranform which refers to other transforms
has 'transforms' => (is => 'rw', isa => 'ArrayRef', required => 1, default => sub { [] });
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => '');

sub BUILDARGS {
	my $class = shift;
	my %params = @_;
	
	unless ($params{log}){
		my $logfile = $params{conf}->get('log/file');
		my $debug_level = $params{conf}->get('log/level');
		my $l4pconf = qq(
			log4perl.category.App       = $debug_level, Screen
			log4perl.appender.File			 = Log::Log4perl::Appender::File
			log4perl.appender.File.filename  = $logfile
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
		$params{log} =  Log::Log4perl::get_logger("App") or die("Unable to init logger\n");
	}
	
	return \%params;
} 
1;