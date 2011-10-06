package ELSA::Writer;
use strict;
use Data::Dumper;
use Time::HiRes;
use Sys::Info; 
use Sys::Info::Constants qw( :device_cpu );
use Sys::MemInfo qw( freemem totalmem freeswap totalswap );
use Fcntl;
use File::Temp;
use Socket;
use String::CRC32;

use ELSA;
use IO::Socket;
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
require Exporter;
our @ISA = qw(Exporter ELSA);
use ELSA::Exceptions;

our $Missing_field_tolerance = 1;
our $Add_programs_timeout = 3;
our $Default_class_id = 1;

sub new {
	my $class = shift;
	my $config_file_name = shift;
	my $id = 0; # optional id indicates that this is a forked worker Indexer using POE
	if (@_){
		$id = sprintf("%d", shift);
	}
	throw_params param => 'config_file_name', value => $config_file_name unless $config_file_name;
	my $self = $class->SUPER::new($config_file_name);
	$self->{_RUN} = 1;
	$self->{_STATS} = {
		log_hosts => {},
		overall => {
			received => 0,
			invalid => 0,
		}
	};
		
	bless ($self, $class);
	
	if ($id){
		$self->log->debug("Fork with id $id and pid $$");
		$self->{_ID} = $id;
		Log::Log4perl::NDC->remove();
		Log::Log4perl::NDC->push("WorkerID: $id");
	}
	
	$self->init_db();
	$self->init_classes();
	$self->init_cache();
	$self->{_FIELD_CONVERSIONS} = $self->get_field_conversions();
	$self->{_DEFAULT_CLASSES} = {};
	
	# Find number of CPU's
	my $info = Sys::Info->new;
	my $cpuinfo = $info->device( CPU => () );
	$self->{_NUM_CPUS} = $cpuinfo->count;
	
	$self->log->debug("Inited in pid $$");
	
	return $self;
}

sub default_classes {
	my $self = shift;
	if (@_){
		$self->{_DEFAULT_CLASSES} = shift;
	}
	else {
		return $self->{_DEFAULT_CLASSES};
	}
}

sub process_batch {
	my $self = shift;
	my $filename = shift;
	my $fh = \*STDIN;
	if ($filename){
		open($fh, $filename) or throw_e error => 'Unable to open file: ' . $!;
		$self->log->debug('Reading from file ' . $filename);
		$self->{_OFFLINE_PROCESSING} = 1;
		$self->{_OFFLINE_PROCESSING_START} = time();
		$self->{_OFFLINE_PROCESSING_END} = 0;
	}
	$fh->autoflush(1);
	
	throw_e error => "Non-existent buffer_dir: " . $self->conf->get('buffer_dir')
		unless -d $self->conf->get('buffer_dir');
		
#	$self->log->debug("Starting up with batch_id: $batch_id, first_id: $first_id");
	
	my $start_time = Time::HiRes::time();
		
	my $tempfile = File::Temp->new( DIR => $self->conf->get('buffer_dir'), UNLINK => 0 );
	$tempfile->autoflush(1);
	my $batch_counter = 0;
	my $error_counter = 0;
	
	# Reset the miss cache
	$self->{_CACHE_ADD} = {};
	
	# End the loop after table_interval seconds
	local $SIG{ALRM} = sub {
		$self->log->trace("ALARM");
		$self->{_RUN} = 0;
	};
	unless ($self->{_OFFLINE_PROCESSING}){
		alarm $self->conf->get('sphinx/index_interval');
	}
	
	while (<$fh>){	
		eval { 
			$tempfile->print(join("\t", @{ $self->_parse_line($_) }) . "\n");
			$batch_counter++;
		};
		if ($@){
			my $e = $@;
			$error_counter++;
			if ($self->conf->get('log_parse_errors')){
				ELSA::log_error($e) 
			}
		}
		last unless $self->{_RUN};
	}
		
	# Update args to be results
	my $args = {
		file => $tempfile->filename(),
		id => $self->{_ID},
		start => $self->{_OFFLINE_PROCESSING} ? $self->{_OFFLINE_PROCESSING_START} : $start_time,
		end => $self->{_OFFLINE_PROCESSING} ? $self->{_OFFLINE_PROCESSING_END} : Time::HiRes::time(),
		total_processed => $batch_counter,
		total_errors => $error_counter,
	};
	
	# Report back that we've finished
	$self->log->debug("Finished job process_batch with cache hits: $batch_counter and " . (scalar keys %{ $self->{_CACHE_ADD} }) . ' new programs');
	$self->log->debug('Total errors: ' . $error_counter . ' (%' . (($error_counter / $batch_counter) * 100) . ')' ) if $batch_counter;
		
	if (scalar keys %{ $self->{_CACHE_ADD} }){
		$self->log->trace('Adding programs: ' . Dumper($self->{_CACHE_ADD}));
		foreach my $node (keys %{ $self->get_peers() }, 'localhost'){
			my $msg = POE::Event::Message->package( $self->{_CACHE_ADD} );
			$msg->addRouteTo('asynch', 'agent', 'execute', 'manager', 'add_programs');
			$msg->addRemoteRouteTo($node, $self->conf->get('manager/listen_port'), 'asynch');
			
			eval {	
				local $SIG{ALRM} = sub { die 'alarm'; };
				alarm $Add_programs_timeout;
				$msg->route();
				alarm 0;
			};
			if ($@){
				$self->log->error('Add program timed out after ' . $Add_programs_timeout . ' seconds');
				return;
			}
		}
	}
	
	if ($batch_counter){
		my ($query, $sth);
		$query = sprintf('INSERT INTO %s.buffers (filename) VALUES (?)', $ELSA::Meta_db_name);
		$sth = $self->db->prepare($query);
		$sth->execute($args->{file});
	}
		
	# Reset the run marker
	$self->{_RUN} = 1;
	
	return $batch_counter;        
}

sub _parse_line {
	my $self = shift;
	my $raw_line = shift;
	
	chomp($raw_line);
		
	my @line = split(/\t/, $raw_line);
	
	# Fix class_id for "unknown"
    if ($line[FIELD_CLASS_ID] eq 'unknown'){
    	$line[FIELD_CLASS_ID] = $self->_get_default_class(\@line);
    }
		        
	# If we're configured to do so, we'll tolerate missing a missing field and make up a default
	if ($Missing_field_tolerance){
		my $missing_fields = 0;
		# Make sure that we've got the basics--things we don't want to fake
		unless ($line[FIELD_HOST] and $line[FIELD_MSG]){
			throw_parse 
				parse_error => "Unable to parse log line: no host or msg.  Only parsed into:\n" . Dumper(\@line), 
				text => $raw_line;
		}
		unless ($line[FIELD_TIMESTAMP]){
			$line[FIELD_TIMESTAMP] = time();
			$self->log->error('Missing required field timestamp') if $self->conf->get('log_parse_errors');
			$missing_fields++;
		}
		unless ($line[FIELD_PROGRAM]){
			$line[FIELD_PROGRAM] = 'unknown';
			$self->log->error('Missing required field program') if $self->conf->get('log_parse_errors');
			$missing_fields++;
		}
		unless ($line[FIELD_CLASS_ID]){
			$line[FIELD_CLASS_ID] = '1';
			$self->log->error('Missing required field class id') if $self->conf->get('log_parse_errors');
			$missing_fields++;
		}
		
		if ($missing_fields > $Missing_field_tolerance){
			throw_parse 
				parse_error => "Unable to parse log line: not enough fields.  Only parsed into:\n" . Dumper(\@line), 
				text => $raw_line;
		}
	}
	else {
		# No tolerance for any missing fields
		unless ($line[FIELD_TIMESTAMP] and $line[FIELD_CLASS_ID] and $line[FIELD_HOST] and
			$line[FIELD_PROGRAM] and $line[FIELD_MSG]){
			throw_parse 
				parse_error => "Unable to parse log line: no tolerance for missing fields.  Only parsed into:\n" . Dumper(\@line), 
				text => $raw_line;
		}
	}
    
    unless ($self->{_CLASSES}->{ $line[FIELD_CLASS_ID] }){
		throw_parse 
			parse_error => "Unable to parse valid class id from log line.  Only parsed into:\n" . Dumper(\@line),
			text => $raw_line;
	}
	
	# Fix weird programs that may be wrong
	if ($line[FIELD_PROGRAM] =~ /^\d+$/){
#		$self->log->debug("ALL NUMBER PROG: " . $line[FIELD_PROGRAM] . ", raw_line: $raw_line");
		$line[FIELD_PROGRAM] = 'unknown';
	}
	
	# Escape any backslashes in MSG
	$line[FIELD_MSG] =~ s/\\/\\\\/g;
	
	# Normalize program name to be all lowercase
	$line[FIELD_PROGRAM] = lc($line[FIELD_PROGRAM]);
	
	# Normalize program name to swap any weird chars with underscores
	$line[FIELD_PROGRAM] =~ s/[^a-zA-Z0-9\_\-]/\_/g;
	
	# Host gets the int version of itself
	$line[FIELD_HOST] = unpack('N*', inet_aton($line[FIELD_HOST]));
	
	if ($self->{_CACHE}->{ $line[FIELD_PROGRAM] }){
		$line[FIELD_PROGRAM] = $self->{_CACHE}->{ $line[FIELD_PROGRAM] };
	}
	else {
		my $program = $line[FIELD_PROGRAM];
		$line[FIELD_PROGRAM] = $self->_generate_program_id( $program );
		$self->{_CACHE_ADD}->{ $program } = { id => $line[FIELD_PROGRAM], class_id => $line[FIELD_CLASS_ID] };
		$self->{_CACHE}->{ $program } = $line[FIELD_PROGRAM];
	}
	
	if ($line[FIELD_CLASS_ID] ne 1){ #skip default since there aren't any fields
		# Convert any IP fields as necessary
		foreach my $field_order (keys %{ $self->{_FIELD_CONVERSIONS}->{ $line[FIELD_CLASS_ID] }->{'IPv4'} }){
			$line[$field_order] = unpack('N', inet_aton($line[$field_order]));
		}
		
		# Convert any proto fields as necessary
		foreach my $field_order (keys %{ $self->{_FIELD_CONVERSIONS}->{ $line[FIELD_CLASS_ID] }->{PROTO} }){
			$line[$field_order] = $ELSA::Proto_map->{ $line[$field_order] };
		}
	}
	
	# Update start/end times if necessary
	if ($self->{_OFFLINE_PROCESSING}){
		if ($line[FIELD_TIMESTAMP] < $self->{_OFFLINE_PROCESSING_START}){
			$self->{_OFFLINE_PROCESSING_START} = $line[FIELD_TIMESTAMP];
		}
		if ($line[FIELD_TIMESTAMP] > $self->{_OFFLINE_PROCESSING_END}){
			$self->{_OFFLINE_PROCESSING_END} = $line[FIELD_TIMESTAMP];
		}
	}
		
	# Push our auto-inc dummy val on
	unshift(@line, '0');
	
	return \@line;
}

sub _generate_program_id {
	my $self = shift;
	my $program = shift;
	my $id = crc32( $program );
	
	while (my $collision = $self->_has_collision($id)){
		# There was a collision!
		$self->log->warn('CRC id collision on program ' . $program . ' on id ' . $id 
			. ' with existing program ' . $collision);
		$program .= '_'; # Add another char, hopefully this new string won't collide with anything
		$id = crc32( $program );
	}
	
	return $id;
}

sub _has_collision {
	my $self = shift;
	my $id = shift;
	my ($query, $sth);
	$query = sprintf('SELECT id, program FROM %s.programs WHERE id=?', $ELSA::Meta_db_name);
	$sth = $self->db->prepare($query);
	$sth->execute($id);
	my $row = $sth->fetchrow_hashref;
	if ($row){
		return $row->{program};
	}
	else {
		return 0;
	}
}

sub _get_default_class {
	my $self = shift;
	my $line = shift;
	
	return $self->default_classes->{ $line->[FIELD_HOST] } ? $self->default_classes->{ $line->[FIELD_HOST] } : $Default_class_id;
}

1;

__END__
