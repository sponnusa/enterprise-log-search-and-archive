package Transform::Grep;
use Moose;
use Data::Dumper;
use CHI;
use AnyEvent::HTTP;
use Socket;
use JSON;
extends 'Transform';
our $Name = 'Grep';
has 'name' => (is => 'ro', isa => 'Str', required => 1, default => $Name);
has 'field' => (is => 'ro', isa => 'Ref', required => 1, default => sub { qr/./ });
has 'regex' => (is => 'ro', isa => 'Ref', required => 1, default => sub { qr/./ });

sub BUILDARGS {
	my $class = shift;
	##my $params = $class->SUPER::BUILDARGS(@_);
	my %params = @_;
	$params{field} = qr/$params{args}->[0]/ if defined $params{args}->[0];
	$params{regex} = qr/$params{args}->[1]/ if defined $params{args}->[1];
	return \%params;
}

sub BUILD {
	my $self = shift;
	$self->log->debug('regex: ' . Dumper($self->regex));
	
	DATUM_LOOP: foreach my $datum (@{ $self->data }){
		foreach my $transform (keys %{ $datum->{transforms} }){
			next unless ref($datum->{transforms}->{$transform}) eq 'HASH';
			foreach my $transform_field (keys %{ $datum->{transforms}->{$transform} }){
				next unless ref($datum->{transforms}->{$transform}->{$transform_field}) eq 'HASH';
				foreach my $key (keys %{ $datum->{transforms}->{$transform}->{$transform_field} }){
					next unless $key =~ $self->field;
					if ($datum->{transforms}->{$transform}->{$transform_field}->{$key} =~ $self->regex){
						$datum->{transforms}->{$Name} = '__KEEP__';
						next DATUM_LOOP;
					}
				}
			}
		}
	}
	foreach my $datum (@{ $self->data }){
		unless (exists $datum->{transforms}->{$Name}){
			$datum->{transforms}->{'__DELETE__'} = 1;
		}
		delete $datum->{transforms}->{$Name};
	}
	
	$self->log->debug('data: ' . Dumper($self->data));
	
	return $self;
}

 
1;