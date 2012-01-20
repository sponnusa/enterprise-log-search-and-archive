package Transform::Filter;
use Moose;
use Data::Dumper;
use CHI;
use AnyEvent::HTTP;
use Socket;
use JSON;
extends 'Transform';
our $Name = 'Filter';
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
	
	DATUM_LOOP: foreach my $datum (@{ $self->data }){
		foreach my $transform (keys %{ $datum->{transforms} }){
			next unless ref($datum->{transforms}->{$transform}) eq 'HASH';
			foreach my $transform_field (keys %{ $datum->{transforms}->{$transform} }){
				foreach my $key (keys %{ $datum->{transforms}->{$transform}->{$transform_field} }){
					next unless $key =~ $self->field;
					if ($datum->{transforms}->{$transform}->{$transform_field}->{$key} =~ $self->regex){
						$datum->{transforms}->{'__DELETE__'} = 1;
						next DATUM_LOOP;
					}
				}
			}
		}
	}
	
	$self->log->debug('data: ' . Dumper($self->data));
	
	return $self;
}

 
1;