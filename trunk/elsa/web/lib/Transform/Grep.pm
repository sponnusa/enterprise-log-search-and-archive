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
	$params{field} = qr/$params{args}->[0]/i if defined $params{args}->[0];
	$params{regex} = qr/$params{args}->[1]/i if defined $params{args}->[1];
	return \%params;
}

sub BUILD {
	my $self = shift;
	$self->log->debug('field: ' . Dumper($self->field));
	$self->log->debug('regex: ' . Dumper($self->regex));
	$self->log->debug('begin with data: ' . Dumper($self->data));
	
	DATUM_LOOP: foreach my $datum (@{ $self->data }){
		foreach my $transform (keys %{ $datum->{transforms} }){
			next unless ref($datum->{transforms}->{$transform}) eq 'HASH';
			foreach my $transform_field (keys %{ $datum->{transforms}->{$transform} }){
				if (ref($datum->{transforms}->{$transform}->{$transform_field}) eq 'HASH'){
					foreach my $key (keys %{ $datum->{transforms}->{$transform}->{$transform_field} }){
						next unless ($transform_field . '.' . $key) =~ $self->field;
						#$self->log->trace('passed field ' . $transform_field . '.' . $key);
						if (ref($datum->{transforms}->{$transform}->{$transform_field}->{$key}) eq 'ARRAY'){
							foreach my $value (@{ $datum->{transforms}->{$transform}->{$transform_field}->{$key} }){
								if ($value =~ $self->regex){
									#$self->log->trace('passed value ' . $value);
									$datum->{transforms}->{$Name} = '__KEEP__';
									next DATUM_LOOP;
								}
							}
						}
						else {
							if ($datum->{transforms}->{$transform}->{$transform_field}->{$key} =~ $self->regex){
								#$self->log->trace('passed value ' . $datum->{transforms}->{$transform}->{$transform_field}->{$key});
								$datum->{transforms}->{$Name} = '__KEEP__';
								next DATUM_LOOP;
							}
						}
					}
				}
			}
		}
	}

	my $count = scalar @{ $self->data };
	for (my $i = 0; $i < $count; $i++){
		unless (exists $self->data->[$i]->{transforms}->{$Name}){
			splice(@{ $self->data }, $i, 1);
			$count--;
			$i--;
		}
	}
	
	$self->log->debug('data: ' . Dumper($self->data));
	
	return $self;
}

 
1;