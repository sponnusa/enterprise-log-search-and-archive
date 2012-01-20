package Transform::Sum;
use Moose;
use Data::Dumper;
use CHI;
use AnyEvent::HTTP;
use Socket;
use JSON;
extends 'Transform';
our $Name = 'Sum';
# Whois transform plugin
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);

sub BUILDARGS {
	my $class = shift;
	##my $params = $class->SUPER::BUILDARGS(@_);
	my %params = @_;
	$params{groupby} = $params{args}->[0];
	return \%params;
}

sub BUILD {
	my $self = shift;
	
	my $sums = {};
	foreach my $datum (@{ $self->data }){
		foreach my $transform (keys %{ $datum->{transforms} }){
			next unless ref($datum->{transforms}->{$transform}) eq 'HASH';
			foreach my $transform_field (keys %{ $datum->{transforms}->{$transform} }){
				if (exists $datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby }){
					if ($datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } =~ /^\d+$/){
						$sums->{ $datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } } += 
							$datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby };
					}
					else {
						$self->log->debug('incrementing ' . $datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } . ' from ' .
							$sums->{ $datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } }); 
						$sums->{ $datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } }++;
					}
				}
			}
		}
		if (exists $datum->{ $self->groupby } ){
			if ($datum->{ $self->groupby } =~ /^\d+$/){
				$sums->{ $self->groupby } += $datum->{ $self->groupby };
			}
			else {
				$sums->{ $datum->{ $self->groupby } }++;
			}
		}
	}
	my $ret = [];
	foreach my $key (keys %$sums){
		push @$ret, { '@groupby' => $key, intval => $sums->{$key}, '@count' => $sums->{$key} };
	}
	$self->data($ret);
	$self->log->debug('data: ' . Dumper($self->data));
	
	return $self;
}

 
1;