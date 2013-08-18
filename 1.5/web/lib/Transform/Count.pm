package Transform::Count;
use Moose;
use Data::Dumper;
use CHI;
use AnyEvent::HTTP;
use Socket;
use JSON;
extends 'Transform';
our $Name = 'Count';
# Whois transform plugin
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);

sub BUILDARGS {
	my $class = shift;
	my %params = @_;
	$params{groupby} = $params{args}->[0];
	return \%params;
}

sub BUILD {
	my $self = shift;
	$self->log->trace('data: ' . Dumper($self->data));
	
	my $sums = {};
	foreach my $datum (@{ $self->data }){
		foreach my $transform (keys %{ $datum->{transforms} }){
			next unless ref($datum->{transforms}->{$transform}) eq 'HASH';
			foreach my $transform_field (keys %{ $datum->{transforms}->{$transform} }){
				if (ref($datum->{transforms}->{$transform}->{$transform_field}) eq 'HASH'){
					if (exists $datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby }){
						if (ref($datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby }) eq 'ARRAY'){
							foreach my $value (@{ $datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } }){
#								if ($value =~ /^\d+$/){
#									$sums->{ $value } += $value;
#								}
#								else {
#									$sums->{ $value } += $datum->{'count'};
#								}
								$sums->{ $value }++;
							}
						}
						else {
#							if ($datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } =~ /^\d+$/){
#								$sums->{ $datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } } += 
#									$datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby };
#							}
#							else {
#								$sums->{ $datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } } += $datum->{count};
#							}
							$sums->{ $datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } }++;
						}
					}
				}
				elsif (ref($datum->{transforms}->{$transform}->{$transform_field}) eq 'ARRAY' 
					and $transform_field eq $self->groupby){
					foreach my $value (@{ $datum->{transforms}->{$transform}->{$transform_field} }){
#						if ($value =~ /^\d+$/){
#							$sums->{ $value } += $value;
#						}
#						else {
#							$sums->{ $value } += $datum->{'count'};
#						}
						$sums->{ $value }++;
					}
				}
			}
		}
		if (exists $datum->{ $self->groupby } ){
			if ($datum->{ $self->groupby } =~ /^\d+$/){
				#$sums->{ $self->groupby } += $datum->{ $self->groupby };
				$sums->{ $self->groupby }++;
			}
			else {
				#$sums->{ $datum->{ $self->groupby } } += $datum->{'count'};
				$sums->{ $datum->{ $self->groupby } }++;
			}
		}
	}
	$self->log->debug('sums: ' . Dumper($sums));
	my $ret = [];
	foreach my $key (keys %$sums){
		push @$ret, { _groupby => $key, intval => $sums->{$key}, _count => $sums->{$key} };
	}
	
	# Sort
	$ret = [ sort { $b->{intval} <=> $a->{intval} } @$ret ];
	
	$self->data($ret);
	$self->log->debug('data: ' . Dumper($self->data));
	
	return $self;
}

 
1;