package Transform::Has;
use Moose;
use Data::Dumper;
use CHI;
use AnyEvent::HTTP;
use Socket;
use JSON;
extends 'Transform';
our $Name = 'Has';
# Whois transform plugin
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);
has 'count' => (is => 'ro', isa => 'Num', required => 1, default => 0);
has 'operator' => (is => 'ro', isa => 'Str', required => 1, default => '>=');

sub BUILDARGS {
	my $class = shift;
	##my $params = $class->SUPER::BUILDARGS(@_);
	my %params = @_;
	if (defined $params{args}->[0]){
		$params{count} = $params{args}->[0];
	}
	if ($params{args}->[1]){
		$params{operator} = $params{args}->[1];
	}
	return \%params;
}

sub BUILD {
	my $self = shift;
	$self->log->trace('data: ' . Dumper($self->data));
	
	DATUM_LOOP: foreach my $datum (@{ $self->data }){
		foreach my $transform (keys %{ $datum->{transforms} }){
			next unless ref($datum->{transforms}->{$transform}) eq 'HASH';
			foreach my $transform_field (keys %{ $datum->{transforms}->{$transform} }){
				if (ref($datum->{transforms}->{$transform}->{$transform_field}) eq 'HASH'){
					if (exists $datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby }){
						if (ref($datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby }) eq 'ARRAY'){
							foreach my $value (@{ $datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } }){
								my $test = $value . ' ' . $self->operator . ' ' . $self->count;
								if (eval($test)){
									#$self->log->trace('passed value ' . $value);
									$datum->{transforms}->{$Name} = '__KEEP__';
									next DATUM_LOOP;
								}
							}
						}
						else {
							if ($datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } =~ /^\d+$/){
								my $test = $datum->{transforms}->{$transform}->{$transform_field}->{ $self->groupby } . ' ' . $self->operator . ' ' . $self->count;
								if (eval($test)){
									#$self->log->trace('passed value ' . $value);
									$datum->{transforms}->{$Name} = '__KEEP__';
									next DATUM_LOOP;
								}
							}
						}
					}
				}
				elsif (ref($datum->{transforms}->{$transform}->{$transform_field}) eq 'ARRAY' 
					and $transform_field eq $self->groupby){
					foreach my $value (@{ $datum->{transforms}->{$transform}->{$transform_field} }){
						my $test = $value . ' ' . $self->operator . ' ' . $self->count;
						if (eval($test)){
							#$self->log->trace('passed value ' . $value);
							$datum->{transforms}->{$Name} = '__KEEP__';
							next DATUM_LOOP;
						}
					}
				}
			}
		}
		if (exists $datum->{ $self->groupby } ){
			my $test = $datum->{count} . ' ' . $self->operator . ' ' . $self->count;
			#$self->log->trace('test: ' . $test);
			if (eval($test)){
				#$self->log->trace('passed value ' . $value);
				$datum->{transforms}->{$Name} = '__KEEP__';
				next DATUM_LOOP;
			}
		}
		elsif (exists $datum->{_count}){
			my $test = $datum->{_count} . ' ' . $self->operator . ' ' . $self->count;
			if (eval($test)){
				#$self->log->trace('passed value ' . $value);
				$datum->{transforms}->{$Name} = '__KEEP__';
				next DATUM_LOOP;
			}
		}
	}
	
	my $ret = [];
		
	my $count = scalar @{ $self->data };
	for (my $i = 0; $i < $count; $i++){
		if (exists $self->data->[$i]->{transforms}->{$Name}){
			delete $self->data->[$i]->{transforms}->{$Name}; # no need to clutter our final results
			if (exists $self->data->[$i]->{_groupby}){
				push @$ret, $self->data->[$i];
			}
			else {
				push @$ret, { 
					_groupby => $self->data->[$i]->{ $self->groupby }, 
					intval => $self->data->[$i]->{count}, 
					_count => $self->data->[$i]->{count},
					count => $self->data->[$i]->{count},
					$self->groupby => $self->data->[$i]->{ $self->groupby },
				};
			}
		}
	}
	
	# Sort
	$ret = [ sort { $b->{intval} <=> $a->{intval} } @$ret ];
	$self->data($ret);
		
	$self->log->debug('final data: ' . Dumper($self->data));
	
	return $self;
}

 
1;