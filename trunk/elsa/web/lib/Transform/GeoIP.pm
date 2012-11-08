package Transform::GeoIP;
use Moose;
use Data::Dumper;
use Socket;
use JSON;
use Geo::IP; # get additional databases from Maxmind.com
extends 'Transform';
our $Name = 'GeoIP';
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);

sub BUILD {
	my $self = shift;
	
	my $geoip;
	my $cc_only = 0;
	eval {
		$geoip = Geo::IP->open_type(GEOIP_CITY_EDITION_REV1, GEOIP_MEMORY_CACHE) or die('Unable to create GeoIP object: ' . $!);
	};
	if ($@){
		$self->log->warn('GeoIP city edition not found, falling back to country edition');
		$geoip = Geo::IP->open_type(GEOIP_COUNTRY_EDITION, GEOIP_MEMORY_CACHE) or die('Unable to create GeoIP object: ' . $!);
		$cc_only = 1;
	}
	
	foreach my $datum (@{ $self->data }){
		$datum->{transforms}->{$Name} = {};
		
		foreach my $key (keys %{ $datum }){
			if ($key eq 'srcip' or $key eq 'dstip' or $key eq 'ip'){
				if ($cc_only){
					my $record = $geoip->country_code_by_addr($datum->{$key});
					next unless $record;
					$datum->{transforms}->{$Name}->{$key} = {
						cc => $record
					}
				}
				else {
					my $record = $geoip->record_by_addr($datum->{$key});
					next unless $record;
					$datum->{transforms}->{$Name}->{$key} = {
						cc => $record->country_code,
						latitude => $record->latitude,
						longitude => $record->longitude,
						state => $record->region,
						city => $record->city,
					}
				}
			}
		}
		foreach my $key (keys %{ $datum }){
			if ($key eq 'hostname'){
				$datum->{transforms}->{$Name}->{$key} = $geoip->record_by_name($datum->{$key});
			}
		}
	}
	
	return $self;
}
 
1;