package Transform::CIF;
use Moose;
use Data::Dumper;
use CHI;
use AnyEvent::HTTP;
use Socket;
use JSON;
use URL::Encode qw(url_encode);
use Time::HiRes;
extends 'Transform';

our $Name = 'CIF';
# Whois transform plugin
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);
has 'cache' => (is => 'rw', isa => 'Object', required => 1);
has 'cv' => (is => 'rw', isa => 'Object');

#sub BUILDARGS {
#	my $class = shift;
#	my $params = $class->SUPER::BUILDARGS(@_);
#	$params->{cv} = AnyEvent->condvar;
#	return $params;
#}

sub BUILD {
	my $self = shift;
	
	my $keys = {};
	if (scalar @{ $self->args }){
		foreach my $arg (@{ $self->args }){
			$keys->{$arg} = 1;
		}
	}
	else {
		$keys = { srcip => 1, dstip => 1 };
	}	
	
	foreach my $datum (@{ $self->data }){
		$datum->{transforms}->{$Name} = {};
		
		$self->cv(AnyEvent->condvar);
		$self->cv->begin;
		foreach my $key (keys %{ $datum }){
			if ($keys->{$key}){
				$datum->{transforms}->{$Name}->{$key} = {};
				$self->_query($datum, $key, $datum->{$key});
			}
		}
		
		$self->cv->end;
		$self->cv->recv;
	}
	
	return $self;
}

sub _query {
	my $self = shift;
	my $datum = shift;
	my $key = shift;
	my $query = shift;
	
	$self->cv->begin;
	
	$query = url_encode($query);
	my $url = sprintf('http://%s/api/%s?apikey=%s&fmt=json', 
		$self->conf->get('transforms/cif/server_ip'), $query, $self->conf->get('transforms/cif/apikey'));
	
	my $info = $self->cache->get($url, expire_if => sub {
		my $obj = $_[0];
		eval {
			my $data = $obj->value;
			#$self->log->debug('data: ' . Dumper($data));
			unless (scalar keys %{ $data }){
				$self->log->debug('expiring ' . $url);
				return 1;
			}
		};
		if ($@){
			$self->log->debug('error: ' . $@ . 'value: ' . Dumper($obj->value) . ', expiring ' . $url);
			return 1;
		}
		return 0;
	});
	if ($info){
		$datum->{transforms}->{$Name}->{$key} = $info;
		$self->cv->end;
		return;
	}
	
	$self->log->debug('getting ' . $url);
	http_request GET => $url, headers => { Host => $self->conf->get('transforms/cif/server_name'), Accept => 'application/json' }, sub {
		my ($body, $hdr) = @_;
		my $data;
		eval {
			$data = decode_json($body);
		};
		if ($@){
			$self->log->error($@ . 'hdr: ' . Dumper($hdr) . ', url: ' . $url . ', body: ' . ($body ? $body : ''));
			$self->cv->end;
			return;
		}
		$self->cache->set($url, $body);
				
		if ($data and ref($data) eq 'HASH' and $data->{status} eq '200' and $data->{data}->{feed} and $data->{data}->{feed}->{entry}){
			foreach my $entry ( @{ $data->{data}->{feed}->{entry}} ){
				my $cif_datum = {};
				if ($entry->{Incident}){
					if ($entry->{Incident}->{Assessment}){
						if ($entry->{Incident}->{Assessment}->{Impact}){
							$self->log->debug('$entry' . Dumper($entry));
							if (ref($entry->{Incident}->{Assessment}->{Impact})){
								$cif_datum->{type} = $entry->{Incident}->{Assessment}->{Impact}->{content};
								$cif_datum->{severity} = $entry->{Incident}->{Assessment}->{Impact}->{severity};
							}
							else {
								$cif_datum->{type} = $entry->{Incident}->{Assessment}->{Impact};
								$cif_datum->{severity} = 'low';
							}
						}
						if ($entry->{Incident}->{Assessment}->{Confidence}){
							$cif_datum->{confidence} = $entry->{Incident}->{Assessment}->{Confidence}->{content};
						}
					}
					
					$cif_datum->{timestamp} = $entry->{Incident}->{DetectTime};
					
					if ($entry->{Incident}->{EventData}){
						if ($entry->{Incident}->{EventData}->{Flow}){
							if ($entry->{Incident}->{EventData}->{Flow}->{System}){
								if ($entry->{Incident}->{EventData}->{Flow}->{System}->{Node}){
									if ($entry->{Incident}->{EventData}->{Flow}->{System}->{Node}->{Address}){
										my $add = $entry->{Incident}->{EventData}->{Flow}->{System}->{Node}->{Address};
										if (ref($add) eq 'HASH'){
											$cif_datum->{ $add->{'ext-category'} } = $add->{content};
										}
										else {
											$cif_datum->{ip} = $add;
										}
									}
								}
								if ($entry->{Incident}->{EventData}->{Flow}->{System}->{AdditionalData}){
									if (ref($entry->{Incident}->{EventData}->{Flow}->{System}->{AdditionalData}) eq 'ARRAY'){
										$cif_datum->{description} = '';
										foreach my $add (@{ $entry->{Incident}->{EventData}->{Flow}->{System}->{AdditionalData} }){
											$cif_datum->{description} .= $add->{meaning} . '=' . $add->{content} . ' ';
										}
									}
									elsif (ref($entry->{Incident}->{EventData}->{Flow}->{System}->{AdditionalData}) eq 'HASH'){
										my $add = $entry->{Incident}->{EventData}->{Flow}->{System}->{AdditionalData};
										$cif_datum->{description} = $add->{meaning} . '=' . $add->{content};
									}
									else {
										$cif_datum->{description} = $entry->{Incident}->{EventData}->{Flow}->{System}->{AdditionalData};
									}
								}
							}
						}
					}
					
					if ($entry->{Incident}->{AlternativeID}){
						if ($entry->{Incident}->{AlternativeID}->{IncidentID}){
							if ($entry->{Incident}->{AlternativeID}->{IncidentID}->{content}){
								$cif_datum->{reference} = $entry->{Incident}->{AlternativeID}->{IncidentID}->{content};
							}
						}
					}
					
					if ($entry->{Incident}->{Description}){
						$cif_datum->{reason} = $entry->{Incident}->{Description};
					}
					foreach my $cif_key (keys %$cif_datum){
						$datum->{transforms}->{$Name}->{$key}->{$cif_key} ||= {};
						$datum->{transforms}->{$Name}->{$key}->{$cif_key}->{ $cif_datum->{$cif_key} } = 1;
					}
					#$datum->{transforms}->{$Name}->{$key} = $cif_datum;
					#$self->cache->set($url, $cif_datum);
				}
			}
			my $final = {};
			foreach my $cif_key (sort keys %{ $datum->{transforms}->{$Name}->{$key} }){
				$final->{$cif_key} = join(' ', sort keys %{ $datum->{transforms}->{$Name}->{$key}->{$cif_key} });
			}
			$datum->{transforms}->{$Name}->{$key} = $final;
					
			$self->cache->set($url, $datum->{transforms}->{$Name}->{$key});
		}
		$self->cv->end;
	};
}
 
1;