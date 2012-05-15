package Connector::CIF;
use Moose;
use Data::Dumper;
use DBI qw(:sql_types);
use Socket qw(inet_aton);
extends 'Connector';

our $Timeout = 10;
our $DefaultTimeOffset = 120;
our $Description = 'Run CIF via map/reduce';
sub description { return $Description }
sub admin_required { return 1 }
our $Fields = { map { $_ => 1 } qw(srcip dstip site hostname) };

has 'known_subnets' => (is => 'rw', isa => 'HashRef');
has 'known_orgs' => (is => 'rw', isa => 'HashRef');

sub BUILD {
	my $self = shift;
	
	if ($self->api->conf->get('transforms/whois/known_subnets')){
		$self->known_subnets($self->api->conf->get('transforms/whois/known_subnets'));
	}
	if ($self->api->conf->get('transforms/whois/known_orgs')){
		$self->known_orgs($self->api->conf->get('transforms/whois/known_orgs'));
	}
		
	my $cif = DBI->connect($self->api->conf->get('connectors/cif/dsn', '', ''), 
		{ 
			RaiseError => 1,
			mysql_multi_statements => 1,
			mysql_bind_type_guessing => 1,
			autocommit => 0, # speeds up query times by not requiring extra command to be sent
		}) or die($DBI::errstr);
	my ($query, $sth);
	$query = 'SELECT * FROM url, domain WHERE MATCH(?)';
	$sth = $cif->prepare($query);
	$query = 'SELECT * FROM infrastructure WHERE MATCH(?) AND subnet_start <= ? AND subnet_end >= ?';
	my $ip_sth = $cif->prepare($query);
	
	my @results;
	RECORD_LOOP: foreach my $record (@{ $self->results->{results} }){
		foreach my $field_hash (@{ $record->{_fields} }){
			if ($Fields->{ $field_hash->{field} }){
				my $row;
				# Handle IP's
				if ($field_hash->{value} =~ /^(\d{1,3}\.\d{1,3}\.)\d{1,3}\.\d{1,3}$/){
					next if $self->_check_local($field_hash->{value});
					my $first_octets = $1;
					my $ip_int = unpack('N*', inet_aton($field_hash->{value}));
					$ip_sth->bind_param(1, '@address ' . $first_octets . '* @description -search @alternativeid -www.alexa.com');
					$ip_sth->bind_param(2, $ip_int, SQL_INTEGER);
					$ip_sth->bind_param(3, $ip_int, SQL_INTEGER);
					$ip_sth->execute;
#					$ip_sth->execute('@address ' . $first_octet . '* @description -search', 
#						$ip_int, $ip_int);
					$row = $ip_sth->fetchrow_hashref;
					if ($row){
						foreach my $key (keys %$row){
							push @{ $record->{_fields} }, { field => $key, value => $row->{$key}, class => 'Transform.CIF' };
						}
						push @results, $record;
						next RECORD_LOOP;
					}
				}
				
				$sth->execute($field_hash->{value} . ' -@description search');
				$row = $sth->fetchrow_hashref;
			
				next unless $row;
				foreach my $key (keys %$row){
					push @{ $record->{_fields} }, { field => $key, value => $row->{$key}, class => 'Transform.CIF' };
				}
				push @results, $record;
				next RECORD_LOOP;
			}
		}
	}
	$self->results({results => \@results });
	
	return 1;
}

sub _check_local {
	my $self = shift;
	my $ip = shift;
	my $ip_int = unpack('N*', inet_aton($ip));
	
	return unless $ip_int and $self->known_subnets and $self->known_orgs;
	
	foreach my $start (keys %{ $self->known_subnets }){
		if (unpack('N*', inet_aton($start)) <= $ip_int 
			and unpack('N*', inet_aton($self->known_subnets->{$start}->{end})) >= $ip_int){
			return 1;
		}
	}
}


1