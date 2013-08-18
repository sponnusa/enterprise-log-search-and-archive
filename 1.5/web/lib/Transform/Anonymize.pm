package Transform::Anonymize;
use Moose;
use Data::Dumper;
use Socket;
extends 'Transform';
our $Name = 'Anonymize';
has 'name' => (is => 'ro', isa => 'Str', required => 1, default => $Name);
has 'known_subnets' => (is => 'ro', isa => 'HashRef', required => 1);
has 'lookup_table' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });

sub BUILDARGS {
	my $class = shift;
	my $params = $class->SUPER::BUILDARGS(@_);
	
	$params->{known_subnets} = $params->{conf}->get('transforms/whois/known_subnets');
	
	return $params;
}

sub BUILD {
	my $self = shift;
	
	DATUM_LOOP: foreach my $datum (@{ $self->data }){
		$datum->{transforms}->{$Name} = { __REPLACE__ => {} };
		$self->_anonymize_msg($datum);
	}
	
	$self->log->debug('data: ' . Dumper($self->data));
	
	return $self;
}

sub _anonymize_msg {
	my $self = shift;
	my $datum = shift;
	my @matches;
	foreach my $key (keys %$datum){
		push @matches, ($datum->{$key} =~ /(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/g);
	}
	foreach my $ip (@matches){
		$self->log->debug('checking ' . $ip);
		next unless $self->_is_local($ip);
		unless ($self->lookup_table->{$ip}){
			$self->lookup_table->{$ip} = '?.?.?.' . (scalar keys %{ $self->lookup_table });
		}
	}
	$self->log->debug('lookup_table: ' . Dumper($self->lookup_table));
	
	foreach my $ip (keys %{ $self->lookup_table }){
		my $replacement = $self->lookup_table->{$ip};
		$datum->{msg} =~ s/$ip/$replacement/g;
		$datum->{transforms}->{$Name}->{__REPLACE__}->{msg} = 1;
		$self->_anonymize_ip($datum, $ip);
	}
}

sub _anonymize_ip {
	my $self = shift;
	my $datum = shift;
	my $ip = shift;
	
	foreach my $key (keys %$datum){
		next if ref($datum->{$key});
		if ($datum->{$key} eq $ip){
			$datum->{$key} = $self->lookup_table->{$ip};
			$datum->{transforms}->{$Name}->{__REPLACE__}->{$key} = 1;
		}
	}
}

sub _is_local {
	my $self = shift;
	my $ip = shift;
	my $ip_int = unpack('N*', inet_aton($ip));
	
	foreach my $start (keys %{ $self->known_subnets }){
		my $start_int = unpack('N*', inet_aton($start));
		if ($start_int <= $ip_int and unpack('N*', inet_aton($self->known_subnets->{$start}->{end})) >= $ip_int){
			return 1;
		}
	}
	return 0;
}

 
1;