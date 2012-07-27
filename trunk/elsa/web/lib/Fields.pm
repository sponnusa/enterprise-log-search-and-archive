package Fields;
use Moose::Role;
with 'MooseX::Traits';
use Data::Dumper;
use Sys::Hostname::FQDN;
use Net::DNS;
use String::CRC32;
use Socket qw(inet_aton inet_ntoa);

our $Field_order_to_attr = {
	0 => 'timestamp',
	100 => 'minute',
	101 => 'hour',
	102 => 'day',
	1 => 'host_id',
	2 => 'program_id',
	3 => 'class_id',
	4 => 'msg',
	5 => 'attr_i0',
	6 => 'attr_i1',
	7 => 'attr_i2',
	8 => 'attr_i3',
	9 => 'attr_i4',
	10 => 'attr_i5',
	11 => 'attr_s0',
	12 => 'attr_s1',
	13 => 'attr_s2',
	14 => 'attr_s3',
	15 => 'attr_s4',
	16 => 'attr_s5',
};

our $Field_order_to_meta_attr = {
	0 => 'timestamp',
	100 => 'minute',
	101 => 'hour',
	102 => 'day',
	1 => 'host_id',
	2 => 'program_id',
	3 => 'class_id',
	4 => 'msg',
};

our $Field_order_to_field = {
	1 => 'host',
	4 => 'msg',
	5 => 'i0',
	6 => 'i1',
	7 => 'i2',
	8 => 'i3',
	9 => 'i4',
	10 => 'i5',
	11 => 's0',
	12 => 's1',
	13 => 's2',
	14 => 's3',
	15 => 's4',
	16 => 's5',
};

our $Field_to_order = {
	'timestamp' => 0,
	'minute' => 100,
	'hour' => 101,
	'day' => 102,
	'host' => 1,
	'program' => 2,
	'class' => 3,
	'msg' => 4,
	'i0' => 5,
	'i1' => 6,
	'i2' => 7,
	'i3' => 8,
	'i4' => 9,
	'i5' => 10,
	's0' => 11,
	's1' => 12,
	's2' => 13,
	's3' => 14,
	's4' => 15,
	's5' => 16,
};

our $Proto_map = {
	'ICMP' => 1,
	'icmp' => 1,
	'TCP' => 6,
	'tcp' => 6,
	'UDP' => 17,
	'udp' => 17,
};

our $Inverse_proto_map = {
	1 => 'ICMP',
	6 => 'TCP',
	17 => 'UDP',
};

our $Time_values = {
	timestamp => 1,
	minute => 60,
	hour => 3600,
	day => 86400,
};

# Helper methods for dealing with resolving fields
has 'node_info' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });

sub epoch2iso {
	my $epochdate = shift;
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($epochdate);
	my $date = sprintf("%04d-%02d-%02d %02d:%02d:%02d", 
		$year + 1900, $mon + 1, $mday, $hour, $min, $sec);
	return $date;
}

sub resolve {
	my $self = shift;
	my $raw_field = shift;
	my $raw_value = shift;
	my $operator = shift;
	
	# Return all possible class_id, real_field, real_value combos
	$self->log->debug("resolving: raw_field: $raw_field, raw_value: $raw_value, operator: $operator");
	
	my %values = ( fields => {}, attrs => {} );
	# Find all possible real fields/classes for this raw field
	
	my $operator_xlate = {
		'=' => 'and',
		'' => 'or',
		'-' => 'not',
	};

	my $field_infos = $self->get_field($raw_field);
	$self->log->trace('field_infos: ' . Dumper($field_infos));
	foreach my $class_id (keys %{$field_infos}){
		if (scalar keys %{ $self->classes->{given} } and not $self->classes->{given}->{0}){
			unless ($self->classes->{given}->{$class_id} or $class_id == 0){
				$self->log->debug("Skipping class $class_id because it was not given");
				next;
			}
		}
		# we don't want to count class_id 0 as "distinct"
		if ($class_id){
			$self->classes->{distinct}->{$class_id} = 1;
		}
		
		my $field_order = $field_infos->{$class_id}->{field_order};
		# Check for string match and make that a term
		if ($field_infos->{$class_id}->{field_type} eq 'string' and
			($operator eq '=' or $operator eq '-' or $operator eq '')){
			$values{fields}->{$class_id}->{ $Field_order_to_field->{ $field_order } } = $raw_value;
		}
		elsif ($field_infos->{$class_id}->{field_type} eq 'string'){
			die('Invalid operator for string field');
		}
		elsif ($Field_order_to_attr->{ $field_order }){
			$values{attrs}->{$class_id}->{ $Field_order_to_attr->{ $field_order } } =
				$self->normalize_value($class_id, $raw_value, $field_order);			
		}
		else {
			$self->log->warn("Unknown field: $raw_field");
		}
	}
	$self->log->trace('values: ' . Dumper(\%values));
	return \%values;
}

sub normalize_value {
	my $self = shift;
	my $class_id = shift;
	my $value = shift;
	my $field_order = shift;
	
	my $orig_value = $value;
	$value =~ s/^\"//;
	$value =~ s/\"$//;
	
	#$self->log->trace('args: ' . Dumper($args) . ' value: ' . $value . ' field_order: ' . $field_order);
	
	unless (defined $class_id and defined $value and defined $field_order){
		$self->log->error('Missing an arg: ' . $class_id . ', ' . $value . ', ' . $field_order);
		return $value;
	}
	
	return $value unless $self->node_info->{field_conversions}->{ $class_id };
	#$self->log->debug("normalizing for class_id $class_id with the following: " . Dumper($self->node_info->{field_conversions}->{ $class_id }));
	
	if ($field_order == $Field_to_order->{host}){ #host is handled specially
		my @ret;
		if ($value =~ /^"?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"?$/) {
			@ret = ( unpack('N*', inet_aton($1)) ); 
		}
		elsif ($value =~ /^"?([a-zA-Z0-9\-\.]+)"?$/){
			my $host_to_resolve = $1;
			unless ($host_to_resolve =~ /\./){
				my $fqdn_hostname = Sys::Hostname::FQDN::fqdn();
				$fqdn_hostname =~ /^[^\.]+\.(.+)/;
				my $domain = $1;
				$self->log->debug('non-fqdn given, assuming to be domain: ' . $domain);
				$host_to_resolve .= '.' . $domain;
			}
			$self->log->debug('resolving and converting host ' . $host_to_resolve. ' to inet_aton');
			my $res   = Net::DNS::Resolver->new;
			my $query = $res->search($host_to_resolve);
			if ($query){
				my @ips;
				foreach my $rr ($query->answer){
					next unless $rr->type eq "A";
					$self->log->debug('resolved host ' . $host_to_resolve . ' to ' . $rr->address);
					push @ips, $rr->address;
				}
				if (scalar @ips){
					foreach my $ip (@ips){
						my $ip_int = unpack('N*', inet_aton($ip));
						push @ret, $ip_int;
					}
				}
				else {
					die 'Unable to resolve host ' . $host_to_resolve . ': ' . $res->errorstring;
				}
			}
			else {
				die 'Unable to resolve host ' . $host_to_resolve . ': ' . $res->errorstring;
			}
		}
		else {
			die 'Invalid host given: ' . $value;
		}
		if (wantarray){
			return @ret;
		}
		else {
			return $ret[0];
		}
	}
	elsif ($self->node_info->{field_conversions}->{ $class_id }->{'IPv4'}
		and $self->node_info->{field_conversions}->{ $class_id }->{'IPv4'}->{$field_order}){
		return unpack('N', inet_aton($value));
	}
	elsif ($self->node_info->{field_conversions}->{ $class_id }->{PROTO} 
		and $self->node_info->{field_conversions}->{ $class_id }->{PROTO}->{$field_order}){
		$self->log->trace("Converting $value to proto");
		return $Proto_map->{ $value };
	}
	elsif ($self->node_info->{field_conversions}->{ $class_id }->{COUNTRY_CODE} 
		and $self->node_info->{field_conversions}->{ $class_id }->{COUNTRY_CODE}->{$field_order}){
		$self->log->trace("Converting $value to country_code");
		return join('', unpack('c*', pack('A*', uc($value))));
	}
	elsif ($Field_order_to_attr->{$field_order} eq 'program_id'){
		$self->log->trace("Converting $value to attr");
		return crc32($value);
	}
	elsif ($Field_order_to_attr->{$field_order} =~ /^attr_s\d+$/){
		# String attributes need to be crc'd
		return crc32($value);
	}
	else {
		#apparently we don't know about any conversions
		#$self->log->debug("No conversion for $value and class_id $class_id, field_order $field_order.");
		return $orig_value; 
	}
}


sub get_field {
	my $self = shift;
	my $raw_field = shift;
		
	# Account for FQDN fields which come with the class name
	my ($class, $field) = split(/\./, $raw_field);
	
	if ($field){
		# We were given an FQDN, so there is only one class this can be
		foreach my $field_hash (@{ $self->node_info->{fields} }){
			if (lc($field_hash->{fqdn_field}) eq lc($raw_field)){
				return { $self->node_info->{classes}->{uc($class)} => $field_hash };
			}
		}
	}
	else {
		# Was not FQDN
		$field = $raw_field;
	}
	
	$class = 0;
	my %fields;
		
	# Could also be a meta-field/attribute
	if (defined $Field_to_order->{$field}){
		$fields{$class} = { 
			value => $field, 
			text => uc($field), 
			field_id => $Field_to_order->{$field},
			class_id => $class, 
			field_order => $Field_to_order->{$field},
			field_type => 'int',
		};
	}
		
	foreach my $row (@{ $self->node_info->{fields} }){
		if ($row->{value} eq $field){
			$fields{ $row->{class_id} } = $row;
		}
	}
	
	return \%fields;
}

# Opposite of normalize
sub resolve_value {
	my $self = shift;
	my $class_id = shift;
	my $value = shift;
	my $col = shift;
	
	my $field_order = $Field_to_order->{$col};
	unless (defined $field_order){
		$col =~ s/\_id$//;
		$field_order = $Field_to_order->{$col};
		unless ($field_order){
			$self->log->warn('No field_order not found for col ' . $col);
			return $value;
		}
	}
	#$self->log->debug('field_order ' . $field_order . ', col: ' . $col . ', conversions: ' . Dumper($self->node_info->{field_conversions}->{ $class_id }));
	
	if ($Field_order_to_meta_attr->{$field_order}){
		#$self->log->trace('interpreting field_order ' . $field_order . ' with class ' . $class_id . ' to be meta');
		$class_id = 0;
	}
	
	if ($self->node_info->{field_conversions}->{ $class_id }->{TIME}->{$field_order}){
		return epoch2iso($value * $Time_values->{ $Field_order_to_attr->{$field_order} });
	}
	elsif ($self->node_info->{field_conversions}->{ $class_id }->{IPv4}->{$field_order}){
		#$self->log->debug("Converting $value from IPv4");
		return inet_ntoa(pack('N', $value));
	}
	elsif ($self->node_info->{field_conversions}->{ $class_id }->{PROTO}->{$field_order}){
		#$self->log->debug("Converting $value from proto");
		return $Inverse_proto_map->{ $value };
	}
	elsif ($self->node_info->{field_conversions}->{ $class_id }->{COUNTRY_CODE} 
		and $self->node_info->{field_conversions}->{ $class_id }->{COUNTRY_CODE}->{$field_order}){
		my @arr = $value =~ /(\d{2})(\d{2})/;
		return unpack('A*', pack('c*', @arr));
	}
	elsif ($Field_order_to_attr->{$field_order} eq 'class_id'){
		return $self->node_info->{classes_by_id}->{$class_id};
	}
	else {
		#apparently we don't know about any conversions
		#$self->log->debug("No conversion for $value and class_id $class_id");
		return $value; 
	}
}

sub normalize_quoted_value {
	my $self = shift;
	my $value = shift;
	
	# Quoted integers don't work for some reason
	if ($value =~ /^\d+$/){
		return $value;
	}
	else {
		return '"' . $value . '"';
	}
}

sub resolve_field_permissions {
	my ($self, $user) = @_;
	return if $user->permissions->{resolved}; # allows this to be idempotent
	
	if ($user->is_admin){
		$user->permissions->{fields} = {};
		$user->permissions->{resolved} = 1;
		return;
	}
	
	my %permissions;
	foreach my $field (keys %{ $user->permissions->{fields} }){
		my $value = $user->permissions->{fields}->{$field};
		my $field_infos = $self->get_field($field);
										
		# Set attributes for searching
		foreach my $class_id (keys %{ $field_infos }){
			my $attr_name = $Field_order_to_attr->{ $field_infos->{$class_id}->{field_order } };
			my $field_name = $Field_order_to_field->{ $field_infos->{$class_id}->{field_order } };
			my $attr_value = $value;
			if ($field_infos->{$class_id}->{field_type} eq 'string'){
				$attr_value = crc32($value);
			}
			$permissions{$class_id} ||= [];
			push @{ $permissions{$class_id} }, 
				{ name => $field, attr => [ $attr_name, $attr_value ], field => [ $field_name, $value ] };
		}
	}
	$user->permissions->{fields} = \%permissions;
	$user->permissions->{resolved} = 1;
}

1;