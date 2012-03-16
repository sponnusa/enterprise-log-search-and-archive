package Transform::Database;
use Moose;
use Moose::Meta::Class;
use Data::Dumper;
use CHI;
use DBI;
use JSON;
use URL::Encode qw(url_encode);
use Time::HiRes;
extends 'Transform';

our $Name = 'Database';
# Whois transform plugin
has 'name' => (is => 'rw', isa => 'Str', required => 1, default => $Name);
has 'cache' => (is => 'rw', isa => 'Object', required => 1);
has 'dsn' => (is => 'rw', isa => 'Str', required => 1);
has 'username' => (is => 'rw', isa => 'Str', required => 1);
has 'password' => (is => 'rw', isa => 'Str', required => 1);
has 'query_template' => (is => 'rw', isa => 'Str', required => 1);
has 'query_placeholders' => (is => 'rw', isa => 'ArrayRef', required => 1);
has 'fields' => (is => 'rw', isa => 'ArrayRef', required => 1);

sub BUILD {
	my $self = shift;
	
	my @fields_for_placeholders;
	foreach my $item (@{ $self->query_placeholders }){
		push @fields_for_placeholders, $item;
	}
	
	my $dbh = DBI->connect($self->dsn, $self->username, $self->password, { RaiseError => 1 });
	my ($query, $sth);
	if (scalar @{ $self->args }){
		$self->fields($self->args);
	}
	$query = sprintf($self->query_template, join(', ', @{ $self->fields }));
	$self->log->debug('query: ' . $query);
	$sth = $dbh->prepare($query);
		
	foreach my $datum (@{ $self->data }){
		$datum->{transforms}->{$Name} = {};
		
		my @placeholders;
		foreach my $key (@fields_for_placeholders){
			if ($datum->{$key}){
				push @placeholders, $datum->{$key};
			}
		}
		#$self->log->debug('placeholders: ' . Dumper(\@placeholders));
		$sth->execute(@placeholders) or die($sth->errstr);
		my @rows;
		while (my $row = $sth->fetchrow_hashref){
			push @rows, $row;
		}
		
		foreach my $field (@{ $self->fields }){
			$datum->{transforms}->{$Name}->{$field} = {};
			foreach my $row (@rows){
				#$self->log->debug('row: ' . Dumper($row));
				foreach my $key (keys %$row){
					$datum->{transforms}->{$Name}->{$key} = $row->{$key};
				}
			} 
		}
	}
		
	return $self;
}

 
1;
