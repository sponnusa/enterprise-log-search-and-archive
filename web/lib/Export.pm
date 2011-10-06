package Export;
use strict;
use Data::Dumper;

# Base class for Result plugins

sub new {
	my $class = shift;
	my ($results) = @_;
	
	my $self = { _RAW_RESULTS => $results };
	
	bless ($self, $class);
	
	$self->{_COLUMNS} = [ 'timestamp' ];
	$self->{_GRID} = [];
	
	# Was this a result set of data rows or a groupby?
	if ($self->{_RAW_RESULTS}->[0]->{groupby}){
		$self->{_COLUMNS} = [ 'count', 'groupby' ];
		$self->{_GRID} = $self->{_RAW_RESULTS};
	}
	else {
		foreach my $field_hash (@{ $self->{_RAW_RESULTS}->[0]->{_fields} }){
			push @{ $self->{_COLUMNS} }, $field_hash->{field};
		}
		foreach my $row (@{ $self->{_RAW_RESULTS} }){
			my $grid_hash = { timestamp => $row->{timestamp} };
			foreach my $field_hash (@{ $row->{_fields} }){
				$grid_hash->{ $field_hash->{field} } = $field_hash->{value};
			}
			push @{ $self->{_GRID} }, $grid_hash;
		}
	}
	
	$self->{_RESULTS} = {};
	
	return $self;
}

sub results {
	my $self = shift;
	$self->{_RESULTS} = $self->{_RAW_RESULTS};
	return $self->{_RESULTS};
}

sub get_mime_type {
	my $self = shift;
	return $self->{_MIME_TYPE};
}

sub extension {
	my $self = shift;
	return $self->{_EXTENSION};
}


1;