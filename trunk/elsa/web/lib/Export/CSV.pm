package Export::CSV;
use strict;
use Data::Dumper;
use base qw( Export );

sub new {
	my $class = shift;
	my $self = $class->SUPER::new(@_);
	$self->{_MIME_TYPE} = 'text/plain';
	$self->{_EXTENSION} = 'csv';
	#$self->{_MIME_TYPE} = 'application/csv';
	return bless($self, $class);
}

sub results {
	my $self = shift;
	
	my @cols = @{ $self->{_COLUMNS} };
	
	# Write column headers
	my $text = join(",", @cols) . "\n";
	
	# Write data rows
	foreach my $row (@{ $self->{_GRID} }){
		my @vals;
		foreach my $col (@cols){
			push @vals, $row->{$col};
		}
		$text .= join(",", @vals) . "\n";
	}
		
	$self->{_RESULTS} = $text;
	return $self->{_RESULTS};
}

1;