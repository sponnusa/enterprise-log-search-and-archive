package Export::PDF;
use strict;
use Data::Dumper;
use base qw( Export );
use PDF::API2::Simple;
use IO::String;

sub new {
	my $class = shift;
	my $self = $class->SUPER::new(@_);
	$self->{_MIME_TYPE} = 'application/pdf';
	return bless($self, $class);
}

sub results {
	my $self = shift;
	
	# Create an in-memory filehandle for our result
	my $io = new IO::String();
	
	# Create a new PDF
	my $pdf = PDF::API2::Simple->new( file => $io );

	$pdf->add_font('Verdana');

	# Add a page
	$pdf->add_page();
	
	my @cols = @{ $self->{_COLUMNS} };
	
	# Write column headers
	my $text = join("\t", @cols);
	$pdf->text($text, autoflow => 'on');

	# Write data rows
	foreach my $row (@{ $self->{_GRID} }){
		$text = '';
		for (my $i = 0; $i <=$#cols; $i++){
			$text .= $row->{ $cols[$i] } . "\t";
		}
		$pdf->text($text, autoflow => 'on');	
	}
		
	$self->{_RESULTS} = $pdf->as_string();
	return $self->{_RESULTS};
}

1;