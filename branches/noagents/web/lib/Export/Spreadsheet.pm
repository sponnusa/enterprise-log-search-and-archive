package Export::Spreadsheet;
use strict;
use Data::Dumper;
use base qw( Export );
use Spreadsheet::WriteExcel;
use IO::String;

sub new {
	my $class = shift;
	my $self = $class->SUPER::new(@_);
	$self->{_MIME_TYPE} = 'application/excel';
	$self->{_EXTENSION} = 'xls';
	return bless($self, $class);
}

sub results {
	my $self = shift;
	
	# Create an in-memory filehandle for our result
	my $io = new IO::String();
	
	# Create a new Excel workbook
	my $workbook = Spreadsheet::WriteExcel->new($io);

	# Add a worksheet
	my $worksheet = $workbook->add_worksheet();
	
	my @cols = @{ $self->{_COLUMNS} };
	
	# Write column headers
	for (my $i = 0; $i <= $#cols; $i++){
		$worksheet->write(0, $i, $cols[$i]);
	}
	
	#warn('cols: ' . Dumper(\@cols));

	# Write data rows
	my $row_counter = 1;
	foreach my $row (@{ $self->{_GRID} }){
		for (my $i = 0; $i <= $#cols; $i++){
			#warn('writing i: ' . $i . ', col ' . $cols[$i] . ', val: ' . $row->{ $cols[$i] });
			$worksheet->write($row_counter, $i, $row->{ $cols[$i] });
		}
		$row_counter++;	
	}
	
	$workbook->close();
		
	$self->{_RESULTS} = ${ $io->string_ref() };
	return $self->{_RESULTS};
}

1;