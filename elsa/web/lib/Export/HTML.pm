package Export::HTML;
use strict;
use Data::Dumper;
use base qw( Export );
use XML::Writer;
use IO::String;

sub new {
	my $class = shift;
	my $self = $class->SUPER::new(@_);
	$self->{_MIME_TYPE} = 'text/html';
	return bless($self, $class);
}

sub results {
	my $self = shift;
	
	my @cols = @{ $self->{_COLUMNS} };
	
	my $io = new IO::String;
	my $xw = new XML::Writer(OUTPUT => $io);
	
	$xw->startTag('html');
	$xw->startTag('body');
	$xw->startTag('table');
	
	# Write column headers
	$xw->startTag('tr');
	foreach my $col (@cols){
		$xw->dataElement('th', $col);
	}
	$xw->endTag('tr');
		
	# Write data rows
	foreach my $row (@{ $self->{_GRID} }){
		$xw->startTag('tr');
		foreach my $col (@cols){
			$xw->dataElement('td', $row->{$col});
		}
		$xw->endTag('tr');
	}
	
	$xw->endTag('table');
	$xw->endTag('body');
	$xw->endTag('html');
	$xw->end();
		
	$self->{_RESULTS} = ${ $io->string_ref() };
	return $self->{_RESULTS};
}

1;