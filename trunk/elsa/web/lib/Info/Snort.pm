package Info::Snort;
use strict;
use Data::Dumper;
use base qw( Info );
use Parse::Snort;

sub new {
	my $class = shift;
	my $self = $class->SUPER::new(@_);
	die('No sid given') unless $self->data and $self->data->{sig_sid};
	$self->data->{sig_sid} =~ /(\d+):(\d+):(\d+)/;
	$self->{_SID} = $2;
	bless($self, $class);

	my $urls = [];
	my $rule = _get_rule($self->{_SID}, $self->conf->get('info/snort/rules_file'));
	if ($rule){
		$self->{_RULE} = $rule;
		$self->summary($rule->as_string);
		# Get URL's from references
		if ($rule->references){
			foreach my $reference (@{ $rule->references }){
				if ($reference->[0] eq 'url'){
					push @{ $urls }, 'http://' . $reference->[1];
				}
			}
		}
	}
	else {
		$self->summary('Unknown sid ' . $self->{_SID});
	}
	
	$self->urls($urls);
	
	$self->plugins([qw(getPcap)]);
	
	return $self;
}

sub _get_rule {
	my $sid = shift;
	my $rules_file = shift;
	
	die('Invalid file given: ' . $rules_file) unless -f $rules_file;
	open(FH, $rules_file);
	while (<FH>){
		chomp;
		next if /^\s*#/;
		my $rule = new Parse::Snort();
		$rule->parse($_);
		if ($rule->sid eq $sid){
			close(FH);
			return $rule;
		}	
	}
	close(FH);
	return 0;
}

1;