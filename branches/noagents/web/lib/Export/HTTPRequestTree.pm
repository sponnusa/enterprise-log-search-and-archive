package Export::HTTPRequestTree;
use strict;
use Data::Dumper;
use base qw( Export );

sub _flows { my $self = shift; return $self->{_FLOWS}; }
sub _referers { my $self = shift; return $self->{_REFERERS}; }
sub _sites { my $self = shift; return $self->{_SITES}; }
sub _tree { my $self = shift; return $self->{_TREE}; }

sub new {
	my $class = shift;
	my $self = $class->SUPER::new(@_);
	$self->{_MIME_TYPE} = 'text/plain';
	$self->{_TREE} = { value => 'root', children => [] };
	$self->{_FLOWS} = {};
	$self->{_REFERERS} = {};
	$self->{_SITES} = {};
	$self = bless($self, $class);
	$self->_parse();
	eval {
		$self->{_RESULTS} = _draw($self->_tree, 0);
	};
	if ($@){
		$self->{_ERROR} = $@;
	}
	return $self;
}

sub results {
	my $self = shift;
	return $self->{_RESULTS};
}

sub _draw {
	my $node = shift;
	my $level = shift;
	
	my $tmp = '';
	if ($node->{value}){
		my $tabs = '';
		for (my $i = 0; $i < $level; $i++){
			$tabs .= "\t";
		}
		$tmp .= $tabs . $node->{value} . "\n";
	}
	if (scalar @{ $node->{children} }){
		$level++;
		foreach my $child (@{ $node->{children} }){
			$tmp .= _draw($child, $level);
		}
	}
	return $tmp;
}

sub _parse {
	my $self = shift;
	
	foreach my $log (@{ $self->{_GRID} }){
		next unless $log->{class} eq 'URL';
		my $tuple = $log->{srcip} . ':' . $log->{dstip}; 
		#warn $tuple;
		#$self->_flows->{$tuple} ||= [];
		push @{ $self->_flows->{$tuple} }, $log;
		
		if ($log->{referer}){
			my $ref = $log->{referer};
			$ref =~ s/.+\/\/(.+)/$1/;
			$self->_referers->{$tuple} = $ref;
		}
		
		$self->_sites->{ $log->{dstip} } = $log->{site};
		
		my $url = sprintf('%s%s', $log->{site}, $log->{uri});
		
		my $to_find = 'root';
		if ($self->_referers->{$tuple}){
			$to_find = $self->_referers->{$tuple};
		}
		my $parent = $self->_find_node($to_find, $self->_tree);
		$parent->{value} ||= 'root';
		if ($parent->{value} eq 'root' and $to_find ne 'root'){
			push @{ $self->_tree->{children} }, { value => $to_find, children => [] };
			$parent = $self->_find_node($to_find, $self->_tree);
		}
		push @{ $parent->{children} }, { value => $url, children => [] };
	}
}

sub _find_node {
	my $self = shift;
	my $to_find = shift;
	my $node = shift;
	#warn 'to_find: ' . $to_find . ', value: ' . $node->{value};
	
	if ($node->{value} eq $to_find){
		return $node;
	}
	else {
		foreach my $child (@{ $node->{children} }){
			my $ret = $self->_find_node($to_find, $child);
			return $ret if $ret;
		}
	}
	return;
}

1;