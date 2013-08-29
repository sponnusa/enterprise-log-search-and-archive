package Query::SQL;
use Moose;
extends 'Query';

sub BUILD {
	my $self = shift;
	
	# Normalize query terms
	foreach my $boolean (keys %{ $self->terms }){
		foreach my $term_hash (keys %{ $self->terms->{$boolean} }){
			# Escape any special chars
			$term_hash->{value} =~ s/([^a-zA-Z0-9\.\_\-\@])/\\$1/g;
		}
	}
	return $self;
}

sub estimate_query_time {
	my $self = shift;
	
	my $query_time = 0;
	
	my $largest = 0;
	
	my $archive_query_rows_per_second = 300_000; # guestimate
	if ($self->conf->get('archive_query_rows_per_second')){
		$archive_query_rows_per_second = $self->conf->get('archive_query_rows_per_second');
	}
	
#	# For every node, find the total rows that will have to be searched and use the largest value (each node queries in parallel).
#	foreach my $node (keys %{ $self->node_info->{nodes} }){
#		my $node_info = $self->node_info->{nodes}->{$node};
#		my $total_rows = 0;
#		foreach my $table (@{ $node_info->{tables}->{tables} }){
#			next unless $table->{table_type} eq 'archive';
#			if ($self->start and $q->end){
#				if ((($q->start >= $table->{start_int} and $q->start <= $table->{end_int})
#					or ($q->end >= $table->{start_int} and $q->end <= $table->{end_int})
#					or ($q->start <= $table->{start_int} and $q->end >= $table->{end_int})
#					or ($table->{start_int} <= $q->start and $table->{end_int} >= $q->end))
#				){
#					$self->log->trace('including ' . ($table->{max_id} - $table->{min_id}) . ' rows');
#					$total_rows += ($table->{max_id} - $table->{min_id});
#				}
#			}
#			else {
#				$self->log->trace('including ' . ($table->{max_id} - $table->{min_id}) . ' rows');
#				$total_rows += ($table->{max_id} - $table->{min_id});
#			}
#		}
#		if ($total_rows > $largest){
#			$largest = $total_rows;
#			$self->log->trace('found new largest ' . $largest);
#		}
#	}
	$query_time = $largest / $archive_query_rows_per_second;
	
	return $query_time;
}

1;