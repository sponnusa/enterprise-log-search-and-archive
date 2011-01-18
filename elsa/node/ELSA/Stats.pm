package ELSA::Stats;
use strict;
use warnings;
use Data::Dumper;
use Time::HiRes qw(sleep time);
use Log::Log4perl;


sub new {
	my $class = shift;
	my $self = {
		_TIMELINE => [
			{ _init => time(), },
		],
	};
	return bless $self, $class;
}

sub mark {
	my $self = shift;
	my $task_name = shift;
	my $is_complete = shift;
	push @{ $self->{_TIMELINE} }, { task_name => $task_name, time => time(), complete => $is_complete };
	return 1; 
}

sub timeline {
	my $self = shift;
	return $self->{_TIMELINE};
}

sub execution_tree {
	my $self = shift;
	my @tree;
	my $last_time;
	foreach my $stat (@{ $self->timeline() }){
		next if $stat->{task_name} eq '_init';
		if ($stat->{complete}){
			OUTER_LOOP: for (my $i = (scalar @tree - 1); $i >= 0; $i--){
				if ($tree[$i]->{task_name} eq $stat->{task_name}){
					my $time_taken = $stat->{time} - $tree[$i]->{time};
					$tree[$i]->{time} = $time_taken;
					last;
				}
				else {
					INNER_LOOP: for (my $j = (scalar @{ $tree[$i]->{subtasks} }); $j >= 0; $j--){
						next unless defined $tree[$i]->{subtasks}->[$j];
						if ($tree[$i]->{subtasks}->[$j]->{task_name} eq $stat->{task_name}){
							my $time_taken = $stat->{time} - $tree[$i]->{subtasks}->[$j]->{time};
							$tree[$i]->{subtasks}->[$j]->{time} = $time_taken;
							last OUTER_LOOP;
						}
					}
				}
				if (scalar @tree){
					my $node = $tree[0];
					while (scalar @{ $node->{subtasks} }){
						$node = $node->{subtasks}->[-1];
					}
					push @{ $node->{subtasks} }, { task_name => $stat->{task_name}, subtasks => [], 
						time => ($stat->{time} - $last_time) };
				}
				else {
					push @tree, { task_name => $stat->{task_name}, subtasks => [], time => 0 };
				}
			}
		}
		else {
			if (scalar @tree){
				push @{ $tree[-1]->{subtasks} }, { task_name => $stat->{task_name}, subtasks => [], time => $stat->{time} };
			}
			else {
				push @tree, { task_name => $stat->{task_name}, subtasks => [], time => $stat->{time} };
			}
		}
		$last_time = $stat->{time};
	}
	return \@tree;
}

sub clear {
	my $self = shift;
	
	my $cleared = scalar @{ $self->{_TIMELINE} };
	$self->{_TIMELINE} = [
		{ _init => time(), },
	];
	return $cleared;
}

sub get_task {
	my $self = shift;
	my $task_name = shift;
	my $tree = shift;
	
	unless ($tree){
		$tree = $self->execution_tree();
	}
	
	my $task;
	foreach my $hash (@$tree){
		if ($hash->{task_name} eq $task_name){
			$task = $hash;
		}
		else {
			# recurse
			$task = $self->get_task($task_name, $hash->{subtasks});
		}
		last if $task;
	}
	return $task;
}

1;