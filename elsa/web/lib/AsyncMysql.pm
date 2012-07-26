package AsyncMysql;
use Moose;
use Data::Dumper;
use DBI;
use AnyEvent;

has 'log' => ( is => 'ro', isa => 'Object', required => 1 );
has 'db_args' => (is => 'rw', isa => 'ArrayRef', required => 1);
has 'query_id' => (traits => ['Counter'], is => 'rw', isa => 'Num', required => 1, default => 1, handles => { next_id => 'inc' });
has 'watchers' => (is => 'rw', isa => 'HashRef', required => 1, default => sub { {} });

our $Retries = 3;

sub BUILD {
	my $self = shift;
	# Validate connection
	DBI->connect_cached(@{$self->db_args}) or die('cannot connect to ' . $self->db_args->[0]);
	return $self;
}

sub query {
	my $self = shift;
	my $query = shift;
	my $cb = shift;
	my @values = @_;
	
	
	my $attempts = 0;
	my $dbh;
	while ($attempts < $Retries){
		$attempts++;
		eval {
			$dbh = DBI->connect_cached(@{$self->db_args}) or die($DBI::errstr);
			$attempts = $Retries;
		};
		if ($@){
			$self->log->error('Got connection error ' . $@);
			undef $dbh;
			sleep 1;
		}
	}
	unless ($dbh){
		$self->log->error('Unable to make a connection after ' . $Retries . ' attempts');
		$cb->(undef, $@, 0);
		return;
	}
		
	eval {
		# Make sure RaiseError is enabled so we can catch problems in this eval block
		$dbh->{RaiseError} = 1;
		my $sth = $dbh->prepare($query, { async => 1 });
		$sth->execute(@values);
		my $id = $self->next_id;
		#$self->log->trace("Executing query $query with id $id");
		$self->watchers->{$id} = AnyEvent->io( fh => $dbh->mysql_fd, poll => 'r', cb => sub {
			my @rows;
			eval {
				while (my $row = $sth->fetchrow_hashref){
					push @rows, $row;
				}
				$cb->($dbh, \@rows, 1);
			};
			if ($@){
				$self->log->error($@);
			}
			#$self->log->trace("Got " . (scalar @rows) . " results for query $query");
			delete $self->watchers->{$id};
			return;
		});
	};
	if ($@){
		$self->log->error('Query: ' . $query . ' with values ' . join(',', @values) . ' got error ' . $@);
		$cb->(undef, $@, 0);
	}
}

sub multi_query {
	my $self = shift;
	my $query = shift;
	my $cb = shift;
	my @values = @_;
	
	
	my $attempts = 0;
	my $dbh;
	while ($attempts < $Retries){
		$attempts++;
		eval {
			$dbh = DBI->connect_cached(@{$self->db_args}) or die($DBI::errstr);
			$attempts = $Retries;
		};
		if ($@){
			$self->log->error('Got connection error ' . $@);
			undef $dbh;
			sleep 1;
		}
	}
	unless ($dbh){
		$self->log->error('Unable to make a connection after ' . $Retries . ' attempts');
		$cb->(undef, $@, 0);
		return;
	}
		
	eval {
		# Make sure RaiseError is enabled so we can catch problems in this eval block
		$dbh->{RaiseError} = 1;
		my $sth = $dbh->prepare($query, { async => 1 });
		$sth->execute(@values);
		my $id = $self->next_id;
		#$self->log->trace("Executing query $query with id $id");
		$self->watchers->{$id} = AnyEvent->io( fh => $dbh->mysql_fd, poll => 'r', cb => sub {
			my @rows;
			eval {
				do {
					while (my $row = $sth->fetchrow_hashref){
						push @rows, $row;
					}
				} while ($sth->more_results);
				$cb->(1, \@rows, 1);
			};
			if ($@){
				$self->log->error($@);
			}
			#$self->log->trace("Got " . (scalar @rows) . " results for query $query");
			delete $self->watchers->{$id};
			return;
		});
	};
	if ($@){
		$self->log->error('Query: ' . $query . ' with values ' . join(',', @values) . ' got error ' . $@);
		$cb->(undef, $@, 0);
	}
}

# Sphinx needs a special query procedure to deal with the SHOW META query summary being tacked on to a multi-query
sub sphinx {
	my $self = shift;
	my $query = shift;
	my $cb = shift;
	my @values = @_;
	
	my $attempts = 0;
	my $dbh;
	while ($attempts < $Retries){
		$attempts++;
		eval {
			$dbh = DBI->connect_cached(@{$self->db_args}) or die($DBI::errstr);
			$attempts = $Retries;
		};
		if ($@){
			$self->log->error('Got connection error ' . $@);
			undef $dbh;
			sleep 1;
		}
	}
	unless ($dbh){
		$self->log->error('Unable to make a connection after ' . $Retries . ' attempts');
		$cb->(undef, $@, 0);
		return;
	}
	
	eval {
		my $sth = $dbh->prepare($query, { async => 1 });
		$sth->execute(@values);
		my $id = $self->next_id;
		$self->log->trace("Executing query $query with id $id");
		$self->watchers->{$id} = AnyEvent->io( fh => $dbh->mysql_fd, poll => 'r', cb => sub {
			my @rows;
			my %meta;
			eval {
				do {
					while (my $row = $sth->fetchrow_hashref){
						# Is this a meta block row?
						if (exists $row->{Value} and exists $row->{Variable_name} and (scalar keys %$row) eq 2){
							next unless $row->{Variable_name};
							if ($row->{Value} =~ /^\d+(?:\.\d+)?$/){
								$meta{ $row->{Variable_name} } += $row->{Value};
							}
							else {
								$meta{ $row->{Variable_name} } = $row->{Value};
							}
						}
						else {
							push @rows, $row;
						}
					}
				} while ($sth->more_results);
				$cb->(1, { rows => \@rows, meta => \%meta }, 1);
			};
			if ($@){
				$self->log->error($@);
			}
			#$self->log->trace("Got " . (scalar @rows) . " results for query $query");
			delete $self->watchers->{$id};
			return;
		});
	};
	if ($@){
		$self->log->error('Query: ' . $query . ' got error ' . $@);
		$cb->(undef, $@, 0);
	}
}

1;