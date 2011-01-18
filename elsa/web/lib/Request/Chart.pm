package Request::Chart;
use strict;
use warnings;
use Data::Dumper;
use base qw( CGI::Application::Dispatch CGI::Application Request);
use JSON -convert_blessed_universally;
use Apache2::Request;
use CGI::Application::Plugin::Apache;
use Apache2::Const qw(:http);
use APR::Request::Param;
use MIME::Base64;
use URI::Escape;

our %Types = (
	'Pie' => 1,
	'Bar' => 1,
	'Grid' => 1,
	'HBar' => 1,
	'Line' => 1,
	'Scatter' => 1,
);

sub run {
	my $self = shift;
	my $r = $self->init_request(1); # no_auth = 1
	unless ($r){
		$self->query()->header( -type => 'application/javascript' );
		print $self->json->encode( { error => 'No value returned.' } );
		return HTTP_INTERNAL_SERVER_ERROR;
	}
	$self->log->debug('param: ' . Dumper($self->{_QUERY_PARAMS}));
	my $method = 'sql';
	$method = $self->param('method') if $self->param('method');
	$self->log->debug('method: ' . $method);
	
	if ($method eq 'sql'){
		my $ret = $self->_get_data_from_sql($self->{_QUERY_PARAMS});
		$self->query()->header( -type => 'application/javascript' );
		print $self->json->encode($ret);
		return HTTP_OK;
	}
	elsif ($method eq 'save'){
		if ($self->{_QUERY_PARAMS}->{base64_image_data}){
			# Write this to a file so it can be spit back out
			$self->session->param('chart_img', $self->{_QUERY_PARAMS}->{base64_image_data});
			$self->query()->header( -type => 'application/javascript' );
			print $self->json->encode({ok => 1});
			return HTTP_OK;
		}
		else {
			print $self->query()->header( -type => 'application/javascript' );
			print $self->json->encode( { error => 'No value returned.' } );
			return HTTP_INTERNAL_SERVER_ERROR;
		}
	}
	elsif ($method eq 'show'){
		if ($self->session->param('chart_img')){
			print $self->query()->header( -type => 'image/png' );
			print decode_base64($self->session->param('chart_img'));
			return HTTP_OK;
		}
		else {
			print $self->query()->header( -type => 'application/javascript' );
			print $self->json->encode( { error => 'No value returned.' } );
			return HTTP_INTERNAL_SERVER_ERROR;
		}
	}
	elsif ($method eq 'json'){
		my $json;
		eval {
			$json = $self->json->decode($self->{_QUERY_PARAMS}->{data});
		};
		if ($@){
			print $self->json->encode( { error => $@ } );
			return HTTP_INTERNAL_SERVER_ERROR;
		}
		unless ($json and ref($json) eq 'HASH' and $json->{data} and ref($json->{data}) eq 'ARRAY'
			and $json->{func}){
			print $self->json->encode( { error => 'Invalid JSON object received: ' . Dumper($json) } );
			return HTTP_INTERNAL_SERVER_ERROR;
		}
		my $ret = $self->_get_data($json);
		$self->query()->header( -type => 'application/javascript' );
		print $self->json->encode($ret);
		return HTTP_OK;
	}
	
	$self->query()->header( -type => 'application/javascript' );
	print $self->json->encode( { error => 'No value returned.' } );
	return HTTP_INTERNAL_SERVER_ERROR;
}

sub _get_data_from_sql {
	my $self = shift;
	my $params = shift;
	$self->log->debug('params: ' . Dumper($params));
	$params->{db_driver} = 'mysql' unless $params->{db_driver};
	my %hash;
	eval {
		my $dsn = sprintf('dbi:%s:database=%s;host=%s;', $params->{db_driver}, $params->{db}, $params->{host});
		my $dbh = DBI->connect($dsn, $params->{user}, $params->{pass}, { RaiseError => 1}) or die($DBI::errstr);
		my $sth = $dbh->prepare(uri_unescape($params->{sql}));
		$sth->execute();
		while (my $row = $sth->fetchrow_hashref){
			foreach my $col (keys %$row){
				$hash{$col} ||= [];
				push @{ $hash{$col} }, $row->{$col};
			}
		}
		$self->log->debug('hash: ' . Dumper(\%hash));
	};
	if ($@){
		$self->log->error($@);
	}
	return \%hash;
}

sub _get_data {
	my $self = shift;
	my $args = shift;
	$self->log->debug('args: ' . Dumper($args));
	
	my %hash;
	my $field;
	foreach my $row (@{ $args->{data} }){
		foreach my $col (keys %$row){
			$field ||= $col;
			if ($args->{func} eq 'SUM'){
				$hash{$col} += $row->{$col};
			}
			else { #COUNT
				$hash{ $row->{$col} }++;
			}
		}
	}
		
	return {
		x => [ keys %hash ],
		$field => [ values %hash ],
	}
}


1;

__END__

