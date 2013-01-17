# -*- mode: Perl; -*-
package PgToolkit::DatabasePsqlTest;

use base qw(PgToolkit::Test);

use strict;
use warnings;

use Test::More;
use Test::MockObject;
use Test::Exception;

use PgToolkit::Database::Psql;

sub setup : Test(setup) {
	my $self = shift;

	$self->{'database_constructor'} = sub {
		return PgToolkit::DatabasePsqlTest::Stub->new(
			path => 'psql', host => 'somehost', port => '5432',
			dbname => 'somedb', user => 'someuser', password => 'somepassword',
			@_);
	}
}

sub test_init : Test(5) {
	my $self = shift;

	my $db = $self->{'database_constructor'}->();

	is($db->{'psql_command_line'},
	   'PGPASSWORD=somepassword psql -w -q -A -t -X -h somehost -p 5432 '.
	   '-d somedb -U someuser -P null="<NULL>"');
	is($db->get_dbname(), 'somedb');

	$db = $self->{'database_constructor'}->(
		path => '/usr/bin/psql', host => 'anotherhost', port => '6432',
		dbname => 'anotherdb', user => 'anotheruser',
		password => 'anotherpassword');

	is($db->{'psql_command_line'},
	   'PGPASSWORD=anotherpassword /usr/bin/psql -w -q -A -t -X '.
	   '-h anotherhost -p 6432 -d anotherdb -U anotheruser -P null="<NULL>"');
	is($db->get_dbname(), 'anotherdb');

	is(PgToolkit::DatabasePsqlTest::Stub->new()
	   ->{'psql_command_line'},
	   'psql -w -q -A -t -X -P null="<NULL>"');
}

sub test_execute : Test(5) {
	my $self = shift;

	my $db = $self->{'database_constructor'}->();

	my $data_hash = {
		'SELECT 1 WHERE false;' => [],
		'SELECT 1;' => [[1]],
		'SELECT NULL;' => [[undef]],
		'SELECT 1, \'text\';' => [[1, 'text']],
		'SELECT column1, column2 '.
		'FROM (VALUES (1, \'text1\'), (2, \'text2\'))_;' => [
			[1, 'text1'], [2, 'text2']]
	};

	for my $sql (keys %{$data_hash}) {
		is_deeply($db->execute(sql => $sql), $data_hash->{$sql});
	}
}

sub test_set_parameters : Test(6) {
	my $self = shift;

	my $db = $self->{'database_constructor'}->();

	is($db->{'mock'}->call_pos(1),'_send_to_psql');
	is({'self', $db->{'mock'}->call_args(1)}->{'command'},
	   'SELECT 1;');

	$db = $self->{'database_constructor'}->(
		set_hash => {'statement_timeout' => 0});

	is($db->{'mock'}->call_pos(1),'_send_to_psql');
	is({'self', $db->{'mock'}->call_args(1)}->{'command'},
	   'SET statement_timeout TO 0; SELECT 1;');

	$db = $self->{'database_constructor'}->(
		set_hash => {
			'synchronous_commit' => '\'off\'',
			'vacuum_cost_delay' => 1});

	is($db->{'mock'}->call_pos(1),'_send_to_psql');
	is({'self', $db->{'mock'}->call_args(1)}->{'command'},
	   'SET synchronous_commit TO \'off\'; '.
	   'SET vacuum_cost_delay TO 1; SELECT 1;');
}

sub test_adapter_name : Test {
	my $self = shift;

	is($self->{'database_constructor'}->()->get_adapter_name(), 'psql');
}

1;

package PgToolkit::DatabasePsqlTest::Stub;

use base qw(PgToolkit::Database::Psql);

sub init {
	my ($self, %arg_hash) = @_;

	$self->{'mock'} = Test::MockObject->new();

	my $data_hash = {
		'SELECT 1 WHERE false;' => '',
		'SELECT 1;' => '1',
		'SELECT NULL;' => '<NULL>',
		'SELECT 1, \'text\';' => '1|text',
		('SELECT column1, column2 FROM (VALUES (1, \'text1\'), '.
		 '(2, \'text2\'))_;') => "1|text1\n2|text2",
		'SET statement_timeout TO 0; SELECT 1;' => '1',
		('SET synchronous_commit TO \'off\'; '.
		 'SET vacuum_cost_delay TO 1; SELECT 1;') => '1'};

	$self->{'mock'}->mock(
		'_send_to_psql',
		sub {
			my ($self, %arg_hash) = @_;

			return $data_hash->{$arg_hash{'command'}};
		});

	$self->SUPER::init(%arg_hash);

	return;
}

sub _start_psql {
	my ($self, %arg_hash) = @_;

	$self->{'psql_command_line'} = $arg_hash{'psql_command_line'};

	return;
}

sub _send_to_psql {
	my ($self, %arg_hash) = @_;

	return $self->{'mock'}->_send_to_psql(%arg_hash);
}

1;
