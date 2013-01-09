# -*- mode: Perl; -*-
package PgToolkit::DatabasePsqlTest;

use base qw(PgToolkit::Test);

use strict;
use warnings;

use Test::More;
use Test::Exception;

use PgToolkit::Database::Psql;

sub setup : Test(setup) {
	my $self = shift;

	$self->{'database_constructor'} = sub {
		return PgToolkit::DatabasePsqlTest::DatabasePsql->new(
			path => 'psql', host => 'somehost', port => '5432',
			dbname => 'somedb', user => 'someuser', password => 'somepassword',
			@_);
	}
}

sub test_init : Test(5) {
	my $self = shift;

	my $db = $self->{'database_constructor'}->();

	is($db->get_command(),
	   'PGPASSWORD=somepassword psql -w -q -A -t -X -h somehost -p 5432 '.
	   '-d somedb -U someuser -P null="<NULL>"');
	is($db->get_dbname(), 'somedb');

	$db = $self->{'database_constructor'}->(
		path => '/usr/bin/psql', host => 'anotherhost', port => '6432',
		dbname => 'anotherdb', user => 'anotheruser',
		password => 'anotherpassword');

	is($db->get_command(),
	   'PGPASSWORD=anotherpassword /usr/bin/psql -w -q -A -t -X '.
	   '-h anotherhost -p 6432 -d anotherdb -U anotheruser -P null="<NULL>"');
	is($db->get_dbname(), 'anotherdb');

	is(PgToolkit::DatabasePsqlTest::DatabasePsql->new()->get_command(),
	   'psql -w -q -A -t -X -P null="<NULL>"');
}

sub test_can_not_run : Test {
	my $self = shift;

	throws_ok(
		sub {
			PgToolkit::Database::Psql->new(
				psql => 'psql', host => 'localhost', port => '7432',
				dbname => 'test', user => 'test', password => '');
		},
		qr/DatabaseError Can not run psql:/);
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

sub test_set_parameters : Test(2) {
	my $self = shift;

	is_deeply(
		$self->{'database_constructor'}->(
			set_hash => {'statement_timeout' => 0}
		)->execute(sql => 'SELECT 10;'), [[10]]);

	is_deeply(
		$self->{'database_constructor'}->(
			set_hash => {
				'synchronous_commit' => '\'off\'',
				'vacuum_cost_delay' => 1}
		)->execute(sql => 'SELECT 20;'), [[20]]);
}

sub test_adapter_name : Test {
	my $self = shift;

	is($self->{'database_constructor'}->()->get_adapter_name(), 'psql');
}

1;

package PgToolkit::DatabasePsqlTest::DatabasePsql;

use base qw(PgToolkit::Database::Psql);

sub get_command {
	my $self  = shift;

	return $self->{'_command'};
}

sub _run_psql {
	my ($self, %arg) = @_;

	my $data_hash = {
		'SELECT 1 WHERE false;' => '',
		'SELECT 1;' => '1',
		'SELECT NULL;' => '<NULL>',
		'SELECT 1, \'text\';' => '1|text',
		('SELECT column1, column2 FROM (VALUES (1, \'text1\'), '.
		 '(2, \'text2\'))_;') => "1|text1\n2|text2",
		'SET statement_timeout TO 0; SELECT 1;' => '1',
		('SET synchronous_commit TO \'off\'; SET vacuum_cost_delay TO 1; '.
		 'SELECT 1;') => '1',
		'SET statement_timeout TO 0; SELECT 10;' => '10',
		('SET synchronous_commit TO \'off\'; SET vacuum_cost_delay TO 1; '.
		 'SELECT 20;') => '20'};

	return $data_hash->{$arg{'sql'}};
}

1;
