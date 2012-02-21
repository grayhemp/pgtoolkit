package PgToolkit::DbiStub;

use strict;
use warnings;

use Test::MockObject;
use Test::MockObject::Extends;

sub mock_sth {
	my $sql = shift;

	my $sth_mock = Test::MockObject->new();

	$sth_mock->mock(
		'execute',
		sub {
			my $self = shift;

			$self->set_always('value_list', [@_]);

			return;
		});

	$sth_mock->mock(
		'fetchall_arrayref',
		sub {
			my $self = shift;

			my $return_list = [];

			if ($sql eq 'SELECT 1 WHERE false;') {
				# empty
			}

			if ($sql eq 'SET statement_timeout TO 0;') {
				# empty
			}

			if ($sql eq ('SET synchronous_commit TO \'off\'; '.
						 'SET vacuum_cost_delay TO 1;'))
			{
				# empty
			}

			if ($sql eq 'SELECT 1;') {
				push(@{$return_list}, [1]);
			}

			if ($sql eq 'SELECT 1, \'text\';') {
				push(@{$return_list}, [1, 'text']);
			}

			if ($sql eq ('SELECT column1, column2 '.
						 'FROM (VALUES (1, \'text1\'), (2, \'text2\'))_;'))
			{
				for (my $i = 1; $i <= 2; $i++) {
					push(@{$return_list}, [$i, 'text'.$i]);
				}
			}

			return $return_list;
		});

	return $sth_mock;
}

sub mock_dbh {
	my @args = @_;

	my $dbh_mock = Test::MockObject::Extends->new('DBI');

	$dbh_mock->mock('connect', sub { return shift; });

	$dbh_mock->mock(
		'prepare',
		sub {
			shift;

			return mock_sth(@_);
		});

	$dbh_mock->mock(
		'do',
		sub {
			shift;

			return mock_sth(@_)->execute(@_);
		});

	$dbh_mock->mock(
		'available_drivers',
		sub {
			return ('Pg', 'somepg', 'anotherpg');
		});

	return $dbh_mock->connect(@args);
}

sub main {
	Test::MockObject::Extends->new('DBI')->fake_module(
		'DBI',
		'connect' => sub { return mock_dbh(@_); },
		'available_drivers' => sub { return ('Pg', 'somepg', 'anotherpg'); });
}

main();

1;
