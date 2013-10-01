# -*- mode: Perl; -*-
package PgToolkit::CompactorDatabaseTest;

use base qw(PgToolkit::Test);

use strict;
use warnings;

use Test::MockObject;
use Test::More;

use Test::Exception;

use PgToolkit::DatabaseStub;

use PgToolkit::Logger;

use PgToolkit::Compactor::Database;

sub setup : Test(setup) {
	my $self = shift;

	$self->{'database'} = PgToolkit::DatabaseStub->new(dbname => 'dbname');

	$self->{'database_compactor_constructor'} = sub {
		$self->{'table_compactor_mock_list'} = [];
		$self->{'table_compactor_mock_process_counter'} = 0;

		PgToolkit::Compactor::Database->new(
			database => $self->{'database'},
			logger => PgToolkit::Logger->new(
				level => 'info', err_handle => \*STDOUT),
			dry_run => 0,
			table_compactor_constructor => sub {
				return $self->create_table_compactor_mock(@_);
			},
			schema_name_list => [],
			excluded_schema_name_list => [],
			table_name_list => [],
			excluded_table_name_list => [],
			no_pgstattuple => 0,
			system_catalog => 0,
			@_);
	};
}

sub create_table_compactor_mock {
	my ($self, @arg_list) = @_;
	my %arg_hash = @arg_list;

	my $mock = Test::MockObject->new();
	$mock->set_true('init');
	$mock->mock(
		'process',
		sub {
			$mock->set_always(
				'-process_order',
				$self->{'table_compactor_mock_process_counter'});
			$self->{'table_compactor_mock_process_counter'}++;

			return;
		});
	$mock->set_false('-is_processed');
	$mock->set_always(
		'-get_log_ident',
		'dbname, '.
		$self->{'database'}->quote_ident(
			string => $arg_hash{'schema_name'}).'.'.
		$self->{'database'}->quote_ident(
			string => $arg_hash{'table_name'}));
	$mock->set_always('-get_size_delta', int(rand() * 1000));
	$mock->set_always('-get_total_size_delta', int(rand() * 1000));

	$mock->init(@arg_list);
	push(@{$self->{'table_compactor_mock_list'}}, $mock);

	return $mock;
}

sub test_init_creates_table_compactors_in_the_returning_order : Test(20) {
	my $self = shift;

	my $schema_name_list = [
		map($_->[0],
			@{$self->{'database'}->{'mock'}->{'data_hash'}
			  ->{'get_table_data_list1'}->{'row_list'}})];

	my $table_name_list = [
		map($_->[1],
			@{$self->{'database'}->{'mock'}->{'data_hash'}
			  ->{'get_table_data_list1'}->{'row_list'}})];

	my $data_hash_list = [
		{'arg' => {
			'schema_name_list' => [],
			'excluded_schema_name_list' => [],
			'table_name_list' => [],
			'excluded_table_name_list' => []},
		 'expected' => [$schema_name_list, $table_name_list]},
		{'arg' => {
			'schema_name_list' => [@{$schema_name_list}[0, 2]],
			'excluded_schema_name_list' => [@{$schema_name_list}[2]],
			'table_name_list' => [],
			'excluded_table_name_list' => []},
		 'expected' => [[@{$schema_name_list}[0, 0]],
						[@{$table_name_list}[0, 1]]]},
		{'arg' => {
			'schema_name_list' => [],
			'excluded_schema_name_list' => [@{$schema_name_list}[0]],
			'table_name_list' => [@{$table_name_list}[0, 1]],
			'excluded_table_name_list' => []},
		 'expected' => [[@{$schema_name_list}[2, 2]],
						[@{$table_name_list}[0, 1]]]},
		{'arg' => {
			'schema_name_list' => [],
			'excluded_schema_name_list' => [],
			'table_name_list' => [@{$table_name_list}[0, 1]],
			'excluded_table_name_list' => [@{$table_name_list}[1]]},
		 'expected' => [[@{$schema_name_list}[0, 2]],
						[@{$table_name_list}[0, 0]]]}];

	for my $data_hash (@{$data_hash_list}) {
		$self->{'database_compactor_constructor'}->(
			schema_name_list => $data_hash->{'arg'}->{'schema_name_list'},
			excluded_schema_name_list => (
				$data_hash->{'arg'}->{'excluded_schema_name_list'}),
			table_name_list => $data_hash->{'arg'}->{'table_name_list'},
			excluded_table_name_list => (
				$data_hash->{'arg'}->{'excluded_table_name_list'}));

		for my $i (0 .. @{$self->{'table_compactor_mock_list'}} - 1) {
			my $mock = $self->{'table_compactor_mock_list'}->[$i];
			is($mock->call_pos(1), 'init');
			is_deeply(
				[$mock->call_args(1)],
				[$mock, 'database' => $self->{'database'},
				 'schema_name' => $data_hash->{'expected'}->[0]->[$i],
				 'table_name' => $data_hash->{'expected'}->[1]->[$i],
				 'pgstattuple_schema_name' => undef]);
		}
	}
}

sub test_process_system_catalog_if_specified : Test(2) {
	my $self = shift;

	$self->{'database_compactor_constructor'}->(
		schema_name_list => ['pg_catalog'],
		table_name_list => ['pg_class'],
		system_catalog => 1)->process();

	my $mock = $self->{'table_compactor_mock_list'}->[0];

	is($mock->call_pos(1), 'init');
	is_deeply(
		[$mock->call_args(1)],
		[$mock, 'database' => $self->{'database'},
		 'schema_name' => 'pg_catalog',
		 'table_name' => 'pg_class',
		 'pgstattuple_schema_name' => undef]);
}

sub test_process_procecces_table_compactors_in_their_order : Test(12) {
	my $self = shift;

	$self->{'database_compactor_constructor'}->()->process(attempt => 2);

	for my $i (0 .. @{$self->{'table_compactor_mock_list'}} - 1) {
		is($self->{'table_compactor_mock_list'}->[$i]->call_pos(2), 'process');
		is(
			{'self',
			 $self->{'table_compactor_mock_list'}->[$i]->call_args(2)
			}->{'attempt'}, 2);
		is($self->{'table_compactor_mock_list'}->[$i]->process_order(), $i);
	}
}

sub test_processing_status_depends_on_table_compactors : Test(4) {
	my $self = shift;

	my $schema_name_list = [
		map($_->[0],
			@{$self->{'database'}->{'mock'}->{'data_hash'}
			  ->{'get_table_data_list1'}->{'row_list'}})];

	my $database_compactor = $self->{'database_compactor_constructor'}->(
		'schema_name_list' => [@{$schema_name_list}[0, 2]],
		'excluded_schema_name_list' => [@{$schema_name_list}[2]]);

	for my $j (0 .. 3) {
		my $expected = [($j & 1) ? 1 : 0, ($j & 2) ? 1 : 0];

		$self->{'table_compactor_mock_list'}->[0]->set_always(
			'is_processed', $expected->[0]);
		$self->{'table_compactor_mock_list'}->[1]->set_always(
			'is_processed', $expected->[1]);

		is($database_compactor->is_processed(),
		   $expected->[0] & $expected->[1]);
	}
}

sub test_creates_and_drops_environment : Test(4) {
	my $self = shift;

	{
		$self->{'database_compactor_constructor'}->();
		$self->{'database'}->{'mock'}->is_called(1, 'create_clean_pages');
	}
	$self->{'database'}->{'mock'}->is_called(4, 'drop_clean_pages');
}

sub test_does_not_create_and_drop_environment_if_dry_run : Test(5) {
	my $self = shift;

	{
		$self->{'database_compactor_constructor'}->(dry_run => 1);
		$self->{'database'}->{'mock'}->is_called(
			1, 'get_pgstattuple_schema_name');
		$self->{'database'}->{'mock'}->is_called(
			2, 'get_table_data_list1');
	}
	$self->{'database'}->{'mock'}->is_called(3, undef);
}

sub test_init_passes_pgstattuple_schema_name_to_table_constructor : Test(16) {
	my $self = shift;

	for my $pgstattuple_schema_name (undef, 'public') {
		$self->{'database'}->{'mock'}->{'data_hash'}
		->{'get_pgstattuple_schema_name'}
		->{'row_list'} =
			$pgstattuple_schema_name ? [[$pgstattuple_schema_name]] : [];

		$self->{'database_compactor_constructor'}->();

		for my $i (0 .. @{$self->{'table_compactor_mock_list'}} - 1) {
			my $mock = $self->{'table_compactor_mock_list'}->[$i];
			is($mock->call_pos(1), 'init');
			is({'self', $mock->call_args(1)}->{'pgstattuple_schema_name'},
			   $pgstattuple_schema_name);
		}
	}
}

sub test_init_no_pgstatuple_passes_nothing_to_table_constructor : Test(8) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_pgstattuple_schema_name'}->{'row_list'} = [['public']];

	$self->{'database_compactor_constructor'}->(no_pgstatuple => 1);

	for my $i (0 .. @{$self->{'table_compactor_mock_list'}} - 1) {
		my $mock = $self->{'table_compactor_mock_list'}->[$i];
		is($mock->call_pos(1), 'init');
		is({'self', $mock->call_args(1)}->{'pgstattuple_schema_name'}, undef);
	}
}

sub test_get_size_delta : Test {
	my $self = shift;

	my $database_compactor = $self->{'database_compactor_constructor'}->();

	for my $table_compactor_mock (@{$self->{'table_compactor_mock_list'}}) {
		$table_compactor_mock->mock(
			'-is_processed',
			sub {
				shift->set_true('-is_processed');
				return 0;
			});
	}

	$database_compactor->process(attempt => 2);

	my $result = 0;
	map($result += $_->get_size_delta(),
		@{$self->{'table_compactor_mock_list'}});

	is($database_compactor->get_size_delta(), $result);
}

sub test_get_total_size_delta : Test {
	my $self = shift;

	my $database_compactor = $self->{'database_compactor_constructor'}->();

	for my $table_compactor_mock (@{$self->{'table_compactor_mock_list'}}) {
		$table_compactor_mock->mock(
			'-is_processed',
			sub {
				shift->set_true('-is_processed');
				return 0;
			});
	}

	$database_compactor->process(attempt => 2);

	my $result = 0;
	map($result += $_->get_total_size_delta(),
		@{$self->{'table_compactor_mock_list'}});

	is($database_compactor->get_total_size_delta(), $result);
}

1;
