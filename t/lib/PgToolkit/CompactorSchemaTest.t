# -*- mode: Perl; -*-
package PgToolkit::CompactorSchemaTest;

use base qw(PgToolkit::Test);

use strict;
use warnings;

use Test::MockObject;
use Test::More;

use Test::Exception;

use PgToolkit::DatabaseStub;

use PgToolkit::Logger;

use PgToolkit::Compactor::Schema;

sub setup : Test(setup) {
	my $self = shift;

	$self->{'database'} = PgToolkit::DatabaseStub->new(dbname => 'dbname');

	$self->{'schema_compactor_constructor'} = sub {
		$self->{'table_compactor_mock_list'} = [];
		$self->{'table_compactor_mock_process_counter'} = 0;

		PgToolkit::Compactor::Schema->new(
			database => $self->{'database'},
			logger => PgToolkit::Logger->new(
				level => 'info', err_handle => \*STDOUT),
			schema_name => 'schema',
			table_compactor_constructor => sub {
				return $self->create_table_compactor_mock(@_);
			},
			table_name_list => [],
			excluded_table_name_list => [],
			pgstattuple_schema_name => undef,
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
		'dbname/schema.'.$self->{'database'}->quote_ident(
			string => $arg_hash{'table_name'}));

	$mock->init(@arg_list);
	push(@{$self->{'table_compactor_mock_list'}}, $mock);

	return $mock;
}

sub test_init_creates_table_compactors_in_returning_order : Test(16) {
	my $self = shift;

	my $table_name_list1 = [
		map(
			$_->[0],
			@{$self->{'database'}->{'mock'}->{'data_hash'}
			  ->{'get_table_name_list1'}->{'row_list'}})];
	my $table_name_list2 = [
		map(
			$_->[0],
			@{$self->{'database'}->{'mock'}->{'data_hash'}
			  ->{'get_table_name_list2'}->{'row_list'}})];

	my $data_hash_list = [
		{'arg' => {
			'table_name_list' => [reverse @{$table_name_list2}],
			'excluded_table_name_list' => []},
		 'expected' => $table_name_list2},
		{'arg' => {
			'table_name_list' => [],
			'excluded_table_name_list' => []},
		 'expected' => $table_name_list1},
		{'arg' => {
			'table_name_list' => [reverse @{$table_name_list2}],
			'excluded_table_name_list' => [$table_name_list2->[0]]},
		 'expected' => [$table_name_list2->[1]]},
		{'arg' => {
			'table_name_list' => [],
			'excluded_table_name_list' => [$table_name_list1->[1]]},
		 'expected' => [$table_name_list1->[0]]}];

	for my $data_hash (@{$data_hash_list}) {
		$self->{'schema_compactor_constructor'}->(
			table_name_list => $data_hash->{'arg'}->{'table_name_list'},
			excluded_table_name_list => (
				$data_hash->{'arg'}->{'excluded_table_name_list'}));

		is(@{$self->{'table_compactor_mock_list'}},
		   @{$data_hash->{'expected'}});
		for my $i (0 .. @{$self->{'table_compactor_mock_list'}} - 1) {
			my $mock = $self->{'table_compactor_mock_list'}->[$i];
			is($mock->call_pos(1), 'init');
			is_deeply(
				[$mock->call_args(1)],
				[$mock, 'database' => $self->{'database'},
				 'schema_name' => 'schema',
				 'table_name' => $data_hash->{'expected'}->[$i],
				 'pgstattuple_schema_name' => undef]);
		}
	}
}

sub test_process_procecces_table_compactors_in_their_order  : Test(4) {
	my $self = shift;

	$self->{'schema_compactor_constructor'}->()->process();

	for my $i (0 .. @{$self->{'table_compactor_mock_list'}} - 1) {
		is($self->{'table_compactor_mock_list'}->[$i]->call_pos(2), 'process');
		is($self->{'table_compactor_mock_list'}->[$i]->process_order(), $i);
	}
}

sub test_processing_status_depends_on_table_compactors : Test(4) {
	my $self = shift;

	my $schema_compactor = $self->{'schema_compactor_constructor'}->();

	for my $j (0 .. 3) {
		my $expected = [($j & 1) ? 1 : 0, ($j & 2) ? 1 : 0];

		$self->{'table_compactor_mock_list'}->[0]->set_always(
			'is_processed', $expected->[0]);
		$self->{'table_compactor_mock_list'}->[1]->set_always(
			'is_processed', $expected->[1]);

		is($schema_compactor->is_processed(), $expected->[0] & $expected->[1]);
	}
}

sub test_init_raises_error_when_no_schema : Test(1) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'has_schema'}
	->{'row_list'} = [[0]];

	throws_ok(
		sub { $self->{'schema_compactor_constructor'}->()->process(); },
		qr/SchemaCompactorError There is no schema schema\./);
}

sub test_init_skips_table_if_cannot_create_its_compactor : Test {
	my $self = shift;

	my $try_count = 0;
	$self->{'schema_compactor_constructor'}->(
		table_compactor_constructor => sub {
			if ($try_count == 1) {
				die('SomeError');
			}
			$try_count++;
			return $self->create_table_compactor_mock(@_);
		});

	is(@{$self->{'table_compactor_mock_list'}}, 1);
}

sub test_process_skips_table_if_cannot_process_it : Test(2) {
	my $self = shift;

	my $schema_compactor = $self->{'schema_compactor_constructor'}->();

	$self->{'table_compactor_mock_list'}->[0]->mock(
		'process', sub { die('SomeError'); });

	$schema_compactor->process();

	for my $i (0 .. @{$self->{'table_compactor_mock_list'}} - 1) {
		is($self->{'table_compactor_mock_list'}->[$i]->call_pos(2), 'process');
	}
}

sub test_init_transits_pgstattuple_schema_name_to_table_compactor : Test(8) {
	my $self = shift;

	for my $pgstattuple_schema_name (undef, 'public') {
		$self->{'schema_compactor_constructor'}->(
			pgstattuple_schema_name => $pgstattuple_schema_name);

		for my $i (0 .. @{$self->{'table_compactor_mock_list'}} - 1) {
			my $mock = $self->{'table_compactor_mock_list'}->[$i];
			is($mock->call_pos(1), 'init');
			is({'self', $mock->call_args(1)}->{'pgstattuple_schema_name'},
			   $pgstattuple_schema_name);
		}
	}
}

1;
