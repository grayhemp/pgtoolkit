# -*- mode: Perl; -*-
package PgToolkit::CompactorDatabaseTest;

use parent qw(PgToolkit::Test);

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
		$self->{'schema_compactor_mock_list'} = [];

		PgToolkit::Compactor::Database->new(
			database => $self->{'database'},
			logger => PgToolkit::Logger->new(
				level => 'info', err_handle => \*STDOUT),
			schema_compactor_constructor => sub {
				return $self->create_schema_compactor_mock(@_);
			},
			schema_name_list => [],
			excluded_schema_name_list => [],
			@_);
	};
}

sub create_schema_compactor_mock {
	my ($self, @arg_list) = @_;
	my %arg_hash = @arg_list;

	my $mock = Test::MockObject->new();
	$mock->set_true('init');
	$mock->set_true('process');
	$mock->set_false('-is_processed');
	$mock->set_always(
		'-get_ident',
		$self->{'database'}->quote_ident(string => $arg_hash{'schema_name'}));

	$mock->init(@arg_list);
	push(@{$self->{'schema_compactor_mock_list'}}, $mock);

	return $mock;
}

sub test_init_creates_schema_compactors : Test(12) {
	my $self = shift;

	$self->{'schema_name_list'} = [
		map(
			$_->[0],
			@{$self->{'database'}->{'mock'}->{'data_hash'}
			  ->{'get_schema_name_list'}->{'row_list'}})];

	my $data_hash_list = [
		{'arg' => {
			'schema_name_list' => $self->{'schema_name_list'},
			'excluded_schema_name_list' => []},
		 'expected' => $self->{'schema_name_list'}},
		{'arg' => {
			'schema_name_list' => [],
			'excluded_schema_name_list' => []},
		 'expected' => $self->{'schema_name_list'}},
		{'arg' => {
			'schema_name_list' => $self->{'schema_name_list'},
			'excluded_schema_name_list' => [$self->{'schema_name_list'}->[0]]},
		 'expected' => [$self->{'schema_name_list'}->[1]]},
		{'arg' => {
			'schema_name_list' => [],
			'excluded_schema_name_list' => [$self->{'schema_name_list'}->[1]]},
		 'expected' => [$self->{'schema_name_list'}->[0]]}];

	for my $data_hash (@{$data_hash_list}) {
		$self->{'database_compactor_constructor'}->(
			schema_name_list => $data_hash->{'arg'}->{'schema_name_list'},
			excluded_schema_name_list => (
				$data_hash->{'arg'}->{'excluded_schema_name_list'}));

		for my $i (0 .. @{$self->{'schema_compactor_mock_list'}} - 1) {
			my $mock = $self->{'schema_compactor_mock_list'}->[$i];
			is($mock->call_pos(1), 'init');
			is_deeply(
				[$mock->call_args(1)],
				[$mock, 'database' => $self->{'database'},
				 'schema_name' => $data_hash->{'expected'}->[$i],
				 'pgstattuple_schema_name' => 0]);
		}
	}
}

sub test_process_processes_schema_compactors : Test(2) {
	my $self = shift;

	$self->{'database_compactor_constructor'}->()->process();

	for my $i (0 .. @{$self->{'schema_compactor_mock_list'}} - 1) {
		is($self->{'schema_compactor_mock_list'}->[$i]->call_pos(2), 'process');
	}
}

sub test_creates_and_drops_clean_pages_stored_function : Test(4) {
	my $self = shift;

	{
		$self->{'database_compactor_constructor'}->();
		$self->{'database'}->{'mock'}->is_called(1, 'create_clean_pages');
	}
	$self->{'database'}->{'mock'}->is_called(4, 'drop_clean_pages');
}

sub test_init_skips_schema_if_cannot_create_its_compactor : Test {
	my $self = shift;

	my $try_count = 0;
	$self->{'database_compactor_constructor'}->(
		schema_compactor_constructor => sub {
			if ($try_count == 1) {
				die('SomeError');
			}
			$try_count++;
			return $self->create_schema_compactor_mock(@_);
		});

	is(@{$self->{'schema_compactor_mock_list'}}, 1);
}

sub test_init_passes_pgstattuple_schema_name_to_schema_constructor : Test(8) {
	my $self = shift;

	for my $pgstattuple_schema_name (undef, 'public') {
		$self->{'database'}->{'mock'}->{'data_hash'}
		->{'get_pgstattuple_schema_name'}
		->{'row_list'} =
			$pgstattuple_schema_name ? [[$pgstattuple_schema_name]] : [];

		$self->{'database_compactor_constructor'}->();

		for my $i (0 .. @{$self->{'schema_compactor_mock_list'}} - 1) {
			my $mock = $self->{'schema_compactor_mock_list'}->[$i];
			is($mock->call_pos(1), 'init');
			is({'self', $mock->call_args(1)}->{'pgstattuple_schema_name'},
			   $pgstattuple_schema_name);
		}
	}
}

1;
