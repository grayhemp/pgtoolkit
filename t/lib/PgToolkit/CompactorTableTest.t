# -*- mode: Perl; -*-
package PgToolkit::CompactorTableTest;

use base qw(PgToolkit::Test);

use strict;
use warnings;

use Test::MockObject;
use Test::More;
use Test::Exception;

use PgToolkit::DatabaseStub;

use PgToolkit::Logger;

sub setup : Test(setup) {
	my $self = shift;

	$self->{'database'} = PgToolkit::DatabaseStub->new(dbname => 'dbname');

	$self->{'logger'} = PgToolkit::Logger->new(
		level => 'info', err_handle => \*STDOUT),

	$self->{'table_compactor_constructor'} = sub {
		$self->{'toast_compactor_mock'} = undef;

		PgToolkit::Compactor::TableStub->new(
			database => $self->{'database'},
			logger => $self->{'logger'},
			dry_run => 0,
			toast_compactor_constructor => sub {
				return $self->create_toast_compactor_mock(@_);
			},
			schema_name => 'schema',
			table_name => 'table',
			min_page_count => 100,
			min_free_percent => 15,
			max_pages_per_round => 5,
			no_initial_vacuum => 0,
			no_routine_vacuum => 0,
			no_final_analyze => 0,
			delay_constant => 1,
			delay_ratio => 0.5,
			force => 0,
			reindex => 0,
			print_reindex_queries => 0,
			progress_report_period => 3,
			pgstattuple_schema_name => undef,
			pages_per_round_divisor => 1,
			pages_before_vacuum_lower_divisor => 16,
			pages_before_vacuum_lower_threshold => 1000,
			pages_before_vacuum_upper_divisor => 50,
			max_retry_count => 2,
			locked_alter_timeout => 500,
			locked_alter_count => 3,
			@_);
	};
}

sub create_toast_compactor_mock {
	my ($self, @arg_list) = @_;
	my %arg_hash = @arg_list;

	my $mock = Test::MockObject->new();

	$mock->set_true('process');

	$mock->mock(
		'-is_called',
		sub {
			my (undef, $pos, $name, @arg_list) = @_;

			is($mock->call_pos($pos), $name);
			is_deeply([$mock->call_args($pos)], [$mock, @arg_list]);

			return;
		});

	$mock->mock(
		'init',
		sub {
			my (undef, %arg_hash) = @_;

			$self->{'logger'}->write(
				message => 'Toast processing mock.',
				level => 'notice',
				target => (
					$arg_hash{'schema_name'}.'.'.
					$arg_hash{'table_name'}));

			return $mock;
		});

	$mock->init(@arg_list);
	$self->{'toast_compactor_mock'} = $mock;

	return $mock;
}

sub test_dry_run : Test(20) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		dry_run => 1,
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 2;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'try_advisory_lock_table');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'has_special_triggers');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_check_special_triggers : Test(16) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'has_special_triggers'}->
	{'row_list'} = [[1]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 2;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'try_advisory_lock_table');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'has_special_triggers');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_no_initial_vacuum : Test(8) {
	my $self = shift;

	splice(
		@{$self->{'database'}->{'mock'}->{'data_hash'}
		  ->{'get_size_statistics'}->{'row_list_sequence'}},
		1, 1);

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		no_initial_vacuum => 1);

	$table_compactor->process(attempt => 1);

	my $i = 2;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'try_advisory_lock_table');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'has_special_triggers');
}

sub test_analyze_if_not_analyzed : Test(17) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'} = [
		[[undef, undef, undef]],
		@{$self->{'database'}->{'mock'}->{'data_hash'}
		  ->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}}];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 2;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'try_advisory_lock_table');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'has_special_triggers');
	ok($table_compactor->is_processed());
}

sub test_skip_processing_if_cant_try_advisory_lock_table : Test(6) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'try_advisory_lock_table'}->{'row_list'} = [[0]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 2;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'try_advisory_lock_table');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_skip_processing_if_table_is_empty : Test(12) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[1] =
		[[0, 0, 0, 0]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 2;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'try_advisory_lock_table');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_min_page_count : Test(16) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[1] =
		[[35000, 42000, 99, 120]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 2;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'try_advisory_lock_table');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'has_special_triggers');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_min_free_percent : Test(16) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[0] =
		[[85, 14, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 2;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'try_advisory_lock_table');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'has_special_triggers');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_skip_after_size_stats_if_toast_and_approximate : Test(10) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		schema_name => 'pg_toast',
		toast_parent_ident => 'schema.table');

	$table_compactor->process(attempt => 1);

	my $i = 2;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'try_advisory_lock_table');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_skip_after_bloat_stats_if_toast_and_pgstattuple : Test(12) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		schema_name => 'pg_toast',
		toast_parent_ident => 'schema.table',
		pgstattuple_schema_name => 'public');

	$table_compactor->process(attempt => 1);

	my $i = 2;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'try_advisory_lock_table');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_pgstattuple_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_skip_after_size_stats_if_toast_and_approximate_forced : Test(10) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		schema_name => 'pg_toast',
		toast_parent_ident => 'schema.table',
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 2;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'try_advisory_lock_table');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_skip_after_bloat_stats_if_toast_and_pgstattuple_forced : Test(12) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		schema_name => 'pg_toast',
		toast_parent_ident => 'schema.table',
		pgstattuple_schema_name => 'public',
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 2;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'try_advisory_lock_table');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_pgstattuple_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_force_processing : Test(14) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[1] =
		[[35000, 42000, 99, 120]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[2] =
		[[35000, 42000, 92, 108]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[0] =
		[[85, 14, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 2;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'try_advisory_lock_table');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'has_special_triggers');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_column');
}

sub test_can_not_get_bloat_statistics : Test(18) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'} = [
		[[undef, undef, undef]],
		[[undef, undef, undef]]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 2;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'try_advisory_lock_table');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_can_not_get_size_statistics : Test(6) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'} = [
		[[undef, undef, undef]]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 2;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'try_advisory_lock_table');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_can_not_get_update_column : Test(4) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_column'}->{'row_list'} = [[undef]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 8;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_column');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_can_not_get_max_tupples_per_page : Test(6) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_max_tupples_per_page'}->{'row_list'} = [[undef]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 8;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_column');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_max_tupples_per_page');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_main_processing : Test(36) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[2] =
		[[35000, 42000, 88, 108]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 8;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_column');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_max_tupples_per_page');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 99);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'commit');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 94);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'commit');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 87);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'commit');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 84);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rollback');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
}

sub test_finish_when_0_pages_returned : Test(16) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'clean_pages'}->{'row_list_sequence'}->[0] =
		[[0]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 8;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_column');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_max_tupples_per_page');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 99);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'commit');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
}

sub test_pages_per_round_not_more_then_to_page : Test(36) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'clean_pages'}->{'row_list_sequence'}->[1] =
		[[10]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'clean_pages'}->{'row_list_sequence'}->[2] =
		[[5]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		max_pages_per_round => 200);

	$table_compactor->process(attempt => 1);

	my $i = 8;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_column');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_max_tupples_per_page');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 99, pages_per_round => 99);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'commit');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 94, pages_per_round => 94);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'commit');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 10, pages_per_round => 10);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'commit');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 5, pages_per_round => 5);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rollback');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
}

sub test_cleaned_during_processing : Test(22) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[2] =
		[[0, 0, 0, 100]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[0, 0, 0, 1]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 8;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_column');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_max_tupples_per_page');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 99);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'commit');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 94);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'commit');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
}

sub test_main_processing_no_routine_vacuum : Test(32) {
	my $self = shift;

	splice(
		@{$self->{'database'}->{'mock'}->{'data_hash'}
		  ->{'get_size_statistics'}->{'row_list_sequence'}},
		2, 1);

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		no_routine_vacuum => 1);

	$table_compactor->process(attempt => 1);

	my $i = 8;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_column');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_max_tupples_per_page');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 99);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'commit');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 94);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'commit');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 89);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'commit');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 84);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rollback');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
}

sub test_can_not_get_index_size_statistics : Test(12) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_size_statistics'}->{'row_list_sequence'} = [
		[[undef, undef]]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_can_not_get_index_bloat_statistics : Test(14) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_bloat_statistics'}->{'row_list_sequence'} = [
		[[undef, undef]]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public',
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_pgstattuple_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_reindex : Test(61) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_acquired_lock_after_several_attempts : Test(78) {
	my $self = shift;

	unshift(
		$self->{'database'}->{'mock'}->{'data_hash'}
		->{'drop_index2'}->{'row_list_sequence'},
		'canceling statement due to statement timeout',
		'canceling statement due to statement timeout');

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	for (my $j = 0; $j < 3; $j++) {
		$self->{'database'}->{'mock'}->is_called(
			$i++, 'begin');
		$self->{'database'}->{'mock'}->is_called(
			$i++, 'set_local_statement_timeout');
		$self->{'database'}->{'mock'}->is_called(
			$i++, 'drop_index2');
		if ($j < 2) {
			$self->{'database'}->{'mock'}->is_called(
				$i++, 'end');
		}
	}
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);

	ok($table_compactor->is_processed());
}

sub test_reindex_didnt_acquire_lock_when_91 : Test(74) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_major_version'}->{'row_list'}->[0][0] = '9.1';

	splice(
		$self->{'database'}->{'mock'}->{'data_hash'}
		->{'get_index_size_statistics'}->{'row_list_sequence'},
		3, 1);

	unshift(
		$self->{'database'}->{'mock'}->{'data_hash'}
		->{'drop_index2'}->{'row_list_sequence'},
		'canceling statement due to statement timeout',
		'canceling statement due to statement timeout',
		'canceling statement due to statement timeout');

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	for (my $j = 0; $j < 3; $j++) {
		$self->{'database'}->{'mock'}->is_called(
			$i++, 'begin');
		$self->{'database'}->{'mock'}->is_called(
			$i++, 'set_local_statement_timeout');
		$self->{'database'}->{'mock'}->is_called(
			$i++, 'drop_index2');
		$self->{'database'}->{'mock'}->is_called(
			$i++, 'end');
	}
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_temp_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);

	ok(not $table_compactor->is_processed());
}

sub test_reindex_didnt_acquire_lock_when_92 : Test(74) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_major_version'}->{'row_list'}->[0][0] = '9.2';

	splice(
		$self->{'database'}->{'mock'}->{'data_hash'}
		->{'get_index_size_statistics'}->{'row_list_sequence'},
		3, 1);

	unshift(
		$self->{'database'}->{'mock'}->{'data_hash'}
		->{'swap_index_names2'}->{'row_list_sequence'},
		'canceling statement due to statement timeout',
		'canceling statement due to statement timeout',
		'canceling statement due to statement timeout');

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	for (my $j = 0; $j < 3; $j++) {
		$self->{'database'}->{'mock'}->is_called(
			$i++, 'begin');
		$self->{'database'}->{'mock'}->is_called(
			$i++, 'set_local_statement_timeout');
		$self->{'database'}->{'mock'}->is_called(
			$i++, 'swap_index_names2');
		$self->{'database'}->{'mock'}->is_called(
			$i++, 'end');
	}
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_temp_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'swap_index_names3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_temp_index_concurrently3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);

	ok(not $table_compactor->is_processed());
}

sub test_reindex_if_last_attempt_and_not_processed : Test(61) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[5] =
		[[35000, 42000, 92, 110]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[6] =
		[[35000, 42000, 92, 110]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[4] =
		[[85, 7, 1500]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[5] =
		[[85, 7, 1500]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 2);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_if_not_last_attempt_and_processed : Test(61) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_if_not_last_attempt_and_not_processed : Test(7) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[2] =
		[[35000, 42000, 100, 120]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 100, 120]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[1] =
		[[85, 15, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_queries_if_last_attempt_and_not_processed : Test(17) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 92, 110]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[4] =
		[[35000, 42000, 92, 110]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[2] =
		[[85, 7, 1500]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[3] =
		[[85, 7, 1500]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 2);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_queries_if_not_last_attempt_and_processed : Test(17) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_queries_if_not_last_attempt_and_not_processed : Test(7) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[2] =
		[[35000, 42000, 100, 120]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 100, 120]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[1] =
		[[85, 15, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_if_in_min_free_percent : Test(53) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_bloat_statistics'}->{'row_list_sequence'}->[2] = [[14, 75]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public',
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_pgstattuple_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_if_in_min_free_percent_and_forced : Test(61) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_bloat_statistics'}->{'row_list_sequence'}->[2] = [[14, 75]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public',
		reindex => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_pgstattuple_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_queries_if_in_min_free_percent : Test(23) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_bloat_statistics'}->{'row_list_sequence'}->[2] = [[14, 75]];


	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public',
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_pgstattuple_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_queries_if_in_min_free_percent_and_forced : Test(17) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_bloat_statistics'}->{'row_list_sequence'}->[2] = [[14, 75]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public',
		print_reindex_queries => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_pgstattuple_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_if_in_min_page_count : Test(47) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_size_statistics'}->{'row_list_sequence'}->[4] = [[495, 99]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_if_in_min_page_count_and_forced : Test(61) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_size_statistics'}->{'row_list_sequence'}->[4] = [[495, 99]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_queries_if_in_min_page_count : Test(23) {
	my $self = shift;

	splice(
		$self->{'database'}->{'mock'}->{'data_hash'}
		->{'get_index_size_statistics'}->{'row_list_sequence'},
		4, 2, [[495, 99]]);

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public',
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_pgstattuple_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_queries_if_in_min_page_count_and_forced : Test(17) {
	my $self = shift;

	splice(
		$self->{'database'}->{'mock'}->{'data_hash'}
		->{'get_index_size_statistics'}->{'row_list_sequence'},
		3, 3, [[495, 99]]);

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public',
		print_reindex_queries => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_pgstattuple_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_if_table_skipped_and_pgstatuple : Test(74) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[0] =
		[[85, 14, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public',
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 2;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'try_advisory_lock_table');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_pgstattuple_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'has_special_triggers');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_reindex_queries_if_table_skipped_and_pgstatuple : Test(30) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[0] =
		[[85, 14, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public',
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 1);

	my $i = 2;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'try_advisory_lock_table');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_pgstattuple_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'has_special_triggers');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_reindex_if_not_processed_and_will_be_skipped : Test(62) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[2] =
		[[35000, 42000, 99, 120]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 99, 120]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[4] =
		[[35000, 42000, 99, 120]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[2] =
		[[85, 14, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_reindex_queries_if_not_processed_and_will_be_skipped : Test(18) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[2] =
		[[35000, 42000, 99, 120]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 99, 120]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[4] =
		[[35000, 42000, 99, 120]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[2] =
		[[85, 14, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_no_reindex_if_index_is_empty_and_forced : Test(47) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_size_statistics'}->{'row_list_sequence'}->[4] = [[0, 0]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_queries_if_index_is_empty_and_forced : Test(17) {
	my $self = shift;

	splice(
		$self->{'database'}->{'mock'}->{'data_hash'}
		->{'get_index_size_statistics'}->{'row_list_sequence'},
		3, 3, [[0, 0]]);

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_if_not_btree : Test(47) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_data_list'}->{'row_list'}->[2][3] = 'gist';

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_queries_if_not_btree : Test(17) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_data_list'}->{'row_list'}->[2][5] = 'gist';

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_if_not_btree_and_forced : Test(61) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_data_list'}->{'row_list'}->[2][5] = 'gist';

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_queries_if_not_btree_and_forced : Test(17) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_data_list'}->{'row_list'}->[2][5] = 'gist';

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_if_not_allowed : Test(47) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_data_list'}->{'row_list'}->[2][6] = 0;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_queries_if_not_allowed : Test(17) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_data_list'}->{'row_list'}->[2][6] = 0;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_if_not_allowed_and_forced : Test(47) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_data_list'}->{'row_list'}->[2][6] = 0;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_queries_if_not_allowed_and_forced : Test(17) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_data_list'}->{'row_list'}->[2][6] = 0;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_drop_index_concurrently_when_92 : Test(61) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_major_version'}->{'row_list'}->[0][0] = '9.2';

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'swap_index_names2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_temp_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'swap_index_names3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_temp_index_concurrently3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_queries_drop_index_concurrently_when_92 : Test(17) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_major_version'}->{'row_list'}->[0][0] = '9.2';

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_drop_index_not_concurrently_when_91 : Test(61) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_major_version'}->{'row_list'}->[0][0] = '9.1';

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'add_constraint1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'create_index_concurrently3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'set_local_statement_timeout');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'drop_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rename_temp_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'end');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_queries_drop_index_not_concurrently_when_91 : Test(17) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_major_version'}->{'row_list'}->[0][0] = '9.1';

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_data_list');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_loops_count : Test(8) {
	my $self = shift;

	splice(@{$self->{'database'}->{'mock'}->{'data_hash'}->{'clean_pages'}->
			 {'row_list_sequence'}}, 3, 1);

	for (my $i = 0; $i < 97; $i++) {
		push(@{$self->{'database'}->{'mock'}->{'data_hash'}->{'clean_pages'}->
			   {'row_list_sequence'}},
			 [[84]]);
	}

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 309;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 84);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'commit');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
}

sub test_processed : Test {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[2] =
		[[35000, 42000, 91, 108]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 91, 108]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[1] =
		[[85, 6, 1400]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		min_page_count => 92,
		min_free_percent => 7);

	$table_compactor->process(attempt => 1);

	ok($table_compactor->is_processed());
}

sub test_not_processed : Test {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[2] =
		[[35000, 42000, 92, 110]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 92, 110]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[1] =
		[[85, 7, 1500]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		min_page_count => 92,
		min_free_percent => 7);

	$table_compactor->process(attempt => 1);

	ok(not $table_compactor->is_processed());
}

sub test_not_processed_and_last_attempt : Test {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[2] =
		[[35000, 42000, 100, 120]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 35000, 100, 100]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[4] =
		[[35000, 35000, 100, 100]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[1] =
		[[85, 10, 1200]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[2] =
		[[85, 10, 1200]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		min_free_percent => 10);

	$table_compactor->process(attempt => 2);

	ok(not $table_compactor->is_processed());
}

sub test_processed_if_in_min_page_count : Test {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 99, 118]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[4] =
		[[35000, 42000, 99, 118]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[2] =
		[[85, 15, 1500]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[3] =
		[[85, 15, 1500]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	ok($table_compactor->is_processed());
}

sub test_processed_if_in_min_free_percent : Test {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 100, 118]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[4] =
		[[35000, 42000, 100, 118]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[2] =
		[[85, 14, 1500]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[3] =
		[[85, 14, 1500]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	ok($table_compactor->is_processed());
}

sub test_not_processed_if_in_base_restrictions_and_forced : Test {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 99, 118]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[4] =
		[[35000, 42000, 99, 118]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[2] =
		[[85, 14, 1500]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[3] =
		[[85, 14, 1500]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		force => 1);

	$table_compactor->process(attempt => 1);

	ok(not $table_compactor->is_processed());
}

sub test_get_pgstattuple_bloat_statistics : Test(2) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public');

	$table_compactor->process(attempt => 1);

	my $i = 6;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_pgstattuple_bloat_statistics');
}

sub test_no_final_analyze : Test(7) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		no_final_analyze => 1);

	$table_compactor->process(attempt => 1);

	my $i = 25;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_process_toast_if_not_last_attempt_and_processed : Test(10) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_toast_table_name'}->{'row_list'} = [['pg_toast_12345']];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 27;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);

	$i = 1;

	$self->{'toast_compactor_mock'}->is_called(
		$i++, 'init',
		'schema_name' => 'pg_toast', 'table_name' => 'pg_toast_12345',
		'toast_parent_ident' => 'schema.table');
	$self->{'toast_compactor_mock'}->is_called(
		$i++, 'process', 'attempt' => 1);

	ok($table_compactor->is_processed());
}

sub test_dont_process_toast_if_not_last_attempt_and_not_processed : Test(5) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[2] =
		[[35000, 42000, 92, 110]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 92, 110]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[1] =
		[[85, 7, 1500]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_toast_table_name'}->{'row_list'} = [['pg_toast_12345']];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		min_page_count => 92,
		min_free_percent => 7);

	$table_compactor->process(attempt => 1);

	my $i = 27;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);

	is($self->{'toast_compactor_mock'}, undef);

	ok(not $table_compactor->is_processed());
}

sub test_process_toast_if_last_attempt_and_not_processed : Test(10) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[2] =
		[[35000, 42000, 92, 110]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 92, 110]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[1] =
		[[85, 7, 1500]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_toast_table_name'}->{'row_list'} = [['pg_toast_12345']];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		min_page_count => 92,
		min_free_percent => 7);

	$table_compactor->process(attempt => 2);

	my $i = 27;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);

	$i = 1;

	$self->{'toast_compactor_mock'}->is_called(
		$i++, 'init',
		'schema_name' => 'pg_toast', 'table_name' => 'pg_toast_12345',
		'toast_parent_ident' => 'schema.table');
	$self->{'toast_compactor_mock'}->is_called(
		$i++, 'process', 'attempt' => 1);

	ok(not $table_compactor->is_processed());
}

sub test_process_toast_if_last_attempt_and_processed : Test(10) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_toast_table_name'}->{'row_list'} = [['pg_toast_12345']];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 2);

	my $i = 27;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_toast_table_name');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);

	$i = 1;

	$self->{'toast_compactor_mock'}->is_called(
		$i++, 'init',
		'schema_name' => 'pg_toast', 'table_name' => 'pg_toast_12345',
		'toast_parent_ident' => 'schema.table');
	$self->{'toast_compactor_mock'}->is_called(
		$i++, 'process', 'attempt' => 1);

	ok($table_compactor->is_processed());
}

sub test_continue_processing_on_deadlock_detected : Test(12) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'clean_pages'}
	->{'row_list_sequence'}->[0] = 'deadlock detected';

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 10;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 99);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rollback');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 99);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'commit');
}

sub test_stop_processing_on_cannot_extract_system_attribute : Test(11) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'clean_pages'}->
	{'row_list_sequence'}->[0] = 'cannot extract system attribute';

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[2] =
		[[35000, 42000, 100, 120]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[1] =
		[[85, 15, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 10;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'begin');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'clean_pages', to_page => 99);
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'rollback');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	ok(not $table_compactor->is_processed());
}

sub test_stop_processing_on_relation_does_not_exist : Test(6) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'vacuum'}
	->{'row_list'} = 'relation "schema.table" does not exist';

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 4;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
	is($table_compactor->get_size_delta(), 0);
	is($table_compactor->get_total_size_delta(), 0);
}

sub test_get_size_delta : Test {
	my $self = shift;

	my $size = (
		$self->{'database'}->{'mock'}->{'data_hash'}->{'get_size_statistics'}->
		{'row_list_sequence'}->[0]->[0]->[0] -
		$self->{'database'}->{'mock'}->{'data_hash'}->{'get_size_statistics'}->
		{'row_list_sequence'}->[4]->[0]->[0]);

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	is($table_compactor->get_size_delta(), $size);
}

sub test_get_total_size_delta : Test {
	my $self = shift;

	my $size = (
		$self->{'database'}->{'mock'}->{'data_hash'}->{'get_size_statistics'}->
		{'row_list_sequence'}->[0]->[0]->[1] -
		$self->{'database'}->{'mock'}->{'data_hash'}->{'get_size_statistics'}->
		{'row_list_sequence'}->[4]->[0]->[1]);

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	is($table_compactor->get_total_size_delta(), $size);
}

sub test_delay_and_proggress_1 : Test(12) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 1;

	$table_compactor->{'mock'}->is_called(
		$i++, 'sleep', 1.5);
	$table_compactor->{'mock'}->is_called(
		$i++, 'log_progress');
	$table_compactor->{'mock'}->is_called(
		$i++, 'sleep', 1.5);
	$table_compactor->{'mock'}->is_called(
		$i++, 'log_progress');
	$table_compactor->{'mock'}->is_called(
		$i++, 'sleep', 1.5);
	$table_compactor->{'mock'}->is_called(
		$i++, 'log_progress');
}

sub test_delay_and_proggress_2 : Test(8) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		delay_constant => 2,
		delay_ratio => 1,
		progress_report_period => 5);

	$table_compactor->process(attempt => 1);

	my $i = 1;

	$table_compactor->{'mock'}->is_called(
		$i++, 'sleep', 3);
	$table_compactor->{'mock'}->is_called(
		$i++, 'sleep', 3);
	$table_compactor->{'mock'}->is_called(
		$i++, 'log_progress');
	$table_compactor->{'mock'}->is_called(
		$i++, 'sleep', 3);
}

1;

package PgToolkit::Compactor::TableStub;

use base qw(PgToolkit::Compactor::Table);

use strict;
use warnings;

use Test::MockObject;
use Test::More;

sub init {
	my $self = shift;

	$self->SUPER::init(@_);

	$self->{'mock'} = Test::MockObject->new();

	$self->{'mock'}->mock(
		'-is_called',
		sub {
			my ($self, $pos, $name, @arg_list) = @_;

			is($self->call_pos($pos), $name);
			is_deeply([$self->call_args($pos)], [$self, @arg_list]);
		});

	$self->{'mock'}->set_true('log_progress');
	$self->{'mock'}->set_true('sleep');
	$self->{'mock'}->set_series('-time', 1 .. 1000);

	return;
}

sub _sleep {
	return shift->{'mock'}->sleep(@_);
}

sub _time {
	return shift->{'mock'}->time();
}

sub _log_progress {
	my ($self, %arg_hash) = @_;

	$self->{'mock'}->log_progress();

	return $self->SUPER::_log_progress(%arg_hash);
}

1;
