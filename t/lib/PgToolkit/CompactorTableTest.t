# -*- mode: Perl; -*-
package PgToolkit::CompactorTableTest;

use base qw(PgToolkit::Test);

use strict;
use warnings;

use Test::MockObject;
use Test::More;

use PgToolkit::DatabaseStub;

use PgToolkit::Logger;

sub setup : Test(setup) {
	my $self = shift;

	$self->{'database'} = PgToolkit::DatabaseStub->new(dbname => 'dbname');

	$self->{'table_compactor_constructor'} = sub {
		PgToolkit::Compactor::TableStub->new(
			database => $self->{'database'},
			logger => PgToolkit::Logger->new(
				level => 'info', err_handle => \*STDOUT),
			dry_run => 0,
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
			progress_report_period => 3,
			pgstattuple_schema_name => undef,
			pages_per_round_divisor => 1,
			pages_before_vacuum_lower_divisor => 16,
			pages_before_vacuum_lower_threshold => 1000,
			pages_before_vacuum_upper_divisor => 50,
			max_retry_count => 2,
			@_);
	};
}

sub test_dry_run : Test(8) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		dry_run => 1);

	$table_compactor->process(attempt => 1);

	$self->{'database'}->{'mock'}->is_called(1, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		2, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(3, 'has_special_triggers');
	$self->{'database'}->{'mock'}->is_called(4, undef);
	ok($table_compactor->is_processed());
}

sub test_check_special_triggers : Test(4) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'has_special_triggers'}->
	{'row_list'} = [[1]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	$self->{'database'}->{'mock'}->is_called(5, 'has_special_triggers');
	$self->{'database'}->{'mock'}->is_called(6, undef);
	ok($table_compactor->is_processed());
}

sub test_no_initial_vacuum : Test(6) {
	my $self = shift;

	splice(
		@{$self->{'database'}->{'mock'}->{'data_hash'}
		  ->{'get_size_statistics'}->{'row_list_sequence'}},
		1, 1);

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		no_initial_vacuum => 1);

	$table_compactor->process(attempt => 1);

	$self->{'database'}->{'mock'}->is_called(1, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		2, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(3, 'has_special_triggers');
}

sub test_analyze_if_not_analyzed : Test(11) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'} = [
		[[undef, undef, undef]],
		@{$self->{'database'}->{'mock'}->{'data_hash'}
		  ->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}}];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	$self->{'database'}->{'mock'}->is_called(3, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		4, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(5, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		6, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(7, 'has_special_triggers');
	ok($table_compactor->is_processed());
}

sub test_min_page_count : Test(4) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[1] =
		[[35000, 42000, 99, 120]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	$self->{'database'}->{'mock'}->is_called(5, 'has_special_triggers');
	$self->{'database'}->{'mock'}->is_called(6, undef);
	ok($table_compactor->is_processed());
}

sub test_min_free_percent : Test(4) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[0] =
		[[85, 14, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	$self->{'database'}->{'mock'}->is_called(5, 'has_special_triggers');
	$self->{'database'}->{'mock'}->is_called(6, undef);
	ok($table_compactor->is_processed());
}

sub test_force_processing : Test(4) {
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

	$self->{'database'}->{'mock'}->is_called(5, 'has_special_triggers');
	$self->{'database'}->{'mock'}->is_called(6, 'get_column');
}

sub test_main_processing : Test(20) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[2] =
		[[35000, 42000, 88, 108]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	$self->{'database'}->{'mock'}->is_called(6, 'get_column');
	$self->{'database'}->{'mock'}->is_called(7, 'get_max_tupples_per_page');
	$self->{'database'}->{'mock'}->is_called(8, 'clean_pages', to_page => 99);
	$self->{'database'}->{'mock'}->is_called(9, 'clean_pages', to_page => 94);
	$self->{'database'}->{'mock'}->is_called(10, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(11, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(12, 'clean_pages', to_page => 87);
	$self->{'database'}->{'mock'}->is_called(13, 'clean_pages', to_page => 84);
	$self->{'database'}->{'mock'}->is_called(14, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(15, 'get_size_statistics');
}

sub test_main_processing_no_routine_vacuum : Test(16) {
	my $self = shift;

	splice(
		@{$self->{'database'}->{'mock'}->{'data_hash'}
		  ->{'get_size_statistics'}->{'row_list_sequence'}},
		2, 1);

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		no_routine_vacuum => 1);

	$table_compactor->process(attempt => 1);

	$self->{'database'}->{'mock'}->is_called(6, 'get_column');
	$self->{'database'}->{'mock'}->is_called(7, 'get_max_tupples_per_page');
	$self->{'database'}->{'mock'}->is_called(8, 'clean_pages', to_page => 99);
	$self->{'database'}->{'mock'}->is_called(9, 'clean_pages', to_page => 94);
	$self->{'database'}->{'mock'}->is_called(10, 'clean_pages', to_page => 89);
	$self->{'database'}->{'mock'}->is_called(11, 'clean_pages', to_page => 84);
	$self->{'database'}->{'mock'}->is_called(12, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(13, 'get_size_statistics');
}

sub test_reindex_if_last_attempt_and_not_processed : Test(19) {
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
		reindex => 1);

	$table_compactor->process(attempt => 2);

	$self->{'database'}->{'mock'}->is_called(15, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(16, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		17, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(18, 'reindex_select');
	$self->{'database'}->{'mock'}->is_called(19, 'reindex_create1');
	$self->{'database'}->{'mock'}->is_called(20, 'reindex_drop_alter1');
	$self->{'database'}->{'mock'}->is_called(21, 'reindex_create2');
	$self->{'database'}->{'mock'}->is_called(22, 'reindex_drop_alter2');
	$self->{'database'}->{'mock'}->is_called(23, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(24, undef);
}

sub test_reindex_if_not_last_attempt_and_processed : Test(19) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	$self->{'database'}->{'mock'}->is_called(15, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(16, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		17, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(18, 'reindex_select');
	$self->{'database'}->{'mock'}->is_called(19, 'reindex_create1');
	$self->{'database'}->{'mock'}->is_called(20, 'reindex_drop_alter1');
	$self->{'database'}->{'mock'}->is_called(21, 'reindex_create2');
	$self->{'database'}->{'mock'}->is_called(22, 'reindex_drop_alter2');
	$self->{'database'}->{'mock'}->is_called(23, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(24, undef);
}

sub test_no_reindex_if_not_last_attempt_and_not_processed : Test(7) {
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
		reindex => 1);

	$table_compactor->process(attempt => 1);

	$self->{'database'}->{'mock'}->is_called(15, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(16, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		17, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(18, undef);
}

sub test_reindex_queries_if_last_attempt_and_not_processed : Test(9) {
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

	$self->{'database'}->{'mock'}->is_called(15, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(16, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		17, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(18, 'reindex_select');
	$self->{'database'}->{'mock'}->is_called(19, undef);
}

sub test_reindex_queries_if_not_last_attempt_and_processed : Test(9) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 1);

	$self->{'database'}->{'mock'}->is_called(15, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(16, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		17, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(18, 'reindex_select');
	$self->{'database'}->{'mock'}->is_called(19, undef);
}

sub test_no_reindex_queries_if_not_last_attempt_and_not_processed : Test(7) {
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

	$table_compactor->process(attempt => 1);

	$self->{'database'}->{'mock'}->is_called(15, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(16, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		17, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(18, undef);
}

sub test_loops_count : Test(4) {
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

	$self->{'database'}->{'mock'}->is_called(109, 'clean_pages', to_page => 84);
	$self->{'database'}->{'mock'}->is_called(110, 'vacuum');
}

sub test_processed : Test {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 91, 108]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[4] =
		[[35000, 42000, 91, 108]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[3] =
		[[85, 6, 1400]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[2] =
		[[85, 6, 1400]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	ok($table_compactor->is_processed());
}

sub test_not_processed : Test {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 92, 110]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[4] =
		[[35000, 42000, 92, 110]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[3] =
		[[85, 7, 1500]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[2] =
		[[85, 7, 1500]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	ok(not $table_compactor->is_processed());
}

sub test_get_pgstattuple_bloat_statistics : Test(2) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public');

	$table_compactor->process(attempt => 1);

	$self->{'database'}->{'mock'}->is_called(
		4, 'get_pgstattuple_bloat_statistics');
}

sub test_no_final_analyze : Test(5) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		no_final_analyze => 1);

	$table_compactor->process(attempt => 1);

	$self->{'database'}->{'mock'}->is_called(15, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		16, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(17, undef);
}

sub test_stop_processing_on_deadlock_detected : Test(7) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'clean_pages'}->
	{'row_list_sequence'}->[0] = 'deadlock detected';

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 100, 120]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[2] =
		[[85, 15, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	$self->{'database'}->{'mock'}->is_called(8, 'clean_pages', to_page => 99);
	$self->{'database'}->{'mock'}->is_called(9, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(10, 'get_size_statistics');
	ok(not $table_compactor->is_processed());
}

sub test_stop_processing_on_cannot_extract_system_attribute : Test(7) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'clean_pages'}->
	{'row_list_sequence'}->[0] = 'cannot extract system attribute';

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 100, 120]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[2] =
		[[85, 15, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	$self->{'database'}->{'mock'}->is_called(8, 'clean_pages', to_page => 99);
	$self->{'database'}->{'mock'}->is_called(9, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(10, 'get_size_statistics');
	ok(not $table_compactor->is_processed());
}

sub test_stop_processing_on_relation_does_not_exist : Test(4) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'vacuum'}
	->{'row_list'} = 'relation "schema.table" does not exist';

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	$self->{'database'}->{'mock'}->is_called(2, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(3, undef);
	ok($table_compactor->is_processed());
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

	$table_compactor->{'mock'}->is_called(1, 'sleep', 1.5);
	$table_compactor->{'mock'}->is_called(2, 'log_progress');
	$table_compactor->{'mock'}->is_called(3, 'sleep', 1.5);
	$table_compactor->{'mock'}->is_called(4, 'log_progress');
	$table_compactor->{'mock'}->is_called(5, 'sleep', 1.5);
	$table_compactor->{'mock'}->is_called(6, 'log_progress');
}

sub test_delay_and_proggress_2 : Test(8) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		delay_constant => 2,
		delay_ratio => 1,
		progress_report_period => 5);

	$table_compactor->process(attempt => 1);

	$table_compactor->{'mock'}->is_called(1, 'sleep', 3);
	$table_compactor->{'mock'}->is_called(2, 'sleep', 3);
	$table_compactor->{'mock'}->is_called(3, 'log_progress');
	$table_compactor->{'mock'}->is_called(4, 'sleep', 3);
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
