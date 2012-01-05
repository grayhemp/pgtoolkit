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
			@_);
	};
}

sub test_check_special_triggers : Test(4) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'has_special_triggers'}->
	{'row_list'} = [[1]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(1, 'has_special_triggers');
	$self->{'database'}->{'mock'}->is_called(2, undef);
	ok($table_compactor->is_processed());
}

sub test_min_page_count : Test(6) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[0] =
		[[35000, 42000, 99, 120]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(
		2, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(3, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(4, undef);
	ok($table_compactor->is_processed());
}

sub test_analyze_if_not_analyzed : Test(9) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[0] =
		[[undef, undef, undef]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(
		2, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(3, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		4, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(5, 'get_size_statistics');
	ok($table_compactor->is_processed());
}

sub test_no_initial_vacuum : Test(10) {
	my $self = shift;

	splice(
		@{$self->{'database'}->{'mock'}->{'data_hash'}
		  ->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}},
		1, 1);
	splice(
		@{$self->{'database'}->{'mock'}->{'data_hash'}
		  ->{'get_size_statistics'}->{'row_list_sequence'}},
		1, 1);

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		no_initial_vacuum => 1);

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(
		2, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(3, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(4, 'get_column');
	$self->{'database'}->{'mock'}->is_called(5, 'get_max_tupples_per_page');
	$self->{'database'}->{'mock'}->is_called(6, 'clean_pages', to_page => 99);
}

sub test_min_page_count_after_initial_vacuum : Test(8) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[1] =
		[[35000, 42000, 99, 120]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(4, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		5, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(6, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(7, undef);
	ok($table_compactor->is_processed());
}

sub test_min_free_percent_after_initial_vacuum : Test(8) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[1] =
		[[85, 14, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(4, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		5, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(6, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(7, undef);
	ok($table_compactor->is_processed());
}

sub test_force_processing : Test(16) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[1] =
		[[35000, 42000, 99, 120]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[1] =
		[[85, 14, 5000]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[2] =
		[[35000, 42000, 92, 108]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[2] =
		[[85, 5, 1250]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(force => 1);

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(
		2, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(3, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(4, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		5, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(6, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(7, 'get_column');
	$self->{'database'}->{'mock'}->is_called(8, 'get_max_tupples_per_page');
	$self->{'database'}->{'mock'}->is_called(9, 'clean_pages', to_page => 98);
}

sub test_main_processing : Test(24) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[2] =
		[[35000, 42000, 88, 108]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[2] =
		[[85, 5, 1250]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(7, 'get_column');
	$self->{'database'}->{'mock'}->is_called(8, 'get_max_tupples_per_page');
	$self->{'database'}->{'mock'}->is_called(9, 'clean_pages', to_page => 99);
	$self->{'database'}->{'mock'}->is_called(10, 'clean_pages', to_page => 94);
	$self->{'database'}->{'mock'}->is_called(11, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(12, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(13, 'clean_pages', to_page => 87);
	$self->{'database'}->{'mock'}->is_called(14, 'clean_pages', to_page => 84);
	$self->{'database'}->{'mock'}->is_called(15, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		16, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(17, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(18, 'analyze');
}

sub test_main_processing_no_routine_vacuum : Test(18) {
	my $self = shift;

	splice(
		@{$self->{'database'}->{'mock'}->{'data_hash'}
		  ->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}},
		2, 1);
	splice(
		@{$self->{'database'}->{'mock'}->{'data_hash'}
		  ->{'get_size_statistics'}->{'row_list_sequence'}},
		2, 1);

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		no_routine_vacuum => 1);

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(7, 'get_column');
	$self->{'database'}->{'mock'}->is_called(8, 'get_max_tupples_per_page');
	$self->{'database'}->{'mock'}->is_called(9, 'clean_pages', to_page => 99);
	$self->{'database'}->{'mock'}->is_called(10, 'clean_pages', to_page => 94);
	$self->{'database'}->{'mock'}->is_called(11, 'clean_pages', to_page => 89);
	$self->{'database'}->{'mock'}->is_called(12, 'clean_pages', to_page => 84);
	$self->{'database'}->{'mock'}->is_called(13, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		14, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(15, 'get_size_statistics');
}

sub test_reindex : Test(19) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(
		16, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(17, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(18, 'analyze');
	$self->{'database'}->{'mock'}->is_called(19, 'reindex_select');
	$self->{'database'}->{'mock'}->is_called(20, 'reindex_create1');
	$self->{'database'}->{'mock'}->is_called(21, 'reindex_drop_alter1');
	$self->{'database'}->{'mock'}->is_called(22, 'reindex_create2');
	$self->{'database'}->{'mock'}->is_called(23, 'reindex_drop_alter2');
	$self->{'database'}->{'mock'}->is_called(24, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(25, undef);
}

sub test_print_reindex_queries : Test(9) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1);

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(
		16, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(17, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(18, 'analyze');
	$self->{'database'}->{'mock'}->is_called(19, 'reindex_select');
	$self->{'database'}->{'mock'}->is_called(20, undef);
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

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(110, 'clean_pages', to_page => 84);
	$self->{'database'}->{'mock'}->is_called(111, 'vacuum');
}

sub test_processed : Test {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 91, 108]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[3] =
		[[85, 6, 1400]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[4] =
		[[35000, 42000, 91, 108]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[4] =
		[[85, 6, 1400]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	ok($table_compactor->is_processed());
}

sub test_not_processed : Test {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 92, 110]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[3] =
		[[85, 7, 1500]];

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[4] =
		[[35000, 42000, 92, 110]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[4] =
		[[85, 7, 1500]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	ok(not $table_compactor->is_processed());
}

sub test_get_pgstattuple_bloat_statistics : Test(2) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public');

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(
		2, 'get_pgstattuple_bloat_statistics');
}

sub test_no_final_analyze : Test(5) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		no_final_analyze => 1);

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(
		16, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(17, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(18, undef);
}

sub test_stop_processing_on_deadlock_detected : Test(11) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'clean_pages'}->
	{'row_list_sequence'}->[0] = 'deadlock detected';

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 100, 120]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[3] =
		[[85, 15, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(9, 'clean_pages', to_page => 99);
	$self->{'database'}->{'mock'}->is_called(10, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		11, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(12, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(13, 'analyze');
	ok(not $table_compactor->is_processed());
}

sub test_stop_processing_on_cannot_extract_system_attribute : Test(11) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'clean_pages'}->
	{'row_list_sequence'}->[0] = 'cannot extract system attribute';

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[3] =
		[[35000, 42000, 100, 120]];
	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[3] =
		[[85, 15, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(9, 'clean_pages', to_page => 99);
	$self->{'database'}->{'mock'}->is_called(10, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(
		11, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(12, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(13, 'analyze');
	ok(not $table_compactor->is_processed());
}

sub test_stop_processing_on_relation_does_not_exist : Test(4) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'has_special_triggers'}->
	{'row_list'} = 'relation "schema.table" does not exist';

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(1, 'has_special_triggers');
	$self->{'database'}->{'mock'}->is_called(2, undef);
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

	$table_compactor->process();

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

	$table_compactor->process();

	is($table_compactor->get_total_size_delta(), $size);
}

sub test_delay_and_proggress_1 : Test(12) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

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

	$table_compactor->process();

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
