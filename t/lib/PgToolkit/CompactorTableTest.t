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

sub test_min_page_count : Test(4) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'get_statistics'}->
	{'row_list_sequence'}->[0] = [[99, 120, 85, 15, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(2, 'get_statistics');
	$self->{'database'}->{'mock'}->is_called(3, undef);
	ok($table_compactor->is_processed());
}

sub test_analyze_if_not_analyzed : Test(9) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'get_statistics'}->
	{'row_list_sequence'}->[0] = [[100, 120, undef, undef, undef]],;

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(2, 'get_statistics');
	$self->{'database'}->{'mock'}->is_called(3, 'analyze');
	$self->{'database'}->{'mock'}->is_called(4, 'get_statistics');
	$self->{'database'}->{'mock'}->is_called(5, 'vacuum');
	ok($table_compactor->is_processed());
}

sub test_no_initial_vacuum : Test(8) {
	my $self = shift;

	splice(
		@{$self->{'database'}->{'mock'}->{'data_hash'}->{'get_statistics'}->
		  {'row_list_sequence'}}, 1, 1);

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		no_initial_vacuum => 1);

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(2, 'get_statistics');
	$self->{'database'}->{'mock'}->is_called(3, 'get_column');
	$self->{'database'}->{'mock'}->is_called(4, 'get_max_tupples_per_page');
	$self->{'database'}->{'mock'}->is_called(5, 'clean_pages', to_page => 99);
}

sub test_min_page_count_after_initial_vacuum : Test(6) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'get_statistics'}->
	{'row_list_sequence'}->[1] = [[99, 120, 85, 15, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(3, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(4, 'get_statistics');
	$self->{'database'}->{'mock'}->is_called(5, undef);
	ok($table_compactor->is_processed());
}

sub test_min_free_percent_after_initial_vacuum : Test(6) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'get_statistics'}->
	{'row_list_sequence'}->[1] = [[100, 120, 85, 14, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(3, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(4, 'get_statistics');
	$self->{'database'}->{'mock'}->is_called(5, undef);
	ok($table_compactor->is_processed());
}

sub test_force_processing : Test(12) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'get_statistics'}->
	{'row_list_sequence'}->[1] = [[99, 120, 85, 14, 5000]];
	$self->{'database'}->{'mock'}->{'data_hash'}->{'get_statistics'}->
	{'row_list_sequence'}->[2] = [[92, 108, 85, 5, 1250]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(force => 1);

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(2, 'get_statistics');
	$self->{'database'}->{'mock'}->is_called(3, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(4, 'get_statistics');
	$self->{'database'}->{'mock'}->is_called(5, 'get_column');
	$self->{'database'}->{'mock'}->is_called(6, 'get_max_tupples_per_page');
	$self->{'database'}->{'mock'}->is_called(7, 'clean_pages', to_page => 98);
}

sub test_main_processing : Test(28) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'get_statistics'}->
	{'row_list_sequence'}->[2] = [[88, 108, 85, 5, 1250]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(5, 'get_column');
	$self->{'database'}->{'mock'}->is_called(6, 'get_max_tupples_per_page');
	$self->{'database'}->{'mock'}->is_called(7, 'clean_pages', to_page => 99);
	$table_compactor->{'mock'}->is_called(1, 'sleep', 2.5);
	$self->{'database'}->{'mock'}->is_called(8, 'clean_pages', to_page => 94);
	$table_compactor->{'mock'}->is_called(2, 'sleep', 2.5);
	$self->{'database'}->{'mock'}->is_called(9, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(10, 'get_statistics');
	$self->{'database'}->{'mock'}->is_called(11, 'clean_pages', to_page => 87);
	$table_compactor->{'mock'}->is_called(3, 'sleep', 2.5);
	$self->{'database'}->{'mock'}->is_called(12, 'clean_pages', to_page => 84);
	$self->{'database'}->{'mock'}->is_called(13, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(14, 'get_statistics');
	$self->{'database'}->{'mock'}->is_called(15, 'analyze');
}

sub test_main_processing_no_routine_vacuum : Test(22) {
	my $self = shift;

	splice(@{$self->{'database'}->{'mock'}->{'data_hash'}->{'get_statistics'}->
			 {'row_list_sequence'}}, 2, 1);

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		no_routine_vacuum => 1);

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(5, 'get_column');
	$self->{'database'}->{'mock'}->is_called(6, 'get_max_tupples_per_page');
	$self->{'database'}->{'mock'}->is_called(7, 'clean_pages', to_page => 99);
	$table_compactor->{'mock'}->is_called(1, 'sleep', 2.5);
	$self->{'database'}->{'mock'}->is_called(8, 'clean_pages', to_page => 94);
	$table_compactor->{'mock'}->is_called(2, 'sleep', 2.5);
	$self->{'database'}->{'mock'}->is_called(9, 'clean_pages', to_page => 89);
	$table_compactor->{'mock'}->is_called(3, 'sleep', 2.5);
	$self->{'database'}->{'mock'}->is_called(10, 'clean_pages', to_page => 84);
	$self->{'database'}->{'mock'}->is_called(11, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(12, 'get_statistics');
}

sub test_reindex : Test(17) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(14, 'get_statistics');
	$self->{'database'}->{'mock'}->is_called(15, 'analyze');
	$self->{'database'}->{'mock'}->is_called(16, 'reindex_select');
	$self->{'database'}->{'mock'}->is_called(17, 'reindex_create1');
	$self->{'database'}->{'mock'}->is_called(18, 'reindex_drop_alter1');
	$self->{'database'}->{'mock'}->is_called(19, 'reindex_create2');
	$self->{'database'}->{'mock'}->is_called(20, 'reindex_drop_alter2');
	$self->{'database'}->{'mock'}->is_called(21, 'get_statistics');
	$self->{'database'}->{'mock'}->is_called(22, undef);
}

sub test_print_reindex_queries : Test(9) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1);

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(14, 'get_statistics');
	$self->{'database'}->{'mock'}->is_called(15, 'analyze');
	$self->{'database'}->{'mock'}->is_called(16, 'reindex_select');
	$self->{'database'}->{'mock'}->is_called(17, 'get_statistics');
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

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(108, 'clean_pages', to_page => 84);
	$self->{'database'}->{'mock'}->is_called(109, 'vacuum');
}

sub test_processed : Test {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	ok($table_compactor->is_processed());
}

sub test_not_processed : Test {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'get_statistics'}->
	{'row_list_sequence'}->[3] = [[92, 110, 85, 7, 1500]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	ok(not $table_compactor->is_processed());
}

sub test_pgstattuple_installed_get_statistics_by_it : Test(2) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public');

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(2, 'get_pgstattuple_statistics');
}

sub test_no_final_analyze : Test(5) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		no_final_analyze => 1);

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(14, 'get_statistics');
	$self->{'database'}->{'mock'}->is_called(15, 'get_statistics');
	$self->{'database'}->{'mock'}->is_called(16, undef);
}

sub test_stop_processing_on_deadlock_detected : Test(9) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'clean_pages'}->
	{'row_list_sequence'}->[0] = 'deadlock detected';

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(7, 'clean_pages', to_page => 99);
	$self->{'database'}->{'mock'}->is_called(8, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(9, 'get_statistics');
	$self->{'database'}->{'mock'}->is_called(10, 'analyze');
	ok(not $table_compactor->is_processed());
}

sub test_stop_processing_on_cannot_extract_system_attribute : Test(9) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'clean_pages'}->
	{'row_list_sequence'}->[0] = 'cannot extract system attribute';

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process();

	$self->{'database'}->{'mock'}->is_called(7, 'clean_pages', to_page => 99);
	$self->{'database'}->{'mock'}->is_called(8, 'vacuum');
	$self->{'database'}->{'mock'}->is_called(9, 'get_statistics');
	$self->{'database'}->{'mock'}->is_called(10, 'analyze');
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

1;
