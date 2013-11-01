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
			print_reindex_queries => 0,
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

sub test_dry_run : Test(16) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		dry_run => 1,
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 1;

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
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_check_special_triggers : Test(12) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'has_special_triggers'}->
	{'row_list'} = [[1]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 1;

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
		$i++, undef);
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

	my $i = 1;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'has_special_triggers');
}

sub test_analyze_if_not_analyzed : Test(15) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'} = [
		[[undef, undef, undef]],
		@{$self->{'database'}->{'mock'}->{'data_hash'}
		  ->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}}];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 1;

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

sub test_skip_processing_if_table_is_empty : Test(8) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[1] =
		[[0, 0, 0, 0]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 1;

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

sub test_min_page_count : Test(12) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'}->[1] =
		[[35000, 42000, 99, 120]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 1;

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
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_min_free_percent : Test(12) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[0] =
		[[85, 14, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 1;

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
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_force_processing : Test(12) {
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

	my $i = 1;

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

sub test_can_not_get_bloat_statistics : Test(14) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'} = [
		[[undef, undef, undef]],
		[[undef, undef, undef]]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 1;

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
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_can_not_get_size_statistics : Test(4) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_size_statistics'}->{'row_list_sequence'} = [
		[[undef, undef, undef]]];

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 1;

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

	my $i = 6;

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

	my $i = 6;

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

	my $i = 6;

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

	my $i = 6;

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

	my $i = 6;

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

	my $i = 6;

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

	my $i = 6;

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

	my $i = 23;

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

	my $i = 23;

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

sub test_reindex : Test(35) {
	my $self = shift;

	# $self->{'database'}->{'mock'}->{'data_hash'}
	# ->{'get_index_size_statistics'}->{'row_list_sequence'}->[4] = [[500, 100]];
	# $self->{'database'}->{'mock'}->{'data_hash'}
	# ->{'get_index_size_statistics'}->{'row_list_sequence'}->[5] = [[425, 85]];

	# $self->{'database'}->{'mock'}->{'data_hash'}
	# ->{'get_index_data_list'}->{'row_list'}->[2] = [
	# 	'table_idx3', 'tablespace',
	# 	'CREATE INDEX table_idx3 ON schema.table '.
	# 	'USING btree (column3)',
	# 	'btree', undef, undef, 1, 3000];

	# $self->{'database'}->{'mock'}->{'data_hash'}
	# ->{'reindex3'}= {
	# 	'sql_pattern' =>
	# 		qr/CREATE INDEX CONCURRENTLY pgcompact_tmp$$ ON /.
	# 		qr/schema\.table USING btree \(column3\) /.
	# 		qr/TABLESPACE tablespace;/,
	# 	'row_list' => []};

	# $self->{'database'}->{'mock'}->{'data_hash'}
	# ->{'alter_index3'}= {
	# 	'sql_pattern' =>
	# 		qr/BEGIN; DROP INDEX schema\.table_idx3; /.
	# 		qr/ALTER INDEX schema\.pgcompact_tmp$$ /.
	# 		qr/RENAME TO table_idx3; END;/,
	# 	'row_list' => []};

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, 'reindex1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_if_last_attempt_and_not_processed : Test(35) {
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

	my $i = 23;

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
		$i++, 'reindex1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_if_not_last_attempt_and_processed : Test(35) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, 'reindex1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
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

	my $i = 23;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_queries_if_last_attempt_and_not_processed : Test(15) {
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

	my $i = 23;

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
		$i++, undef);
}

sub test_reindex_queries_if_not_last_attempt_and_processed : Test(15) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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

	my $i = 23;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'analyze');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_if_in_min_free_percent : Test(35) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_bloat_statistics'}->{'row_list_sequence'}->[2] = [[14, 75]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public',
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, 'reindex1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_if_in_min_free_percent_and_forced : Test(35) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_bloat_statistics'}->{'row_list_sequence'}->[2] = [[14, 75]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public',
		reindex => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, 'reindex1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_queries_if_in_min_free_percent : Test(21) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_bloat_statistics'}->{'row_list_sequence'}->[2] = [[14, 75]];


	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public',
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, undef);
}

sub test_reindex_queries_if_in_min_free_percent_and_forced : Test(15) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_bloat_statistics'}->{'row_list_sequence'}->[2] = [[14, 75]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public',
		print_reindex_queries => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, undef);
}

sub test_no_reindex_if_in_min_page_count : Test(29) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_size_statistics'}->{'row_list_sequence'}->[4] = [[495, 99]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, 'reindex1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_if_in_min_page_count_and_forced : Test(35) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_size_statistics'}->{'row_list_sequence'}->[4] = [[495, 99]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, 'reindex1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_queries_if_in_min_page_count : Test(21) {
	my $self = shift;

	splice(
		$self->{'database'}->{'mock'}->{'data_hash'}
		->{'get_index_size_statistics'}->{'row_list_sequence'},
		4, 2, [[495, 99]]);

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public',
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, undef);
}

sub test_reindex_queries_if_in_min_page_count_and_forced : Test(15) {
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

	my $i = 23;

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
		$i++, undef);
}

sub test_reindex_if_table_skipped_and_pgstatuple : Test(46) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[0] =
		[[85, 14, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public',
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 1;

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
		$i++, 'reindex1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_bloat_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_reindex_queries_if_table_skipped_and_pgstatuple : Test(26) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_approximate_bloat_statistics'}->{'row_list_sequence'}->[0] =
		[[85, 14, 5000]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		pgstattuple_schema_name => 'public',
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 1);

	my $i = 1;

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
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_reindex_if_not_processed_and_will_be_skipped : Test(36) {
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

	my $i = 23;

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
		$i++, 'reindex1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_reindex_queries_if_not_processed_and_will_be_skipped : Test(16) {
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

	my $i = 23;

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
		$i++, undef);
	ok($table_compactor->is_processed());
}

sub test_no_reindex_if_index_is_empty_and_forced : Test(29) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_size_statistics'}->{'row_list_sequence'}->[4] = [[0, 0]];

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, 'reindex1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_queries_if_index_is_empty_and_forced : Test(15) {
	my $self = shift;

	splice(
		$self->{'database'}->{'mock'}->{'data_hash'}
		->{'get_index_size_statistics'}->{'row_list_sequence'},
		3, 3, [[0, 0]]);

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, undef);
}

sub test_no_reindex_if_not_btree : Test(29) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_data_list'}->{'row_list'}->[2][3] = 'gist';

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, 'reindex1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_queries_if_not_btree : Test(15) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_data_list'}->{'row_list'}->[2][5] = 'gist';

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, undef);
}

sub test_reindex_if_not_btree_and_forced : Test(35) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_data_list'}->{'row_list'}->[2][5] = 'gist';

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, 'reindex1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_reindex_queries_if_not_btree_and_force : Test(15) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_data_list'}->{'row_list'}->[2][5] = 'gist';

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, undef);
}

sub test_no_reindex_if_not_allowed : Test(29) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_data_list'}->{'row_list'}->[2][6] = 0;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, 'reindex1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_queries_if_not_allowed : Test(15) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_data_list'}->{'row_list'}->[2][6] = 0;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, undef);
}

sub test_no_reindex_if_not_allowed_and_forced : Test(29) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_data_list'}->{'row_list'}->[2][6] = 0;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		reindex => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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
		$i++, 'reindex1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index1');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_pkey');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'reindex2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'alter_index2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx2');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_index_size_statistics', name => 'table_idx3');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_no_reindex_queries_if_not_allowed_and_forced : Test(15) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}
	->{'get_index_data_list'}->{'row_list'}->[2][6] = 0;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		print_reindex_queries => 1,
		force => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

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

	my $i = 307;

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
		#pgstattuple_schema_name => 'public',
		#reindex => 1);

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

	my $i = 4;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_pgstattuple_bloat_statistics');
}

sub test_no_final_analyze : Test(5) {
	my $self = shift;

	my $table_compactor = $self->{'table_compactor_constructor'}->(
		no_final_analyze => 1);

	$table_compactor->process(attempt => 1);

	my $i = 23;

	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_size_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, 'get_approximate_bloat_statistics');
	$self->{'database'}->{'mock'}->is_called(
		$i++, undef);
}

sub test_continue_processing_on_deadlock_detected : Test(12) {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'clean_pages'}
	->{'row_list_sequence'}->[0] = 'deadlock detected';

	my $table_compactor = $self->{'table_compactor_constructor'}->();

	$table_compactor->process(attempt => 1);

	my $i = 8;

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

	my $i = 8;

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

	my $i = 2;

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
