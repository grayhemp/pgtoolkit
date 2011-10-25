package PgToolkit::DatabaseStub;

use base qw(PgToolkit::Database);

use strict;
use warnings;

use Test::MockObject;
use Test::More;

use Test::Exception;

sub init {
	my $self = shift;

	$self->SUPER::init(@_);

	$self->{'mock'} = Test::MockObject->new();

	$self->{'mock'}->mock(
		'-is_called',
		sub {
			my ($self, $pos, $name, %substitution_hash) = @_;

			if (defined $name) {
				my $sql_pattern = $self->{'data_hash'}->{$name}
				->{'sql_pattern'};
				for my $item (keys %substitution_hash) {
					$sql_pattern =~ s/<$item>/$substitution_hash{$item}/g;
				}

				is($self->call_pos($pos), 'execute');
				like({$self, $self->call_args($pos)}->{'sql'},
					 qr/$sql_pattern/);
			} else {
				is($self->call_pos($pos), undef);
			}

			return;
		});

	$self->{'mock'}->mock(
		'execute',
		sub {
			my ($self, %arg_hash) = @_;

			my $result;
			for my $data (values %{$self->{'data_hash'}}) {
				my $sql_pattern = $data->{'sql_pattern'};
				$sql_pattern =~ s/<[a-z_]+>/.*/g;
				if ($arg_hash{'sql'} =~ qr/$sql_pattern/) {
					if (exists $data->{'row_list'}) {
						$result = $data->{'row_list'};
					} else {
						$result = shift @{$data->{'row_list_sequence'}};
						if (not defined $result) {
							die("Not enough results for: \n".
								$arg_hash{'sql'});
						}
					}
					last;
				}
			}

			if (not defined $result) {
				die("Can not find an appropriate SQL pattern for: \n".
					$arg_hash{'sql'});
			}

			if (ref($result) ne 'ARRAY') {
				die($result);
			}

			return $result;
		});

	$self->{'mock'}->{'data_hash'} = {
		'has_special_triggers' => {
			'sql_pattern' => (
				qr/SELECT count\(1\) FROM pg_catalog\.pg_trigger.+/s.
				qr/tgrelid = 'schema\.table'::regclass/),
			'row_list' => [[0]]},
		'get_max_tupples_per_page' => {
			'sql_pattern' => (
				qr/SELECT ceil\(current_setting\('block_size'\)::real \/ /.
				qr/sum\(attlen\)\).+/s.
				qr/attrelid = 'schema\.table'::regclass/),
			'row_list' => [[10]]},
		'get_statistics' => {
			'sql_pattern' => (
				qr/SELECT\s+page_count, total_page_count.+/s.
				qr/pg_catalog\.pg_class\.oid = 'schema\.table'::regclass/),
			'row_list_sequence' => [
				[[100, 120, 85, 15, 5000]],
				[[100, 120, 85, 15, 5000]],
				[[90, 108, 85, 5, 1250]],
				[[85, 102, 85, 0, 0]],,
				[[85, 102, 85, 0, 0]]]},
		'get_column' => {
			'sql_pattern' => (
				qr/SELECT attname.+attrelid = 'schema\.table'::regclass.+/s.
				qr/indrelid = 'schema\.table'::regclass/),
			'row_list' => [['column']]},
		'clean_pages' => {
			'sql_pattern' => (
				qr/SELECT public\._clean_pages\(\s+'schema.table', 'column', /s.
				qr/<to_page>,\s+5, 10/s),
			'row_list_sequence' => [
				[[94]], [[89]], [[84]],
				'No more free space left in the table']},
		'vacuum' => {
			'sql_pattern' => qr/VACUUM schema\.table/,
			'row_list' => [[undef]]},
		'vacuum_analyze' => {
			'sql_pattern' => qr/VACUUM ANALYZE schema\.table/,
			'row_list' => [[undef]]},
		'analyze' => {
			'sql_pattern' => qr/ANALYZE schema\.table/,
			'row_list' => [[undef]]},
		'reindex_select' => {
			'sql_pattern' => (
				qr/SELECT indexname, tablespace, indexdef.+/s.
				qr/schemaname = 'schema'.+tablename = 'table'/s),
			'row_list' => [
				['i_table__idx1', undef,
				 'CREATE INDEX i_table__idx1 ON schema.table '.
				 'USING btree (column1)'],
				['i_table__idx2', 'tablespace',
				 'CREATE INDEX i_table__idx2 ON schema.table '.
				 'USING btree (column2) WHERE column2 = 1']]},
		'reindex_create1' => {
			'sql_pattern' =>
				qr/CREATE INDEX CONCURRENTLY i_compactor_$$ ON schema\.table /.
				qr/USING btree \(column1\)/,
			'row_list' => []},
		'reindex_create2' => {
			'sql_pattern' =>
				qr/CREATE INDEX CONCURRENTLY i_compactor_$$ ON schema\.table /.
				qr/USING btree \(column2\) TABLESPACE tablespace /.
				qr/WHERE column2 = 1/,
			'row_list' => []},
		'reindex_drop_alter1' => {
			'sql_pattern' =>
				qr/BEGIN; DROP INDEX schema\.i_table__idx1; /.
				qr/ALTER INDEX schema\.i_compactor_$$ /.
				qr/RENAME TO i_table__idx1; END;/,
			'row_list' => []},
		'reindex_drop_alter2' => {
			'sql_pattern' =>
				qr/BEGIN; DROP INDEX schema\.i_table__idx2; /.
				qr/ALTER INDEX schema\.i_compactor_$$ /.
				qr/RENAME TO i_table__idx2; END;/,
			'row_list' => []},
		'get_table_name_list1' => {
			'sql_pattern' =>
				qr/SELECT tablename FROM pg_catalog\.pg_tables\n/.
				qr/WHERE schemaname = 'schema\d?' \n/.
				qr/ORDER BY\n    pg_catalog\.pg_relation_size/,
			'row_list' => [['table1'],['table2']]},
		'get_table_name_list2' => {
			'sql_pattern' =>
				qr/SELECT tablename FROM pg_catalog\.pg_tables\n/.
				qr/WHERE schemaname = 'schema\d?' /.
				qr/AND tablename IN \('table2', 'table1'\)\n/.
				qr/ORDER BY\n    pg_catalog\.pg_relation_size/,
			'row_list' => [['table1'],['table2']]},
		'has_schema' => {
			'sql_pattern' =>
				qr/SELECT count\(1\) FROM pg_catalog\.pg_namespace\n/.
				qr/WHERE nspname = 'schema\d?'/,
			'row_list' => [[1]]},
		'get_schema_name_list' => {
			'sql_pattern' =>
				qr/SELECT nspname FROM pg_catalog\.pg_namespace/,
			'row_list' => [['schema1'],['schema2']]},
		'create_clean_pages' => {
			'sql_pattern' =>
				qr/CREATE OR REPLACE FUNCTION public\._clean_pages/,
			'row_list' => []},
		'drop_clean_pages' => {
			'sql_pattern' =>
				qr/DROP FUNCTION public\._clean_pages/,
			'row_list' => []},
		'get_dbname_list1' => {
			'sql_pattern' =>
				qr/SELECT datname FROM pg_catalog\.pg_database\n/.
				qr/WHERE datname NOT IN \([^\(]*\) \n/.
				qr/ORDER BY pg_catalog\.pg_database_size/,
			'row_list' => [['dbname1'], ['dbname2']]},
		'get_dbname_list2' => {
			'sql_pattern' =>
				qr/SELECT datname FROM pg_catalog\.pg_database\n/.
				qr/WHERE datname NOT IN \([^\(]*\) /.
				qr/AND datname IN \('dbname2', 'dbname1'\)\n/.
				qr/ORDER BY pg_catalog\.pg_database_size/,
			'row_list' => [['dbname1'], ['dbname2']]},
		'get_pgstattuple_schema_name' => {
			'sql_pattern' =>
				qr/SELECT nspname FROM pg_catalog\.pg_proc.+/s.
				qr/WHERE proname = 'pgstattuple' LIMIT 1/,
			'row_list' => [[0]]},
		'get_pgstattuple_statistics' => {
			'sql_pattern' => (
				qr/free_percent, free_space.+/s.
				qr/FROM public\.pgstattuple\('schema\.table'\)/),
			'row_list_sequence' => [
				[[100, 120, 85, 15, 5000]],
				[[100, 120, 85, 15, 5000]],
				[[90, 108, 85, 5, 1250]],
				[[85, 102, 85, 0, 0]],
				[[85, 102, 85, 0, 0]]]}};

	return;
}

sub execute {
	return shift->{'mock'}->execute(@_);
}

sub get_adapter_name {
	return 'Stub';
}

sub _quote_ident {
	my ($self, %arg_hash) = @_;

	return $arg_hash{'string'};
}

1;
