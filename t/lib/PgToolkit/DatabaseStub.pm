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
				if (not exists $self->{'data_hash'}->{$name}) {
					die('No such key in data hash: '.$name);
				}

				my $sql_pattern =
					$self->{'data_hash'}->{$name}->{'sql_pattern'};
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

			my $data_hash = $self->{'data_hash'};

			my $result;
			for my $key (keys %{$data_hash}) {
				if (not defined $data_hash->{$key}->{'sql_pattern'}) {
					die('No such key in data hash: '.$key);
				}

				my $sql_pattern = $data_hash->{$key}->{'sql_pattern'};
				$sql_pattern =~ s/<[a-z_]+>/.*/g;
				if ($arg_hash{'sql'} =~ qr/$sql_pattern/) {
					if (exists $data_hash->{$key}->{'row_list'}) {
						$result = $data_hash->{$key}->{'row_list'};
					} else {
						$result =
							shift @{$data_hash->{$key}->{'row_list_sequence'}};
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
				die('DatabaseError '.$result);
			}

			return $result;
		});

	my $bloat_statistics_row_list_sequence = [
		[[85, 15, 5000]],
		[[85, 5, 1250]],
		[[85, 0, 0]],
		[[85, 0, 0]]];

	my $size_statistics_row_list_sequence = [
		[[35000, 42000, 100, 120]],
		[[35000, 42000, 100, 120]],
		[[31500, 37800, 90, 108]],
		[[29750, 35700, 85, 102]],
		[[29750, 35700, 85, 102]]];

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
		'get_approximate_bloat_statistics' => {
			'sql_pattern' => (
				qr/SELECT\s+effective_page_count,.+/s.
				qr/END AS free_percent,.+END AS free_space.+/s.
				qr/pg_catalog\.pg_class\.oid = 'schema\.table'::regclass/),
			'row_list_sequence' => $bloat_statistics_row_list_sequence},
		'get_pgstattuple_bloat_statistics' => {
			'sql_pattern' => (
				qr/SELECT.+END AS effective_page_count,.+/s.
				qr/END AS free_percent,.+END AS free_space.+/s.
				qr/pgstattuple\('schema\.table'\).+/s.
				qr/pg_catalog\.pg_class\.oid = 'schema\.table'::regclass/),
			'row_list_sequence' => $bloat_statistics_row_list_sequence},
		'get_size_statistics' => {
			'sql_pattern' => (
				qr/SELECT\s+size,\s+total_size,.+/s.
				qr/pg_catalog\.pg_relation_size\('schema\.table'\).+/s.
				qr/pg_catalog\.pg_total_relation_size\('schema\.table'\)/),
			'row_list_sequence' => $size_statistics_row_list_sequence},
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
				'row_list' => [[0]]}};

	return;
}

sub _execute {
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
