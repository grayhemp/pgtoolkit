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
		'get_index_statistics' => {
			'sql_pattern' => (
				qr/SELECT\s+index_size, avg_leaf_density,.+/s.
				qr/public\.pgstatindex\(\s+'schema\.<name>'\).+/s.
				qr/pg_catalog\.pg_class.oid = 'schema\.<name>'/),
			'row_list_sequence' => [[[500, 15, 75]], [[1000, 15, 150]]]},
		'get_index_data_list' => {
			'sql_pattern' => (
				qr/SELECT DISTINCT\s+/s.
				qr/indexname, tablespace, indexdef, conname,.+/s.
				qr/schemaname = 'schema' AND\s+tablename = 'table'/s),
			'row_list' => [
				['i_table__pk', undef,
				 'CREATE UNIQUE INDEX i_table__pk ON schema.table '.
				 'USING btree (column1)',
				 'table_pk', 'PRIMARY KEY', 1000],
				['i_table__idx2', 'tablespace',
				 'CREATE INDEX i_table__idx2 ON schema.table '.
				 'USING btree (column2) WHERE column2 = 1',
				 undef, undef, 2000]]},
		'reindex1' => {
			'sql_pattern' =>
				qr/CREATE UNIQUE INDEX CONCURRENTLY i_compactor_$$/.
				qr/ ON schema\.table USING btree \(column1\); /.
				qr/BEGIN; ALTER TABLE schema\.table DROP CONSTRAINT table_pk; /.
				qr/ALTER TABLE schema\.table ADD CONSTRAINT table_pk /.
				qr/PRIMARY KEY USING INDEX i_compactor_$$; END;/,
			'row_list' => []},
		'reindex2' => {
			'sql_pattern' =>
				qr/CREATE INDEX CONCURRENTLY i_compactor_$$ ON schema\.table /.
				qr/USING btree \(column2\) TABLESPACE tablespace /.
				qr/WHERE column2 = 1; /.
				qr/BEGIN; DROP INDEX schema\.i_table__idx2; /.
				qr/ALTER INDEX schema\.i_compactor_$$ /.
				qr/RENAME TO i_table__idx2; END;/,
			'row_list' => []},
		'get_table_data_list1' => {
			'sql_pattern' =>
				qr/SELECT schemaname, tablename /.
				qr/FROM pg_catalog\.pg_tables\nWHERE\s+/s.
				qr/schemaname NOT IN \('pg_catalog', 'information_schema'\) /.
				qr/AND\s+schemaname !~ 'pg_\.\*'\s+ORDER BY/s,
			'row_list' => [['schema1', 'table1'],['schema2', 'table2']]},
		'get_table_data_list2' => {
			'sql_pattern' =>
				qr/SELECT schemaname, tablename /.
				qr/FROM pg_catalog\.pg_tables\nWHERE\s+/s.
				qr/schemaname IN \('schema3', 'schema4'\) AND\s+/s.
				qr/schemaname NOT IN \('schema1', 'schema2'\) AND\s+/s.
				qr/tablename IN \('table3', 'table4'\) AND\s+/s.
				qr/tablename NOT IN \('table1', 'table2'\) AND\s+/s.
				qr/schemaname NOT IN \('pg_catalog', 'information_schema'\) /.
				qr/AND\s+schemaname !~ 'pg_\.\*'\s+ORDER BY/s,
			'row_list' => [['schema3', 'table3'],['schema4', 'table4']]},
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
				qr/SELECT datname FROM pg_catalog\.pg_database\nWHERE\s+/s.
				qr/datname NOT IN \('postgres', 'template0', 'template1'\)\n/.
				qr/ORDER BY pg_catalog\.pg_database_size/,
			'row_list' => [['dbname1'], ['dbname2']]},
		'get_dbname_list2' => {
			'sql_pattern' =>
				qr/SELECT datname FROM pg_catalog\.pg_database\nWHERE\s+/s.
				qr/datname IN \('dbname3', 'dbname4'\) AND\s+/s.
				qr/datname NOT IN \('dbname1', 'dbname2'\) AND\s+/s.
				qr/datname NOT IN \('postgres', 'template0', 'template1'\)\n/.
				qr/ORDER BY pg_catalog\.pg_database_size/,
			'row_list' => [['dbname3'], ['dbname4']]},
		'get_pgstattuple_schema_name' => {
			'sql_pattern' =>
				qr/SELECT nspname FROM pg_catalog\.pg_proc.+/s.
				qr/WHERE proname = 'pgstattuple' LIMIT 1/,
				'row_list' => []}};

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
