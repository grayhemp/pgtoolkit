package PgToolkit::Compactor::Database;

use parent qw(PgToolkit::Class);

use strict;
use warnings;

=head1 NAME

B<PgToolkit::Compactor::Database> - a database level processing for bloat
reducing.

=head1 SYNOPSIS

	my $database_compactor = PgToolkit::Compactor::Database->new(
		database => $database,
		logger => $logger,
		schema_compactor_constructor => $schema_compactor_constructor,
		schema_name_list => $schema_name_list,
		excluded_schema_name_list => $excluded_schema_name_list);

	$database_compactor->process();

=head1 DESCRIPTION

B<PgToolkit::Compactor::Database> class is an implementation of a database
level processing logic for bloat reducing mechanism.

=head3 Constructor arguments

=over 4

=item C<database>

a database object

=item C<logger>

a logger object

=item C<schema_compactor_constructor>

a schema compactor constructor code reference

=item C<schema_name_list>

a list of schema names to process

=item C<excluded_schema_name_list>

a list of schema names to exclude from processing.

=back

=cut

sub init {
	my ($self, %arg_hash) = @_;

	$self->{'_database'} = $arg_hash{'database'};
	$self->{'_logger'} = $arg_hash{'logger'};

	$self->{'_ident'} = $self->{'_database'}->quote_ident(
		string => $self->{'_database'}->get_dbname());

	$self->{'_logger'}->write(
		message => 'Scanning the '.$self->{'_ident'}.' database.',
		level => 'info');

	$self->{'_logger'}->write(
		message => (
			'Creating the processing stored function in the '.$self->{'_ident'}.
			' database.'),
		level => 'info');

	$self->_create_clean_pages_function();

	my %schema_name_hash = map(
		($_ => 1), @{$arg_hash{'schema_name_list'}} ?
		@{$arg_hash{'schema_name_list'}} : @{$self->_get_schema_name_list()});

	delete @schema_name_hash{@{$arg_hash{'excluded_schema_name_list'}}};

	my $use_pgstattuple = $self->_has_pgstattuple();

	$self->{'_schema_compactor_list'} = [];
	for my $schema_name (sort keys %schema_name_hash) {
		my $schema_compactor;
		eval {
			$schema_compactor = $arg_hash{'schema_compactor_constructor'}->(
				database => $self->{'_database'},
				schema_name => $schema_name,
				use_pgstattuple => $use_pgstattuple);
		};
		if ($@) {
			$self->{'_logger'}->write(
				message => (
					'Can not prepare the '.
					$self->{'_database'}->quote_ident(string => $schema_name).
					' schema to compacting, the following error has occured:'.
					"\n".$@),
				level => 'error');
		} else {
			push(@{$self->{'_schema_compactor_list'}}, $schema_compactor);
		}
	}

	return;
}

=head1 METHODS

=head2 B<process()>

Runs a bloat reducing process for the database.

=cut

sub process {
	my $self = shift;

	$self->{'_logger'}->write(
		message => 'Processing the '.$self->{'_ident'}.' database.',
		level => 'info');

	for my $schema_compactor (@{$self->{'_schema_compactor_list'}}) {
		if (not $schema_compactor->is_processed()) {
			$schema_compactor->process();
		}
	}

	$self->{'_logger'}->write(
		message => 'Finished processing the '.$self->{'_ident'}.' database.',
		level => 'info');

	if (not $self->is_processed()) {
		$self->{'_logger'}->write(
			message => (
				'Processing of the '.$self->{'_ident'}.' database has not '.
				'been completed.'),
			level => 'warning');
	}

	return;
}

=head2 B<is_processed()>

Tests if the database is processed.

=head3 Returns

True or false value.

=cut

sub is_processed {
	my $self = shift;

	my $result = 1;
	map(($result &&= $_->is_processed()),
		@{$self->{'_schema_compactor_list'}});

	return $result;
}

sub DESTROY {
	my $self = shift;

	$self->{'_logger'}->write(
		message => (
			'Dropping the cleaning stored function in the '.$self->{'_ident'}.
			' database.'),
		level => 'info');

	$self->_drop_clean_pages_function();
}

sub _has_pgstattuple {
	my $self = shift;

	my $result = $self->{'_database'}->execute(
			sql => <<SQL
SELECT sign(count(1)) FROM pg_proc WHERE proname = 'pgstattuple'
SQL
		);

	return $result->[0]->[0];
}

sub _get_schema_name_list {
	my $self = shift;

	my $result = $self->{'_database'}->execute(
			sql => <<SQL
SELECT nspname FROM pg_namespace
WHERE nspname NOT IN ('pg_catalog', 'information_schema') AND nspname !~ 'pg_.*'
ORDER BY 1
SQL
		);

	return [map($_->[0], @{$result})];
}

sub _create_clean_pages_function {
	my $self = shift;

	$self->{'_database'}->execute(
		sql => << 'SQL'
CREATE OR REPLACE FUNCTION _clean_pages(
    i_table_ident text,
    i_column_ident text,
    i_to_page integer,
    i_page_offset integer)
RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    _from_page integer := i_to_page - i_page_offset + 1;
    _max_tuples_per_page integer;
    _min_ctid tid;
    _max_ctid tid;
    _ctid_list tid[];
    _next_ctid_list tid[];
    _ctid tid;
    _loop integer;
    _result_page integer;
BEGIN
    -- Check page argument values
    IF NOT (
        i_page_offset IS NOT NULL OR i_page_offset > 1 OR
        i_to_page IS NOT NULL OR i_to_page > 1 OR
        i_to_page > i_page_offset)
    THEN
        RAISE EXCEPTION 'Wrong page arguments specified.';
    END IF;

    -- Check if there are no always or replica triggers on update
    IF EXISTS(
        SELECT 1 FROM pg_trigger
        WHERE
            pg_trigger.tgrelid = i_table_ident::regclass AND
            tgtype & 16 = 8 AND
            tgenabled IN ('A', 'R'))
    THEN
        RAISE EXCEPTION
            'Can not process a table with A or R triggers on update.';
    END IF;

    -- Prevent triggers firing on update
    SET LOCAL session_replication_role TO replica;

    -- Calculate the maximum possible number of tuples per page for
    -- the table
    SELECT ceil(current_setting('block_size')::real / sum(attlen))
    INTO _max_tuples_per_page
    FROM pg_attribute
    WHERE
        attrelid = i_table_ident::regclass AND
        attnum < 0;

    -- Define minimal and maximal ctid values of the range
    _min_ctid := (_from_page, 1)::text::tid;
    _max_ctid := (i_to_page, _max_tuples_per_page)::text::tid;

    -- Build a list of possible ctid values of the range
    SELECT array_agg((pi, ti)::text::tid)
    INTO _ctid_list
    FROM generate_series(_from_page, i_to_page) AS pi
    CROSS JOIN generate_series(1, _max_tuples_per_page) AS ti;

    <<_outer_loop>>
    FOR _loop IN 1.._max_tuples_per_page LOOP
        _next_ctid_list := array[]::tid[];

        -- Update all the tuples in the range
        FOR _ctid IN EXECUTE
            'UPDATE ONLY ' || i_table_ident || ' ' ||
            'SET ' || i_column_ident || ' = ' || i_column_ident || ' ' ||
            'WHERE ctid = ANY($1) RETURNING ctid' USING _ctid_list
        LOOP
            IF _ctid > _max_ctid THEN
                RAISE EXCEPTION 'No more free space left in the table.';
            ELSIF _ctid >= _min_ctid THEN
                -- The tuple is still in the range, more updates are needed
                _next_ctid_list := _next_ctid_list || _ctid;
            END IF;
        END LOOP;

        _ctid_list := _next_ctid_list;

        -- Finish processing if there are no tupples in the range left
        IF coalesce(array_length(_ctid_list, 1), 0) = 0 THEN
            _result_page := _from_page - 1;
            EXIT _outer_loop;
        END IF;
    END LOOP;

    -- No result
    IF _loop = _max_tuples_per_page AND _result_page IS NULL THEN
        RAISE EXCEPTION
            'Maximal loops count has been reached with no result.';
    END IF;

    RETURN _result_page;
END $$;
SQL
		);

	return;
}

sub _drop_clean_pages_function {
	my $self = shift;

	$self->{'_database'}->execute(
		sql => <<SQL
DROP FUNCTION _clean_pages(text, text, integer, integer);
SQL
		);

	return;
}

=head1 SEE ALSO

=over 4

=item L<PgToolkit::Class>

=back

=head1 LICENSE AND COPYRIGHT

Copyright 2010-2011 postgresql-consulting.com

TODO Licence boilerplate

=head1 AUTHOR

=over 4

=item L<Sergey Konoplev|mailto:sergey.konoplev@postgresql-consulting.com>

=back

=cut

1;
