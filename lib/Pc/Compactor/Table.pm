package Pc::Compactor::Table;

use parent qw(Pc::Class);

use strict;
use warnings;

=head1 NAME

B<Pc::Compactor::Table> - a table level processing for bloat reducing.

=head1 SYNOPSIS

	my $table_compactor = Pc::Compactor::Table->new(
		database => $database,
		logger => $logger,
		schema_name => $schema_name,
		table_name => $table_name,
		min_page_count => 100,
		min_free_percent => 10,
		pages_per_round => 5,
		no_initial_vacuum => 0,
		no_routine_vacuum => 0,
		delay_constant => 1,
		delay_ratio => 2,
		force => 0,
		reindex => 0,
		print_reindex_queries => 0,
		progress_report_period => 60,
		use_pgstattuple => 0);

	$table_compactor->process();

=head1 DESCRIPTION

B<Pc::Compactor::Table> class is an implementation of a table level
processing logic for bloat reducing mechanism.

=head3 Constructor arguments

=over 4

=item C<database>

a database object

=item C<logger>

a logger object

=item C<schema_name>

a schema name to process

=item C<table_name>

a table name to process

=item C<min_page_count>

a minimum number of pages that is worth to compact with

=item C<min_free_percent>

a mininum free space percent that is worth to compact with

=item C<pages_per_round>

a number of pages to process per one round

=item C<no_initial_vacuum>

perform no initial vacuum

=item C<no_routine_vacuum>

perform no routine vacuum

=item C<delay_constant>

the constant part of the delay between rounds in seconds

=item C<delay_ratio>

the dynamic part of the delay between rounds

=item C<force>

process the table even if it does not meet the minimum pages and free
space

=item C<reindex>

reindex the table after compacting

=item C<print_reindex_queries>

logs reindex queries after processing

=item C<progress_report_period>

a period in seconds to report the progress with

=item C<use_pgstattuple>

states whether we should use pgstattuple to get statistics or not.

=back

=cut

sub init {
	my ($self, %arg_hash) = @_;

	$self->{'_database'} = $arg_hash{'database'};
	$self->{'_logger'} = $arg_hash{'logger'};
	$self->{'_schema_name'} = $arg_hash{'schema_name'};
	$self->{'_table_name'} = $arg_hash{'table_name'};

	$self->{'_min_page_count'} = $arg_hash{'min_page_count'};
	$self->{'_min_free_percent'} = $arg_hash{'min_free_percent'};
	$self->{'_pages_per_round'} = $arg_hash{'pages_per_round'};
	$self->{'_no_initial_vacuum'} = $arg_hash{'no_initial_vacuum'};
	$self->{'_no_routine_vacuum'} = $arg_hash{'no_routine_vacuum'};
	$self->{'_delay_constant'} = $arg_hash{'delay_constant'};
	$self->{'_delay_ratio'} = $arg_hash{'delay_ratio'};
	$self->{'_force'} = $arg_hash{'force'};
	$self->{'_reindex'} = $arg_hash{'reindex'};
	$self->{'_print_reindex_queries'} = $arg_hash{'print_reindex_queries'};

	$self->{'_progress_report_period'} = $arg_hash{'progress_report_period'};
	$self->{'_use_pgstattuple'} = $arg_hash{'use_pgstattuple'};

	$self->{'_ident'} =
		$self->{'_database'}->quote_ident(
			string => $self->{'_schema_name'}).'.'.
		$self->{'_database'}->quote_ident(
			string => $self->{'_table_name'});

	$self->{'_logger'}->write(
		message => 'Scanning the '.$self->{'_ident'}.' table.',
		level => 'info');

	$self->{'_is_processed'} = 0;

	return;
}

=head1 METHODS

=head2 B<process()>

Runs a bloat reducing process for the schema.

=cut

sub process {
	my $self = shift;

	if ($self->_has_special_triggers()) {
		$self->_log_has_special_triggers();
		$self->{'_is_processed'} = 1;
	}

	if (not $self->{'_is_processed'} and $self->{'_force'}) {
		$self->_log_force();
	}

	if (not $self->{'_is_processed'}) {
		$self->{'_stat'} = $self->_get_statistics();

		if (not $self->{'_force'} and
			$self->{'_stat'}->{'_page_count'} < $self->{'_min_page_count'})
		{
			$self->_log_min_page_count();
			$self->{'_is_processed'} = 1;
		}
	}

	if (not $self->{'_is_processed'} and not $self->{'_no_initial_vacuum'}) {
		$self->_log_vacuum(type => 'analyze initially');
		$self->_do_vacuum(analyze => 1);
		$self->{'_stat'} = $self->_get_statistics();

		if (not $self->{'_force'}) {
			if ($self->{'_stat'}->{'_page_count'} < $self->{'_min_page_count'})
			{
				$self->_log_min_page_count();
				$self->{'_is_processed'} = 1;
			}

			if ($self->{'_stat'}->{'_free_percent'} <
				$self->{'_min_free_percent'} and not $self->{'_is_processed'})
			{
				$self->_log_min_free_percent();
				$self->{'_is_processed'} = 1;
			}
		}
	}

	if (not $self->{'_is_processed'}) {
		$self->_log_statistics(type => 'initially');

		if (not defined $self->{'_column_ident'}) {
			$self->{'_column_ident'} = $self->{'_database'}->quote_ident(
				string => $self->_get_update_column());
		}

		$self->_log_start_compacting();

		my $progress_report_time = $self->_time();
		my $vacuum_page_count = 0;
		my $to_page = $self->{'_stat'}->{'_page_count'} - 1;

		$self->{'_initial_stat'} = {%{$self->{'_stat'}}};

		my $loop;
		for ($loop = $self->{'_initial_stat'}->{'_page_count'};
			 $loop > 0 ; $loop--)
		{
			my $start_time = $self->_time();

			my $last_to_page = $to_page;
			eval {
				$to_page = $self->_clean_pages('to_page' => $last_to_page);
			};
			if ($@) {
				if ($@ =~ 'No more free space left in the table') {
					last;
				} else {
					die($@);
				}
			}

			$self->_sleep(
				$self->{'_delay_constant'} +
				$self->{'_delay_ratio'} * ($self->_time() - $start_time));

			if ($self->_time() - $progress_report_time >=
				$self->{'_progress_report_period'} and
				$last_to_page != $to_page)
			{
				$self->_log_progress(to_page => $to_page);
				$progress_report_time = $self->_time();
			}

			$vacuum_page_count += ($last_to_page - $to_page);
			if (not $self->{'_no_routine_vacuum'} and
				$vacuum_page_count >= $self->_get_pages_before_vacuum())
			{
				$self->_log_vacuum(type => 'routinely');
				$self->_do_vacuum();

				$self->{'_stat'} = $self->_get_statistics();

				$self->_log_vacuum_state(
					to_page => $to_page,
					type => 'routine');

				$vacuum_page_count = 0;

				if ($to_page > $self->{'_stat'}->{'_page_count'} - 1) {
					$to_page = $self->{'_stat'}->{'_page_count'} - 1;
				}
			}
		}

		if ($loop == 0) {
			$self->_log_max_loops();
		}

		$self->_log_vacuum(type => 'analyze finally');

		$self->_do_vacuum(analyze => 1);
		$self->{'_stat'} = $self->_get_statistics();

		$self->_log_vacuum_state(
			to_page => $to_page,
			type => 'final');

		$self->_log_statistics(type => 'finally');

		if ($self->{'_reindex'}) {
			$self->_log_reindex();
			$self->_reindex();
		}

		if ($self->{'_print_reindex_queries'}) {
			$self->_log_reindex_queries();
		}

		$self->{'_is_processed'} = (
			$self->{'_stat'}->{'_page_count'} < $to_page + 1 +
			$self->_get_pages_before_vacuum());
	}

	if (not $self->{'_is_processed'}) {
		$self->_log_not_processed();
	}

	return;
}

=head2 B<is_processed()>

Tests if the table is processed.

=head3 Returns

True or false value.

=cut

sub is_processed {
	my $self = shift;

	return $self->{'_is_processed'};
}

=head2 B<get_ident()>

Returns a table ident.

=head3 Returns

A string representing the ident.

=cut

sub get_ident {
	my $self = shift;

	return $self->{'_ident'};
}

sub _log_has_special_triggers {
	my $self = shift;

	$self->{'_logger'}->write(
		message => ('Can not process the '.$self->{'_ident'}.' table'.
					' as it has "always" and/or "replica" triggers.'),
		level => 'warning');

	return;
}

sub _log_force {
	my $self = shift;

	$self->{'_logger'}->write(
		message => (
			'Forcing processing of the '.$self->{'_ident'}.' table.'),
		level => 'notice');

	return;
}

sub _log_min_page_count {
	my $self = shift;

	$self->{'_logger'}->write(
		message => (
			'Skipping the '.$self->{'_ident'}.' table as it has '.
			$self->{'_stat'}->{'_page_count'}.' pages and minimum '.
			$self->{'_min_page_count'}.' pages are required.'),
		level => 'notice');

	return;
}

sub _log_vacuum {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => ('Performing vacuum '.$arg_hash{'type'}.' for the '.
					$self->{'_ident'}.' table.'),
		level => 'info');

	return;
}

sub _log_min_free_percent {
	my $self = shift;

	$self->{'_logger'}->write(
		message => (
			'Skipping the '.$self->{'_ident'}.' table as it '.
			'has '.$self->{'_stat'}->{'_free_percent'}.'% of its '.
			'space to compact and the minimum required is '.
			$self->{'_min_free_percent'}.'%.'),
		level => 'notice');

	return;
}

sub _log_statistics {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			ucfirst($arg_hash{'type'}).' the '.$self->{'_ident'}.
			' table has '.$self->{'_stat'}->{'_page_count'}.' pages ('.
			$self->{'_stat'}->{'_total_page_count'}.
			' pages including toasts and indexes)'.
			(defined $self->{'_stat'}->{'_free_space'} ? ', approximately '.
			 $self->{'_stat'}->{'_free_percent'}.'% of its space that is '.
			 $self->{'_stat'}->{'_free_space'}.' bytes can be potentially '.
			 'released making it '.
			 ($self->{'_stat'}->{'_page_count'} -
			  $self->{'_stat'}->{'_effective_page_count'}).
			 ' pages less.' : '.')),
		level => 'notice');

	if ($self->{'_use_pgstattuple'}) {
		$self->{'_logger'}->write(
			message => 'pgstattuple is used to calculate statistics.',
			level => 'info');
	}

	return;
}

sub _log_start_compacting {
	my $self = shift;

	$self->{'_logger'}->write(
		message => (
			'Compacting the '.$self->{'_ident'}.' table using the '.
			$self->{'_column_ident'}.' column by '.$self->{'_pages_per_round'}.
			' pages per round.'),
		level => 'info');

	return;
}

sub _log_progress {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'The '.$self->{'_ident'}.' table '.
			(defined $self->{'_initial_stat'}->{'_effective_page_count'} ?
			 'compacting progress is '.
			 int(
				 100 *
				 ($arg_hash{'to_page'} ?
				  ($self->{'_initial_stat'}->{'_page_count'} -
				   $arg_hash{'to_page'} - 1) /
				  ($self->{'_initial_stat'}->{'_page_count'} -
				   $self->{'_initial_stat'}->{'_effective_page_count'}) :
				  1)
			 ).'% with ' : 'has ').
			($self->{'_initial_stat'}->{'_page_count'} -
			 $arg_hash{'to_page'} - 1).' pages completed.'),
		level => 'notice');

	return;
}

sub _log_vacuum_state {
	my ($self, %arg_hash) = @_;

	if ($self->{'_stat'}->{'_page_count'} >= $arg_hash{'to_page'} + 1 +
		$self->_get_pages_before_vacuum())
	{
		$self->{'_logger'}->write(
			message => (
				ucfirst($arg_hash{'type'}).' vacuum of the '.$self->{'_ident'}.
				' has not managed to clean '.
				($self->{'_stat'}->{'_page_count'} - $arg_hash{'to_page'} - 1).
				' pages.'),
			level => 'warning');
	} else {
		$self->{'_logger'}->write(
			message => (
				'There are '.$self->{'_stat'}->{'_page_count'}.
				' pages left in the '.$self->{'_ident'}.' table after '.
				$arg_hash{'type'}.' vacuum.'),
			level => 'info');
	}

	return;
}

sub _log_max_loops {
	my $self = shift;

	$self->{'_logger'}->write(
		message => (
			'The maximum compacting loops are exceeded for the '.
			$self->{'_ident'}.' table.'),
		level => 'warning');

	return;
}

sub _log_reindex {
	my $self = shift;

	$self->{'_logger'}->write(
		message => 'Performing reindexing for the '.$self->{'_ident'}.' table.',
		level => 'notice');

	return;
}

sub _log_reindex_queries {
	my $self = shift;

	$self->{'_logger'}->write(
		message => (
			'Reindex queries for the '.$self->{'_ident'}." table:\n".
			join("\n", @{$self->_get_reindex_queries()})),
		level => 'notice');

	return;
}

sub _log_not_processed {
	my $self = shift;

	$self->{'_logger'}->write(
		message => (
			'Processing of the '.$self->{'_ident'}.' table has not '.
			'been completed.'),
		level => 'warning');

	return;
}

sub _sleep {
	my ($self, $time) = @_;

	sleep($time);

	return;
}

sub _time {
	my ($self, $time) = @_;

	return time();
}

sub _has_special_triggers {
	my $self = shift;

	my $result = $self->{'_database'}->execute(
		sql => <<SQL
SELECT count(1) FROM pg_trigger
WHERE
    tgrelid = '$self->{'_ident'}'::regclass AND
    tgtype & 16 = 8 AND
    tgenabled IN ('A', 'R')
SQL
		);

	return $result->[0]->[0];
}

sub _get_statistics {
	my $self = shift;

	my $result;
	if ($self->{'_use_pgstattuple'}) {
		$result = $self->{'_database'}->execute(
			sql => <<SQL
SELECT
    pg_relpages('$self->{'_ident'}') AS page_count,
    ceil(
        pg_total_relation_size('$self->{'_ident'}')::real /
        current_setting('block_size')::integer
    ) AS total_page_count,
    CASE
        WHEN free_percent = 0 THEN pg_relpages('$self->{'_ident'}')
        ELSE
            ceil(
                pg_relpages('$self->{'_ident'}') *
                (1 - free_percent / 100)
            )
        END as effective_page_count,
    free_percent, free_space
FROM pgstattuple('$self->{'_ident'}');
SQL
			);
	} else {
		$result = $self->{'_database'}->execute(
			sql => <<SQL
SELECT
    page_count, total_page_count, effective_page_count,
    CASE
        WHEN
            effective_page_count = 0 OR page_count <= 1 OR
            page_count < effective_page_count
        THEN 0
        ELSE
            round(
                100 * (
                    (page_count - effective_page_count)::real /
                    page_count
                )::numeric, 2
            )
        END AS free_percent,
    CASE
        WHEN page_count < effective_page_count THEN 0
        ELSE
            round(
                current_setting('block_size')::integer *
                (page_count - effective_page_count)
            )
        END AS free_space
FROM (
    SELECT
        ceil(
            pg_relation_size(pg_class.oid)::real /
            current_setting('block_size')::integer
        ) AS page_count,
        ceil(
            pg_total_relation_size(pg_class.oid)::real /
            current_setting('block_size')::integer
        ) AS total_page_count,
        ceil(
            reltuples * (
                max(stanullfrac) * ma * ceil(
                    (
                        ma * ceil(
                            (
                                header_width +
                                ma * ceil(count(1)::real / ma)
                            )::real / ma
                        ) + sum((1 - stanullfrac) * stawidth)
                    )::real / ma
                ) +
                (1 - max(stanullfrac)) * ma * ceil(
                    (
                        ma * ceil(header_width::real / ma) +
                        sum((1 - stanullfrac) * stawidth)
                    )::real / ma
                )
            )::real / (current_setting('block_size')::integer - 24)
        ) AS effective_page_count
    FROM pg_class
    LEFT JOIN pg_statistic ON starelid = pg_class.oid
    CROSS JOIN (SELECT 23 AS header_width, 8 AS ma) AS const
    WHERE pg_class.oid = '$self->{'_ident'}'::regclass
    GROUP BY pg_class.oid, reltuples, header_width, ma
) AS sq
SQL
			);
	}

	return {
		'_page_count' => $result->[0]->[0],
		'_total_page_count' => $result->[0]->[1],
		'_effective_page_count' => $result->[0]->[2],
		'_free_percent' => $result->[0]->[3],
		'_free_space' => $result->[0]->[4]};
}

sub _do_vacuum {
	my ($self, %arg_hash) = @_;

	$self->{'_database'}->execute(
		sql => ('VACUUM '.($arg_hash{'analyze'} ? 'ANALYZE ' : '').
				$self->{'_ident'}));

	return;
}

sub _get_update_column {
	my $self = shift;

	my $result = $self->{'_database'}->execute(
		sql => <<SQL
SELECT attname
FROM pg_catalog.pg_attribute
WHERE
    attnum > 0 AND -- neither system
    NOT attisdropped AND -- nor dropped
    attrelid = '$self->{'_ident'}'::regclass
ORDER BY
    -- Variable legth attributes have lower priority because of the chance
    -- of being toasted
    (attlen = -1),
    -- Preferably not indexed attributes
    (
        attnum::text IN (
            SELECT regexp_split_to_table(indkey::text, ' ')
            FROM pg_index WHERE indrelid = '$self->{'_ident'}'::regclass)),
    -- Preferably smaller attributes
    attlen,
    attnum
LIMIT 1;
SQL
		);

	return $result->[0]->[0];
}

sub _clean_pages {
	my ($self, %arg_hash) = @_;

	my $result = $self->{'_database'}->execute(
		sql => <<SQL
SELECT _clean_pages(
    '$self->{'_ident'}', '$self->{'_column_ident'}',
    $arg_hash{'to_page'}, $self->{'_pages_per_round'})
SQL
		);

	return $result->[0]->[0];
}

sub _get_reindex_queries {
	my $self = shift;

	my $result = $self->{'_database'}->execute(
		sql => <<SQL
SELECT indexname, tablespace, indexdef FROM pg_indexes
WHERE
    schemaname = '$self->{'_schema_name'}' AND
    tablename = '$self->{'_table_name'}' AND
    NOT EXISTS (
        SELECT 1 FROM pg_depend
        WHERE
            deptype='i' AND
            objid = (quote_ident(schemaname) || '.' ||
                     quote_ident(indexname))::regclass) AND
    NOT EXISTS (
        SELECT 1 FROM pg_depend
        WHERE
            deptype='n' AND
            refobjid = (quote_ident(schemaname) || '.' ||
                        quote_ident(indexname))::regclass)
ORDER BY indexdef
SQL
		);

	my $schema_ident = $self->{'_database'}->quote_ident(
		string => $self->{'_schema_name'});

	my $query_list = [];
	for my $row (@{$result}) {
		my ($indexname, $tablespace, $definition) = @{$row};

		$definition =~ s/INDEX (\S+)/INDEX CONCURRENTLY i_compactor_$$/;
		if (defined $tablespace) {
			$definition =~ s/(WHERE .*)?$/TABLESPACE $tablespace $1/;
		}

		my $index_ident = $schema_ident.'.'.$self->{'_database'}->quote_ident(
			string => $indexname);

		push(@{$query_list}, $definition);
		push(@{$query_list}, 'DROP INDEX '.$index_ident);
		push(@{$query_list},
			 ('ALTER INDEX '.$schema_ident.'.i_compactor_'.$$.
			  ' RENAME TO '.$self->{'_database'}->quote_ident(
				  string => $indexname)));
	}

	return $query_list;
}

sub _reindex {
	my $self = shift;

	for my $query (@{$self->_get_reindex_queries()}) {
		$self->{'_database'}->execute(sql => $query);
	}

	return;
}

sub _get_pages_before_vacuum {
	my $self = shift;

	my $result = 1000;
	if ($self->{'_stat'}->{'_page_count'} / 16 < 1000) {
		$result = $self->{'_stat'}->{'_page_count'} / 16;
	}

	return $result;
}

=head1 SEE ALSO

=over 4

=item L<Pc::Class>

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
