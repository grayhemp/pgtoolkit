package PgToolkit::Compactor::Table;

use parent qw(PgToolkit::Class);

use strict;
use warnings;

=head1 NAME

B<PgToolkit::Compactor::Table> - a table level processing for bloat reducing.

=head1 SYNOPSIS

	my $table_compactor = PgToolkit::Compactor::Table->new(
		database => $database,
		logger => $logger,
		schema_name => $schema_name,
		table_name => $table_name,
		min_page_count => 100,
		min_free_percent => 10,
		max_pages_per_round => 5,
		no_initial_vacuum => 0,
		no_routine_vacuum => 0,
		delay_constant => 1,
		delay_ratio => 2,
		force => 0,
		reindex => 0,
		print_reindex_queries => 0,
		progress_report_period => 60,
		use_pgstattuple => 0,
		pages_per_round_divisor = 1000,
		pages_before_vacuum_lower_divisor = 16,
		pages_before_vacuum_lower_threshold = 1000,
		pages_before_vacuum_upper_divisor = 50)


	$table_compactor->process();

=head1 DESCRIPTION

B<PgToolkit::Compactor::Table> class is an implementation of a table level
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

=item C<max_pages_per_round>

an upper threshold of pages to process per one round

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

states whether we should use pgstattuple to get statistics or not

=item C<pages_per_round_divisor>

is used to calculate a pages per round value, recommended to set to 1000

 min(
     max(1/pages_per_round_divisor of the real page count, 1),
     max_pages_per_round)

=item C<pages_before_vacuum_lower_divisor>

=item C<pages_before_vacuum_lower_threshold>

=item C<pages_before_vacuum_upper_divisor>

are used to calculate a pages before vacuum value, recommended to set to
16, 1000 and 50 respectively

 max(
     min(
         1/pages_before_vacuum_lower_divisor of the real page count,
         1000),
     1/pages_before_vacuum_upper_divisor of the expected page count,
     1)

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
	$self->{'_max_pages_per_round'} = $arg_hash{'max_pages_per_round'};
	$self->{'_no_initial_vacuum'} = $arg_hash{'no_initial_vacuum'};
	$self->{'_no_routine_vacuum'} = $arg_hash{'no_routine_vacuum'};
	$self->{'_delay_constant'} = $arg_hash{'delay_constant'};
	$self->{'_delay_ratio'} = $arg_hash{'delay_ratio'};
	$self->{'_force'} = $arg_hash{'force'};
	$self->{'_reindex'} = $arg_hash{'reindex'};
	$self->{'_print_reindex_queries'} = $arg_hash{'print_reindex_queries'};

	$self->{'_progress_report_period'} = $arg_hash{'progress_report_period'};
	$self->{'_use_pgstattuple'} = $arg_hash{'use_pgstattuple'};
	$self->{'_pages_per_round_divisor'} = $arg_hash{'pages_per_round_divisor'};
	$self->{'_pages_before_vacuum_lower_divisor'} =
		$arg_hash{'pages_before_vacuum_lower_divisor'};
	$self->{'_pages_before_vacuum_lower_threshold'} =
		$arg_hash{'pages_before_vacuum_lower_threshold'};
	$self->{'_pages_before_vacuum_upper_divisor'} =
		$arg_hash{'pages_before_vacuum_upper_divisor'};

	$self->{'_ident'} =
		$self->{'_database'}->quote_ident(
			string => $self->{'_schema_name'}).'.'.
		$self->{'_database'}->quote_ident(
			string => $self->{'_table_name'});
	$self->{'_log_ident'} =
		$self->{'_database'}->quote_ident(
			string => $self->{'_database'}->get_dbname()).'/'.$self->{'_ident'};

	$self->{'_logger'}->write(
		message => 'Scanning the table.',
		level => 'info',
		target => $self->{'_log_ident'});

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

	my $statistics;
	if (not $self->{'_is_processed'}) {
		$statistics = $self->_get_statistics();

		if (not $self->{'_force'} and
			$statistics->{'page_count'} < $self->{'_min_page_count'})
		{
			$self->_log_min_page_count(statistics => $statistics);
			$self->{'_is_processed'} = 1;
		}
	}

	if (not $self->{'_is_processed'} and not $self->{'_no_initial_vacuum'}) {
		$self->_log_vacuum(phrase => 'analyze initially');
		$self->_do_vacuum(analyze => 1);
		$statistics = $self->_get_statistics();

		if (not $self->{'_force'}) {
			if ($statistics->{'page_count'} < $self->{'_min_page_count'})
			{
				$self->_log_min_page_count(statistics => $statistics);
				$self->{'_is_processed'} = 1;
			}

			if ($statistics->{'free_percent'} <
				$self->{'_min_free_percent'} and not $self->{'_is_processed'})
			{
				$self->_log_min_free_percent(statistics => $statistics);
				$self->{'_is_processed'} = 1;
			}
		}
	}

	if (not $self->{'_is_processed'}) {
		$self->_log_statistics(
			statistics => $statistics,
			phrase => 'initially');

		my $column_ident = $self->{'_database'}->quote_ident(
			string => $self->_get_update_column());

		$self->_log_start_compacting(
			statistics => $statistics,
			column_ident => $column_ident);

		my $progress_report_time = $self->_time();
		my $vacuum_page_count = 0;
		my $expected_page_count = $statistics->{'page_count'};
		my $initial_statistics = {%{$statistics}};
		my $to_page = $statistics->{'page_count'} - 1;

		my $loop;
		for ($loop = $statistics->{'page_count'}; $loop > 0 ; $loop--) {
			my $start_time = $self->_time();

			my $last_to_page = $to_page;
			eval {
				$to_page = $self->_clean_pages(
					statistics => $statistics,
					column_ident => $column_ident,
					to_page => $last_to_page);
			};
			if ($@) {
				if ($@ =~ 'No more free space left in the table') {
					last;
				} else {
					die($@);
				}
			}

			$self->_sleep(
				$self->{'_delay_constant'} + $self->{'_delay_ratio'} *
				($self->_time() - $start_time));

			if ($self->_time() - $progress_report_time >=
				$self->{'_progress_report_period'} and
				$last_to_page != $to_page)
			{
				$self->_log_progress(
					initial_statistics => $initial_statistics,
					to_page => $to_page);
				$progress_report_time = $self->_time();
			}

			$expected_page_count -= $self->_get_pages_per_round(
				statistics => $statistics);
			$vacuum_page_count += ($last_to_page - $to_page);

			if (not $self->{'_no_routine_vacuum'} and
				$vacuum_page_count >= $self->_get_pages_before_vacuum(
					expected_page_count => $expected_page_count,
					statistics => $statistics))
			{
				$self->_log_vacuum(phrase => 'routinely');
				$self->_do_vacuum();

				$statistics = $self->_get_statistics();

				$self->_log_vacuum_state(
					expected_page_count => $expected_page_count,
					statistics => $statistics,
					to_page => $to_page,
					phrase => 'routine');

				$vacuum_page_count = 0;

				if ($to_page > $statistics->{'page_count'} - 1) {
					$to_page = $statistics->{'page_count'} - 1;
				}
			}
		}

		if ($loop == 0) {
			$self->_log_max_loops();
		}

		$self->_log_vacuum(phrase => 'analyze finally');

		$self->_do_vacuum(analyze => 1);
		$statistics = $self->_get_statistics();

		$self->_log_vacuum_state(
			expected_page_count => $expected_page_count,
			statistics => $statistics,
			to_page => $to_page,
			phrase => 'final');

		$self->_log_statistics(
			statistics => $statistics,
			phrase => 'finally');

		if ($self->{'_reindex'}) {
			$self->_log_reindex();
			$self->_reindex();
		}

		if ($self->{'_print_reindex_queries'}) {
			$self->_log_reindex_queries();
		}

		$self->{'_is_processed'} =
			$statistics->{'page_count'} < $to_page + 1 +
			$self->_get_pages_before_vacuum(
				expected_page_count => $expected_page_count,
				statistics => $statistics);
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

sub get_log_ident {
	my $self = shift;

	return $self->{'_log_ident'};
}

sub _log_has_special_triggers {
	my $self = shift;

	$self->{'_logger'}->write(
		message => ('Can not process the table as it has "always" and/or '.
					'"replica" triggers.'),
		level => 'warning',
		target => $self->{'_log_ident'});

	return;
}

sub _log_force {
	my $self = shift;

	$self->{'_logger'}->write(
		message => 'Forcing processing of the table.',
		level => 'notice',
		target => $self->{'_log_ident'});

	return;
}

sub _log_min_page_count {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Skipping the table as it has '.
			$arg_hash{'statistics'}->{'page_count'}.' pages and minimum '.
			$self->{'_min_page_count'}.' pages are required.'),
		level => 'notice',
		target => $self->{'_log_ident'});

	return;
}

sub _log_vacuum {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Performing vacuum '.$arg_hash{'phrase'}.' for the table.',
		level => 'info',
		target => $self->{'_log_ident'});

	return;
}

sub _log_min_free_percent {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Skipping the table as it has '.
			$arg_hash{'statistics'}->{'free_percent'}.'% of its '.
			'space to compact and the minimum required is '.
			$self->{'_min_free_percent'}.'%.'),
		level => 'notice',
		target => $self->{'_log_ident'});

	return;
}

sub _log_statistics {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			ucfirst($arg_hash{'phrase'}).' the table has '.
			$arg_hash{'statistics'}->{'page_count'}.' pages ('.
			$arg_hash{'statistics'}->{'total_page_count'}.
			' pages including toasts and indexes)'.
			(defined $arg_hash{'statistics'}->{'free_space'} ?
			 ', approximately '.$arg_hash{'statistics'}->{'free_percent'}.
			 '% of its space that is '.$arg_hash{'statistics'}->{'free_space'}.
			 ' bytes can be potentially released making it '.
			 ($arg_hash{'statistics'}->{'page_count'} -
			  $arg_hash{'statistics'}->{'effective_page_count'}).
			 ' pages less.' : '.')),
		level => 'info',
		target => $self->{'_log_ident'});

	return;
}

sub _log_start_compacting {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Compacting the table using the '.
			$arg_hash{'column_ident'}.' column by '.
			$self->_get_pages_per_round(statistics => $arg_hash{'statistics'}).
			' pages per round.'),
		level => 'info',
		target => $self->{'_log_ident'});

	return;
}

sub _log_progress {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'The table '.
			(defined $arg_hash{'initial_statistics'}->{'effective_page_count'} ?
			 'compacting progress is '.
			 int(
				 100 *
				 ($arg_hash{'to_page'} ?
				  ($arg_hash{'initial_statistics'}->{'page_count'} -
				   $arg_hash{'to_page'} - 1) /
				  ($arg_hash{'initial_statistics'}->{'page_count'} -
				   $arg_hash{'initial_statistics'}->{'effective_page_count'}) :
				  1)
			 ).'% with ' : 'has ').
			($arg_hash{'initial_statistics'}->{'page_count'} -
			 $arg_hash{'to_page'} - 1).' pages completed.'),
		level => 'info',
		target => $self->{'_log_ident'});

	return;
}

sub _log_vacuum_state {
	my ($self, %arg_hash) = @_;

	if ($arg_hash{'statistics'}->{'page_count'} >= $arg_hash{'to_page'} + 1 +
		$self->_get_pages_before_vacuum(
			expected_page_count => $arg_hash{'expected_page_count'},
			statistics => $arg_hash{'statistics'}))
	{
		$self->{'_logger'}->write(
			message => (
				ucfirst($arg_hash{'phrase'}).' vacuum of the table '.
				'has not managed to clean '.
				($arg_hash{'statistics'}->{'page_count'} -
				 $arg_hash{'to_page'} - 1).' pages.'),
			level => 'warning',
			target => $self->{'_log_ident'});
	} else {
		$self->{'_logger'}->write(
			message => (
				'There are '.$arg_hash{'statistics'}->{'page_count'}.
				' pages left in the table after '.$arg_hash{'phrase'}.
				' vacuum.'),
			level => 'info',
			target => $self->{'_log_ident'});
	}

	return;
}

sub _log_max_loops {
	my $self = shift;

	$self->{'_logger'}->write(
		message => 'The maximum compacting loops are exceeded for the table.',
		level => 'warning',
		target => $self->{'_log_ident'});

	return;
}

sub _log_reindex {
	my $self = shift;

	$self->{'_logger'}->write(
		message => 'Performing reindexing for the table.',
		level => 'info',
		target => $self->{'_log_ident'});

	return;
}

sub _log_reindex_queries {
	my $self = shift;

	$self->{'_logger'}->write(
		message => (
			'Reindex queries for the table:'."\n".
			join("\n", @{$self->_get_reindex_queries()})),
		level => 'notice',
		target => $self->{'_log_ident'});

	return;
}

sub _log_not_processed {
	my $self = shift;

	$self->{'_logger'}->write(
		message => 'Processing of the table has not been completed.',
		level => 'warning',
		target => $self->{'_log_ident'});

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
		'page_count' => $result->[0]->[0],
		'total_page_count' => $result->[0]->[1],
		'effective_page_count' => $result->[0]->[2],
		'free_percent' => $result->[0]->[3],
		'free_space' => $result->[0]->[4]};
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

	my $pages_per_round = $self->_get_pages_per_round(
		statistics => $arg_hash{'statistics'});
	my $result = $self->{'_database'}->execute(
		sql => <<SQL
SELECT _clean_pages(
    '$self->{'_ident'}', '$arg_hash{'column_ident'}',
    $arg_hash{'to_page'}, $pages_per_round)
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

		my $index_ident = $self->{'_database'}->quote_ident(
			string => $indexname);

		push(@{$query_list}, $definition);
		push(
			@{$query_list},
			'BEGIN; '.
			'DROP INDEX '.$schema_ident.'.'.$index_ident.'; '.
			'ALTER INDEX '.$schema_ident.'.i_compactor_'.$$.' '.
			'RENAME TO '.$index_ident.'; '.
			'END;');
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

sub _get_pages_per_round {
	my ($self, %arg_hash) = @_;

	return
		(sort {$a <=> $b}
		 (sort {$b <=> $a}
		  $arg_hash{'statistics'}->{'page_count'} /
		  $self->{'_pages_per_round_divisor'},
		  1)[0],
		 $self->{'_max_pages_per_round'})[0];
}

sub _get_pages_before_vacuum {
	my ($self, %arg_hash) = @_;

	return
		(sort {$b <=> $a}
		 (sort {$a <=> $b}
		  $arg_hash{'statistics'}->{'page_count'} /
		  $self->{'_pages_before_vacuum_lower_divisor'},
		  $self->{'_pages_before_vacuum_lower_threshold'})[0],
		 $arg_hash{'expected_page_count'} /
		 $self->{'_pages_before_vacuum_upper_divisor'},
		 1)[0];
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
