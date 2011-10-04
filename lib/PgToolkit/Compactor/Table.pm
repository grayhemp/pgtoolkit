package PgToolkit::Compactor::Table;

use parent qw(PgToolkit::Class);

use strict;
use warnings;

use POSIX;
use Time::HiRes qw(time sleep);

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
	$self->{'_log_ident'} = $self->{'_database'}->quote_ident(
		string => $self->{'_database'}->get_dbname()).', '.$self->{'_ident'};

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
		$self->_log_can_not_process_ar_triggers();
		$self->{'_is_processed'} = 1;
	}

	my $statistics;
	if (not $self->{'_is_processed'}) {
		$statistics = $self->_get_statistics();

		if (not $self->{'_force'} and
			$statistics->{'page_count'} < $self->{'_min_page_count'})
		{
			$self->_log_skipping_min_page_count(statistics => $statistics);
			$self->{'_is_processed'} = 1;
		}
	}

	my $timing;
	if (not $self->{'_is_processed'} and not $self->{'_no_initial_vacuum'}) {
		#$self->_log_vacuum_starting(phrase => 'analyze initial');
		$self->_do_vacuum(analyze => 1, timing => \ $timing);
		$statistics = $self->_get_statistics();
		$self->_log_vacuum_complete(
			statistics => $statistics,
			timing => $timing,
			to_page => $statistics->{'page_count'} - 1,
			phrase => 'analyze initial');

		if (not $self->{'_force'}) {
			if ($statistics->{'page_count'} < $self->{'_min_page_count'})
			{
				$self->_log_skipping_min_page_count(statistics => $statistics);
				$self->{'_is_processed'} = 1;
			}

			if ($statistics->{'free_percent'} <
				$self->{'_min_free_percent'} and not $self->{'_is_processed'})
			{
				$self->_log_skipping_min_free_percent(
					statistics => $statistics);
				$self->{'_is_processed'} = 1;
			}
		}
	}

	if (not $self->{'_is_processed'}) {
		if ($self->{'_force'}) {
			$self->_log_forced_processing();
		} else {
			$self->_log_processing();
		}

		$self->_log_statistics(statistics => $statistics, phrase => 'initial');

		my $expected_page_count = $statistics->{'page_count'};
		my $column_ident = $self->{'_database'}->quote_ident(
			string => $self->_get_update_column());
		my $pages_per_round = $self->_get_pages_per_round(
			statistics => $statistics);
		my $pages_before_vacuum = $self->_get_pages_before_vacuum(
			expected_page_count => $expected_page_count,
			statistics => $statistics);
		$self->_log_column(name => $column_ident);
		$self->_log_pages_per_round(value => $pages_per_round);
		$self->_log_pages_before_vacuum(value => $pages_before_vacuum);

		my $vacuum_page_count = 0;
		my $initial_statistics = {%{$statistics}};
		my $to_page = $statistics->{'page_count'} - 1;
		my $progress_report_time = $self->_time();
		my $clean_pages_total_timing = 0;
		my $last_loop = $statistics->{'page_count'};

		my $loop;
		for ($loop = $statistics->{'page_count'}; $loop > 0 ; $loop--) {
			my $start_time = $self->_time();

			my $last_to_page = $to_page;
			eval {
				$to_page = $self->_clean_pages(
					statistics => $statistics,
					timing => \ $timing,
					column_ident => $column_ident,
					to_page => $last_to_page,
					pages_per_round => $pages_per_round);
				$clean_pages_total_timing = $clean_pages_total_timing + $timing;
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

			$expected_page_count -= $pages_per_round;
			$vacuum_page_count += ($last_to_page - $to_page);

			if (not $self->{'_no_routine_vacuum'} and
				$vacuum_page_count >= $pages_before_vacuum)
			{
				$self->_log_clean_pages_average(
					pages_per_round => $pages_per_round,
					timing => $clean_pages_total_timing / ($last_loop - $loop));
				$clean_pages_total_timing = 0;
				$last_loop = $loop;

				#$self->_log_vacuum_starting(phrase => 'routine');
				$self->_do_vacuum(timing => \ $timing);
				$statistics = $self->_get_statistics();
				$self->_log_vacuum_complete(
					statistics => $statistics,
					timing => $timing,
					to_page => $to_page,
					phrase => 'routine');

				$vacuum_page_count = 0;

				my $last_pages_per_round = $pages_per_round;
				$pages_per_round = $self->_get_pages_per_round(
					statistics => $statistics);
				if ($last_pages_per_round != $pages_per_round) {
					$self->_log_pages_per_round(
						value => $pages_per_round);
				}

				my $last_pages_before_vacuum = $pages_before_vacuum;
				$pages_before_vacuum = $self->_get_pages_before_vacuum(
					expected_page_count => $expected_page_count,
					statistics => $statistics);
				if ($last_pages_before_vacuum != $pages_before_vacuum) {
					$self->_log_pages_before_vacuum(
						value => $pages_before_vacuum);
				}

				if ($to_page > $statistics->{'page_count'} - 1) {
					$to_page = $statistics->{'page_count'} - 1;
				}
			}
		}

		if ($loop == 0) {
			$self->_log_max_loops();
		}

		#$self->_log_vacuum_starting(phrase => 'analyze final');
		$self->_do_vacuum(analyze => 1, timing => \ $timing);
		$statistics = $self->_get_statistics();
		$self->_log_vacuum_complete(
			statistics => $statistics,
			timing => $timing,
			to_page => $to_page + $pages_per_round,
			phrase => 'analyze final');

		$self->_log_statistics(
			statistics => $statistics,
			phrase => 'final');

		if ($self->{'_reindex'}) {
			#$self->_log_reindex_starting();
			$self->_reindex(timing => \ $timing);
			$self->_log_reindex_complete(timing => $timing);
		}

		if ($self->{'_print_reindex_queries'}) {
			$self->_log_reindex_queries();
		}

		$self->{'_is_processed'} =
			$statistics->{'page_count'} <= $to_page + 1 + $pages_per_round;
	}

	if (not $self->{'_is_processed'}) {
		$self->_log_processing_incomplete();
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

sub _log_can_not_process_ar_triggers {
	my $self = shift;

	$self->{'_logger'}->write(
		message => (
			'Can not process: "always" or "replica" triggers are on.'),
		level => 'warning',
		target => $self->{'_log_ident'});

	return;
}

sub _log_skipping_min_page_count {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Skipping processing: '.$arg_hash{'statistics'}->{'page_count'}.
			' pages from '.$self->{'_min_page_count'}.' minimum required.'),
		level => 'notice',
		target => $self->{'_log_ident'});

	return;
}

sub _log_vacuum_starting {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Vacuum '.$arg_hash{'phrase'}.' starting...',
		level => 'info',
		target => $self->{'_log_ident'});

	return;
}

sub _log_vacuum_complete {
	my ($self, %arg_hash) = @_;

	if ($arg_hash{'statistics'}->{'page_count'} > $arg_hash{'to_page'} + 1) {
		$self->{'_logger'}->write(
			message => (
				'Vacuum '.$arg_hash{'phrase'}.': '.
				sprintf("%.3f", $arg_hash{'timing'}).' s, can not clean '.
				($arg_hash{'statistics'}->{'page_count'} -
				 $arg_hash{'to_page'} - 1).' pages, '.
				$arg_hash{'statistics'}->{'page_count'}.' pages left.'),
			level => 'notice',
			target => $self->{'_log_ident'});
	} else {
		$self->{'_logger'}->write(
			message => (
				'Vacuum '.$arg_hash{'phrase'}.': '.
				sprintf("%.3f", $arg_hash{'timing'}).
				' s, '.$arg_hash{'statistics'}->{'page_count'}.' pages left.'),
			level => 'info',
			target => $self->{'_log_ident'});
	}

	return;
}

sub _log_skipping_min_free_percent {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Skipping processing: '.$arg_hash{'statistics'}->{'free_percent'}.
			'% space to compact from '.$self->{'_min_free_percent'}.
			'% minimum required.'),
		level => 'notice',
		target => $self->{'_log_ident'});

	return;
}

sub _log_forced_processing {
	my $self = shift;

	$self->{'_logger'}->write(
		message => 'Forced processing.',
		level => 'notice',
		target => $self->{'_log_ident'});

	return;
}

sub _log_processing {
	my $self = shift;

	$self->{'_logger'}->write(
		message => 'Processing.',
		level => 'notice',
		target => $self->{'_log_ident'});

	return;
}

sub _log_statistics {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Statistics '.$arg_hash{'phrase'}.': '.
			$arg_hash{'statistics'}->{'page_count'}.' pages ('.
			$arg_hash{'statistics'}->{'total_page_count'}.
			' including toasts and indexes)'.
			(defined $arg_hash{'statistics'}->{'free_space'} ?
			 ', approximately '. $arg_hash{'statistics'}->{'free_percent'}.
			 '% ('.$arg_hash{'statistics'}->{'free_space'}.' bytes, '.
			 ($arg_hash{'statistics'}->{'page_count'} -
			  $arg_hash{'statistics'}->{'effective_page_count'}).' pages) '.
			 'is expected to be compacted' : '').
			'.'),
		level => 'info',
		target => $self->{'_log_ident'});

	return;
}

sub _log_column {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Column to perform updates by: '.$arg_hash{'name'}.'.',
		level => 'info',
		target => $self->{'_log_ident'});

	return;
}

sub _log_pages_per_round {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Pages to process per round: '.$arg_hash{'value'}.'.',
		level => 'info',
		target => $self->{'_log_ident'});

	return;
}

sub _log_pages_before_vacuum {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Pages to process before vacuum: '.$arg_hash{'value'}.'.',
		level => 'info',
		target => $self->{'_log_ident'});

	return;
}

sub _log_clean_pages_average {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => ('Cleaning in average: '.
					sprintf("%.3f", $arg_hash{'timing'}).' s per '.
					$arg_hash{'pages_per_round'}.' pages.'),
		level => 'info',
		target => $self->{'_log_ident'});

	return;
}

sub _log_progress {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Progress: '.
			(defined $arg_hash{'initial_statistics'}->{'effective_page_count'} ?
			 int(
				 100 *
				 ($arg_hash{'to_page'} ?
				  ($arg_hash{'initial_statistics'}->{'page_count'} -
				   $arg_hash{'to_page'} - 1) /
				  ($arg_hash{'initial_statistics'}->{'page_count'} -
				   $arg_hash{'initial_statistics'}->{'effective_page_count'}) :
				  1)
			 ).'%, ' : ' ').
			($arg_hash{'initial_statistics'}->{'page_count'} -
			 $arg_hash{'to_page'} - 1).' pages completed.'),
		level => 'info',
		target => $self->{'_log_ident'});

	return;
}

sub _log_max_loops {
	my $self = shift;

	$self->{'_logger'}->write(
		message => 'Maximum loops reached.',
		level => 'warning',
		target => $self->{'_log_ident'});

	return;
}

sub _log_reindex_starting {
	my $self = shift;

	$self->{'_logger'}->write(
		message => 'Reindex starting...',
		level => 'info',
		target => $self->{'_log_ident'});

	return;
}

sub _log_reindex_complete {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Reindex: '.sprintf("%.3f", $arg_hash{'timing'}).' s.',
		level => 'info',
		target => $self->{'_log_ident'});

	return;
}

sub _log_reindex_queries {
	my $self = shift;

	$self->{'_logger'}->write(
		message => ('Reindex queries:'."\n".
					join("\n", @{$self->_get_reindex_queries()})),
		level => 'notice',
		target => $self->{'_log_ident'});

	return;
}

sub _log_processing_incomplete {
	my $self = shift;

	$self->{'_logger'}->write(
		message => 'Processing incomplete.',
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

	${$arg_hash{'timing'}} = $self->_time();

	$self->{'_database'}->execute(
		sql => ('VACUUM '.($arg_hash{'analyze'} ? 'ANALYZE ' : '').
				$self->{'_ident'}));

	${$arg_hash{'timing'}} = $self->_time() - ${$arg_hash{'timing'}};

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

	${$arg_hash{'timing'}} = $self->_time();

	my $result = $self->{'_database'}->execute(
		sql => <<SQL
SELECT _clean_pages(
    '$self->{'_ident'}', '$arg_hash{'column_ident'}',
    $arg_hash{'to_page'}, $arg_hash{'pages_per_round'})
SQL
		);

	${$arg_hash{'timing'}} = $self->_time() - ${$arg_hash{'timing'}};

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
	my ($self, %arg_hash) = @_;

	${$arg_hash{'timing'}} = $self->_time();

	for my $query (@{$self->_get_reindex_queries()}) {
		$self->{'_database'}->execute(sql => $query);
	}

	${$arg_hash{'timing'}} = $self->_time() - ${$arg_hash{'timing'}};

	return;
}

sub _get_pages_per_round {
	my ($self, %arg_hash) = @_;

	return ceil(
		(sort {$a <=> $b}
		 (sort {$b <=> $a}
		  $arg_hash{'statistics'}->{'page_count'} /
		  $self->{'_pages_per_round_divisor'},
		  1)[0],
		 $self->{'_max_pages_per_round'})[0]);
}

sub _get_pages_before_vacuum {
	my ($self, %arg_hash) = @_;

	return ceil(
		(sort {$b <=> $a}
		 (sort {$a <=> $b}
		  $arg_hash{'statistics'}->{'page_count'} /
		  $self->{'_pages_before_vacuum_lower_divisor'},
		  $self->{'_pages_before_vacuum_lower_threshold'})[0],
		 $arg_hash{'expected_page_count'} /
		 $self->{'_pages_before_vacuum_upper_divisor'},
		 1)[0]);
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
