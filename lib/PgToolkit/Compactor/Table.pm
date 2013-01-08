package PgToolkit::Compactor::Table;

use base qw(PgToolkit::Compactor);

use strict;
use warnings;

use POSIX;
use Time::HiRes qw(time sleep);

use PgToolkit::Utils;

=head1 NAME

B<PgToolkit::Compactor::Table> - a table level processing for bloat reducing.

=head1 SYNOPSIS

	my $table_compactor = PgToolkit::Compactor::Table->new(
		database => $database,
		logger => $logger,
		dry_run => 0,
		schema_name => $schema_name,
		table_name => $table_name,
		min_page_count => 100,
		min_free_percent => 10,
		max_pages_per_round => 5,
		no_initial_vacuum => 0,
		no_routine_vacuum => 0,
		no_final_analyze => 0,
		delay_constant => 1,
		delay_ratio => 2,
		force => 0,
		reindex => 0,
		print_reindex_queries => 0,
		progress_report_period => 60,
		pgstattuple_schema_name => 'public',
		pages_per_round_divisor = 1000,
		pages_before_vacuum_lower_divisor = 16,
		pages_before_vacuum_lower_threshold = 1000,
		pages_before_vacuum_upper_divisor = 50,
		max_retry_count => 10);

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

=item C<dry_run>

=item C<schema_name>

a schema name to process

=item C<table_name>

a table name to process

=item C<min_page_count>

a minimum number of pages that is worth to compact with for both
tables and indexes

=item C<min_free_percent>

a mininum free space percent that is worth to compact with for both
tables and indexes

=item C<max_pages_per_round>

an upper threshold of pages to process per one round

=item C<no_initial_vacuum>

perform no initial vacuum

=item C<no_routine_vacuum>

perform no routine vacuum

=item C<no_fianl_analyze>

perform no final analyze

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

=item C<pgstattuple_schema_name>

schema where pgstattuple is if we should use it to get statistics

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

=item C<max_retry_count>

a maximum amount of attempts to compact cluster.

=back

=cut

sub _init {
	my ($self, %arg_hash) = @_;

	$self->{'_database'} = $arg_hash{'database'};
	$self->{'_logger'} = $arg_hash{'logger'};
	$self->{'_schema_name'} = $arg_hash{'schema_name'};
	$self->{'_table_name'} = $arg_hash{'table_name'};

	$self->{'_ident'} =
		$self->{'_database'}->quote_ident(
			string => $self->{'_schema_name'}).'.'.
		$self->{'_database'}->quote_ident(
			string => $self->{'_table_name'});

	$self->{'_log_target'} = $self->{'_database'}->quote_ident(
		string => $self->{'_database'}->get_dbname()).', '.$self->{'_ident'};

	$self->{'_min_page_count'} = $arg_hash{'min_page_count'};
	$self->{'_min_free_percent'} = $arg_hash{'min_free_percent'};
	$self->{'_max_pages_per_round'} = $arg_hash{'max_pages_per_round'};
	$self->{'_no_initial_vacuum'} = $arg_hash{'no_initial_vacuum'};
	$self->{'_no_routine_vacuum'} = $arg_hash{'no_routine_vacuum'};
	$self->{'_no_final_analyze'} = $arg_hash{'no_final_analyze'};
	$self->{'_delay_constant'} = $arg_hash{'delay_constant'};
	$self->{'_delay_ratio'} = $arg_hash{'delay_ratio'};
	$self->{'_force'} = $arg_hash{'force'};
	$self->{'_reindex'} = $arg_hash{'reindex'};
	$self->{'_print_reindex_queries'} = $arg_hash{'print_reindex_queries'};
	$self->{'_max_retry_count'} = $arg_hash{'max_retry_count'};

	$self->{'_progress_report_period'} = $arg_hash{'progress_report_period'};
	if ($arg_hash{'pgstattuple_schema_name'}) {
		$self->{'_pgstattuple_schema_ident'} =
			$self->{'_database'}->quote_ident(
				string => $arg_hash{'pgstattuple_schema_name'});
	}
	$self->{'_pages_per_round_divisor'} = $arg_hash{'pages_per_round_divisor'};
	$self->{'_pages_before_vacuum_lower_divisor'} =
		$arg_hash{'pages_before_vacuum_lower_divisor'};
	$self->{'_pages_before_vacuum_lower_threshold'} =
		$arg_hash{'pages_before_vacuum_lower_threshold'};
	$self->{'_pages_before_vacuum_upper_divisor'} =
		$arg_hash{'pages_before_vacuum_upper_divisor'};

	$self->{'_is_processed'} = 0;

	return;
}

sub process {
	my ($self, %arg_hash) = @_;

	eval {
		$self->_process(%arg_hash);
	};
	if ($@) {
		my $name = $self->{'_schema_name'}.'.'.$self->{'_table_name'};
		if ($@ =~ ('relation "'.$name.'" does not exist')) {
			$self->_log_relation_does_not_exist();
			$self->{'_is_processed'} = 1;
		} elsif ($@ =~ /DataError (.*?)\./) {
			$self->_log_data_error(message => $1);
			$self->{'_is_processed'} = 1;
		} else {
			my $error = $@;
			$self->_wrap(code => sub { die($error); });
		}
	}
}

sub _process {
	my ($self, %arg_hash) = @_;

	my $duration;
	my $is_skipped;

	$self->{'_size_statistics'} = $self->_get_size_statistics();

	if (not defined $self->{'_base_size_statistics'}) {
		$self->{'_base_size_statistics'} = {%{$self->{'_size_statistics'}}};
	}

	if (not $self->{'_dry_run'} and not $self->{'_no_initial_vacuum'}) {
		$self->_do_vacuum();
		$duration = $self->{'_database'}->get_duration();

		$self->{'_size_statistics'} = $self->_get_size_statistics();

		$self->_log_vacuum_complete(
			page_count => $self->{'_size_statistics'}->{'page_count'},
			duration => $duration,
			to_page => $self->{'_size_statistics'}->{'page_count'} - 1,
			pages_before_vacuum => (
				$self->{'_size_statistics'}->{'page_count'}),
			phrase => 'initial');
	}

	if ($self->{'_size_statistics'}->{'page_count'} <= 1) {
		$self->_log_skipping_empty_table();
		$is_skipped = 1;;
	}

	if (not $is_skipped) {
		eval {
			$self->{'_bloat_statistics'} = $self->_get_bloat_statistics();
			if ($self->{'_pgstattuple_schema_ident'}) {
				$self->_log_pgstattuple_duration(
					duration => $self->{'_database'}->get_duration());
			}
		};
		if ($@) {
			if ($@ =~ 'DataError') {
				$self->_do_analyze();
				$self->_log_analyze_complete(
					duration => $self->{'_database'}->get_duration(),
					phrase => 'required initial');

				eval {
					$self->{'_bloat_statistics'} =
						$self->_get_bloat_statistics();
					if ($self->{'_pgstattuple_schema_ident'}) {
						$self->_log_pgstattuple_duration(
							duration => $self->{'_database'}->get_duration());
					}
				};

				if ($@) {
					if ($@ =~ 'DataError') {
						$self->_log_skipping_can_not_get_bloat_statistics();
						$is_skipped = 1;;
					} else {
						die($@);
					}
				}
			} else {
				die($@);
			}
		}
	}

	if (not $is_skipped) {
		$self->_log_statistics(
			size_statistics => $self->{'_size_statistics'},
			bloat_statistics => $self->{'_bloat_statistics'});

		if ($self->_has_special_triggers()) {
			$self->_log_can_not_process_ar_triggers();
			$is_skipped = 1;;
		}

		if (not $self->{'_force'}) {
			if ($self->{'_size_statistics'}->{'page_count'} <
				$self->{'_min_page_count'})
			{
				$self->_log_skipping_min_page_count(
					page_count => $self->{'_size_statistics'}->{'page_count'});
				$is_skipped = 1;;
			}

			if ($self->{'_bloat_statistics'}->{'free_percent'} <
				$self->{'_min_free_percent'})
			{
				$self->_log_skipping_min_free_percent(
					free_percent => (
						$self->{'_bloat_statistics'}->{'free_percent'}));
				$is_skipped = 1;;
			}
		}
	}

	my $is_compacted;
	if (not $is_skipped and not $self->{'_dry_run'}) {
		if ($self->{'_force'}) {
			$self->_log_processing_forced();
		}

		my $vacuum_page_count = 0;
		my $initial_size_statistics = {%{$self->{'_size_statistics'}}};
		my $to_page = $self->{'_size_statistics'}->{'page_count'} - 1;
		my $progress_report_time = $self->_time();
		my $clean_pages_total_duration = 0;
		my $last_loop = $self->{'_size_statistics'}->{'page_count'} + 1;
		my $expected_error_occurred = 0;

		my $expected_page_count = $self->{'_size_statistics'}->{'page_count'};
		my $column_ident = $self->{'_database'}->quote_ident(
			string => $self->_get_update_column());
		my $pages_per_round = $self->_get_pages_per_round(
			page_count => $self->{'_size_statistics'}->{'page_count'},
			to_page => $to_page);
		my $pages_before_vacuum = $self->_get_pages_before_vacuum(
			expected_page_count => $expected_page_count,
			page_count => $self->{'_size_statistics'}->{'page_count'});
		my $max_tupples_per_page = $self->_get_max_tupples_per_page();
		$self->_log_column(name => $column_ident);
		$self->_log_pages_per_round(value => $pages_per_round);
		$self->_log_pages_before_vacuum(value => $pages_before_vacuum);

		my $loop;
		for ($loop = $self->{'_size_statistics'}->{'page_count'};
			 $loop > 0 ; $loop--)
		{
			my $start_time = $self->_time();

			my $last_to_page = $to_page;
			eval {
				$to_page = $self->_clean_pages(
					column_ident => $column_ident,
					to_page => $last_to_page,
					pages_per_round => $pages_per_round,
					max_tupples_per_page => $max_tupples_per_page);
				$clean_pages_total_duration =
					$clean_pages_total_duration +
					$self->{'_database'}->get_duration();
			};
			if ($@) {
				if ($@ =~ 'No more free space left in the table') {
					# Normal cleaning completion
				} elsif ($@ =~ 'deadlock detected') {
					$self->_log_deadlock_detected();
					next;
				} elsif ($@ =~ 'cannot extract system attribute') {
					$self->_log_cannot_extract_system_attribute();
					$expected_error_occurred = 1;
				} else {
					die($@);
				}
				last;
			}

			$self->_sleep(
				$self->{'_delay_constant'} + $self->{'_delay_ratio'} *
				($self->_time() - $start_time));

			if ($self->_time() - $progress_report_time >=
				$self->{'_progress_report_period'} and
				$last_to_page != $to_page)
			{
				$self->_log_progress(
					page_count => $initial_size_statistics->{'page_count'},
					effective_page_count => (
						$self->{'_bloat_statistics'}->{'effective_page_count'}),
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
					average_duration => (
						$clean_pages_total_duration / ($last_loop - $loop)));
				$clean_pages_total_duration = 0;
				$last_loop = $loop;

				$self->_do_vacuum();
				$duration = $self->{'_database'}->get_duration();

				$self->{'_size_statistics'} = $self->_get_size_statistics();

				$self->_log_vacuum_complete(
					page_count => $self->{'_size_statistics'}->{'page_count'},
					duration => $duration,
					to_page => $to_page,
					pages_before_vacuum => $pages_before_vacuum,
					phrase => 'routine');

				$vacuum_page_count = 0;

				my $last_pages_before_vacuum = $pages_before_vacuum;
				$pages_before_vacuum = $self->_get_pages_before_vacuum(
					expected_page_count => $expected_page_count,
					page_count => $self->{'_size_statistics'}->{'page_count'});
				if ($last_pages_before_vacuum != $pages_before_vacuum) {
					$self->_log_pages_before_vacuum(
						value => $pages_before_vacuum);
				}
			}

			if ($to_page >= $self->{'_size_statistics'}->{'page_count'}) {
				$to_page = $self->{'_size_statistics'}->{'page_count'} - 1;
			}

			if ($to_page <= 1) {
				$to_page = 0;
				last;
			}

			my $last_pages_per_round = $pages_per_round;
			$pages_per_round = $self->_get_pages_per_round(
				page_count => $self->{'_size_statistics'}->{'page_count'},
				to_page => $to_page);
			if ($last_pages_per_round != $pages_per_round) {
				$self->_log_pages_per_round(
					value => $pages_per_round);
			}
		}

		if ($loop == 0) {
			$self->_log_max_loops();
		}

		if ($to_page > 0) {
			$self->_do_vacuum();
			$duration = $self->{'_database'}->get_duration();

			$self->{'_size_statistics'} = $self->_get_size_statistics();

			$self->_log_vacuum_complete(
				page_count => $self->{'_size_statistics'}->{'page_count'},
				duration => $duration,
				to_page => $to_page + $pages_per_round,
				pages_before_vacuum => $pages_before_vacuum,
				phrase => 'final');
		}

		if (not $self->{'_no_final_analyze'}) {
			$self->_do_analyze();
			$self->_log_analyze_complete(
				duration => $self->{'_database'}->get_duration(),
				phrase => 'final');
		}

		$self->{'_bloat_statistics'} = $self->_get_bloat_statistics();
		if ($self->{'_pgstattuple_schema_ident'}) {
			$self->_log_pgstattuple_duration(
				duration => $self->{'_database'}->get_duration());
		}

		$pages_before_vacuum = $self->_get_pages_before_vacuum(
			expected_page_count => $expected_page_count,
			page_count => $self->{'_size_statistics'}->{'page_count'});

		$is_compacted = (
			($self->{'_size_statistics'}->{'page_count'} <=
			 $to_page + 1 + $pages_before_vacuum) and
			not $expected_error_occurred);
	}

	my $will_be_skipped = (
		not $self->{'_force'} and (
			$is_skipped or
			$self->{'_size_statistics'}->{'page_count'} <
			$self->{'_min_page_count'} or
			$self->{'_bloat_statistics'}->{'free_percent'} <
			$self->{'_min_free_percent'}));

	my $is_reindexed;
	if (($self->{'_dry_run'} or $is_compacted or
		 $arg_hash{'attempt'} == $self->{'_max_retry_count'} or
		 $is_skipped and $self->{'_pgstattuple_schema_ident'} or
		 not $is_skipped and $will_be_skipped) and
		($self->{'_reindex'} or $self->{'_print_reindex_queries'}))
	{
		for my $index_data (@{$self->_get_index_data_list()}) {
			my $index_ident =
				$self->{'_database'}->quote_ident(
					string => $self->{'_schema_name'}).'.'.
					$self->{'_database'}->quote_ident(
						string => $index_data->{'name'});

			my $initial_index_size_statistics =
				$self->_get_index_size_statistics(ident => $index_ident);

			if ($initial_index_size_statistics->{'page_count'} <= 1) {
				$self->_log_skipping_reindex_empty(
					ident => $index_ident);
				next;
			}

			my $index_bloat_statistics;
			if (not $self->{'_force'}) {
				if ($index_data->{'method'} ne 'btree') {
					$self->_log_skipping_reindex_not_btree(
						index_data => $index_data,
						ident => $index_ident);
					$self->_log_reindex_queries(
						ident => $index_ident,
						initial_size_statistics => (
							$initial_index_size_statistics),
						bloat_statistics => undef,
						data => $index_data);
					next;
				}

				if ($initial_index_size_statistics->{'page_count'} <
					$self->{'_min_page_count'})
				{
					$self->_log_skipping_reindex_min_page_count(
						ident => $index_ident,
						size_statistics => $initial_index_size_statistics);
					next;
				}

				if ($self->{'_pgstattuple_schema_ident'})
				{
					$index_bloat_statistics =
						$self->_get_index_bloat_statistics(
							ident => $index_ident);

					if ($index_bloat_statistics->{'free_percent'} <
						$self->{'_min_free_percent'})
					{
						$self->_log_skipping_reindex_min_free_percent(
							ident => $index_ident,
							bloat_statistics => $index_bloat_statistics);
						next;
					}
				}
			}

			if (not $index_data->{'allowed'}) {
				$self->_log_skipping_reindex_not_allowed(
					ident => $index_ident);
				$self->_log_reindex_queries(
					ident => $index_ident,
					initial_size_statistics => $initial_index_size_statistics,
					bloat_statistics => $index_bloat_statistics,
					data => $index_data);
				next;
			}

			if (not $self->{'_dry_run'} and $self->{'_reindex'}) {
				$self->_reindex(data => $index_data);
				$duration = $self->{'_database'}->get_duration();
				$self->_alter_index(data => $index_data);
				$duration += $self->{'_database'}->get_duration();
				$self->_log_reindex(
					ident => $index_ident,
					initial_size_statistics => $initial_index_size_statistics,
					size_statistics => $self->_get_index_size_statistics(
						ident => $index_ident),
					duration => $duration);

				$is_reindexed = 1;
			}

			if ($self->{'_dry_run'} or $self->{'_print_reindex_queries'}) {
				$self->_log_reindex_queries(
					ident => $index_ident,
					initial_size_statistics => $initial_index_size_statistics,
					bloat_statistics => $index_bloat_statistics,
					data => $index_data);
			}
		}

		if (not $self->{'_dry_run'} and $self->{'_reindex'}) {
			$self->{'_size_statistics'} = $self->_get_size_statistics();
		}
	}

	if (not $self->{'_dry_run'} and not ($is_skipped and not $is_reindexed)) {
		my $complete = (
			$is_compacted or $will_be_skipped or $is_skipped and $is_reindexed);

		if ($complete) {
			$self->_log_complete_processing();
		} else {
			$self->_log_incomplete_processing();
		}

		$self->_log_processing_results(
			size_statistics => $self->{'_size_statistics'},
			bloat_statistics => $self->{'_bloat_statistics'},
			base_size_statistics => $self->{'_base_size_statistics'},
			complete => $complete);
	}

	$self->{'_is_processed'} = (
		$is_compacted or $is_skipped or $will_be_skipped or
		$self->{'_dry_run'});

	return;
}

=head1 METHODS

=head2 B<is_processed()>

Tests if the table is processed.

=head3 Returns

True or false value.

=cut

sub is_processed {
	my $self = shift;

	return $self->{'_is_processed'};
}

=head2 B<get_log_ident()>

Returns a table ident for log.

=head3 Returns

A string representing the ident.

=cut

sub get_log_ident {
	my $self = shift;

	return $self->{'_log_ident'};
}

=head2 B<get_size_delta()>

Returns a size delta in bytes.

=head3 Returns

A number or undef if has not been processed.

=cut

sub get_size_delta {
	my $self = shift;

	return
		$self->{'_base_size_statistics'}->{'size'} -
		$self->{'_size_statistics'}->{'size'};
}

=head2 B<get_total_size_delta()>

Returns a tital (including toasts and indexes) size delta in bytes.

=head3 Returns

A number or undef if has not been processed.

=cut

sub get_total_size_delta {
	my $self = shift;

	return
		$self->{'_base_size_statistics'}->{'total_size'} -
		$self->{'_size_statistics'}->{'total_size'};
}

sub _log_skipping_empty_table {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Skipping processing: empty or 1 page table.',
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_can_not_process_ar_triggers {
	my $self = shift;

	$self->{'_logger'}->write(
		message => 'Can not process: "always" or "replica" triggers are on.',
		level => 'warning',
		target => $self->{'_log_target'});

	return;
}

sub _log_skipping_min_page_count {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Skipping processing: '.$arg_hash{'page_count'}.' pages from '.
			$self->{'_min_page_count'}.' pages minimum required.'),
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_vacuum_complete {
	my ($self, %arg_hash) = @_;

	if ($arg_hash{'page_count'} > $arg_hash{'to_page'} + 1) {
		my $level;
		if ($arg_hash{'page_count'} - ($arg_hash{'to_page'} + 1) <=
			$arg_hash{'pages_before_vacuum'} * 2)
		{
			$level = 'info';
		} else {
			$level = 'notice';
		}

		$self->{'_logger'}->write(
			message => (
				'Vacuum '.$arg_hash{'phrase'}.': can not clean '.
				($arg_hash{'page_count'} - $arg_hash{'to_page'} - 1).' pages, '.
				$arg_hash{'page_count'}.' pages left, duration '.
				sprintf("%.3f", $arg_hash{'duration'}).' seconds.'),
			level => $level,
			target => $self->{'_log_target'});
	} else {
		$self->{'_logger'}->write(
			message => (
				'Vacuum '.$arg_hash{'phrase'}.': '.$arg_hash{'page_count'}.
				' pages left, duration '.sprintf("%.3f", $arg_hash{'duration'}).
				' seconds.'),
			level => 'info',
			target => $self->{'_log_target'});
	}

	return;
}

sub _log_skipping_min_free_percent {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Skipping processing: '.
			$arg_hash{'free_percent'}.'% space to compact from '.
			$self->{'_min_free_percent'}.'% minimum required.'),
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_skipping_can_not_get_bloat_statistics {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Can not get bloat statistics, processing stopped.',
		level => 'warning',
		target => $self->{'_log_target'});

	return;
}

sub _log_processing_forced {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Processing forced.',
		level => 'notice',
		target => $self->{'_log_target'});

	return;
}

sub _log_statistics {
	my ($self, %arg_hash) = @_;

	my $can_be_compacted = (
		$arg_hash{'bloat_statistics'}->{'free_percent'} > 0 and
		$arg_hash{'size_statistics'}->{'page_count'} >
		$arg_hash{'bloat_statistics'}->{'effective_page_count'});

	$self->{'_logger'}->write(
		message => (
			'Statistics: '.
			$arg_hash{'size_statistics'}->{'page_count'}.' pages ('.
			$arg_hash{'size_statistics'}->{'total_page_count'}.
			' pages including toasts and indexes)'.
			($can_be_compacted ? ', approximately '.
			 $arg_hash{'bloat_statistics'}->{'free_percent'}.'% ('.
			 ($arg_hash{'size_statistics'}->{'page_count'} -
			  $arg_hash{'bloat_statistics'}->{'effective_page_count'}).
			 ' pages) can be compacted reducing the size by '.
			 PgToolkit::Utils->get_size_pretty(
				 size => $arg_hash{'bloat_statistics'}->{'free_space'})
			 : '').'.'),
		level => 'notice',
		target => $self->{'_log_target'});

	return;
}

sub _log_column {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Update by column: '.$arg_hash{'name'}.'.',
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_pages_per_round {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Set pages/round: '.$arg_hash{'value'}.'.',
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_pages_before_vacuum {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Set pages/vacuum: '.$arg_hash{'value'}.'.',
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_clean_pages_average {
	my ($self, %arg_hash) = @_;

	my $duration = sprintf("%.3f", $arg_hash{'average_duration'});

	if ($arg_hash{'average_duration'} == 0) {
		$arg_hash{'average_duration'} = 0.0001;
	}

	$self->{'_logger'}->write(
		message => (
			'Cleaning in average: '.
			sprintf("%.1f", $arg_hash{'pages_per_round'} /
					$arg_hash{'average_duration'}).
			' pages/second ('.$duration.' seconds per '.
			$arg_hash{'pages_per_round'}.' pages).'),
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_progress {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Progress: '.
			(defined $arg_hash{'effective_page_count'} ?
			 int(
				 100 *
				 ($arg_hash{'to_page'} ?
				  ($arg_hash{'page_count'} - $arg_hash{'to_page'} - 1) /
				  ($arg_hash{'page_count'} -
				   $arg_hash{'effective_page_count'}) :
				  1)
			 ).'%, ' : ' ').
			($arg_hash{'page_count'} - $arg_hash{'to_page'} - 1).
			' pages completed.'),
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_max_loops {
	my $self = shift;

	$self->{'_logger'}->write(
		message => 'Maximum loops reached.',
		level => 'warning',
		target => $self->{'_log_target'});

	return;
}

sub _log_analyze_complete {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => ('Analyze '.$arg_hash{'phrase'}.': duration '.
					sprintf("%.3f", $arg_hash{'duration'}).' second.'),
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_skipping_reindex_not_allowed {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Skipping reindex: '.$arg_hash{'ident'}.
			', can not reindex without heavy locks because '.
			'of its dependencies, reindexing is up to you.'),
		level => 'notice',
		target => $self->{'_log_target'});

	return;
}

sub _log_skipping_reindex_not_btree {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Skipping reindex: '.$arg_hash{'ident'}.' is a '.
			$arg_hash{'index_data'}->{'method'}.' index not a btree, '.
			'reindexing is up to you.'),
		level => 'notice',
		target => $self->{'_log_target'});

	return;
}

sub _log_skipping_reindex_empty {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Skipping reindex: '.$arg_hash{'ident'}.', empty or 1 page index.'),
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_skipping_reindex_min_page_count {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Skipping reindex: '.$arg_hash{'ident'}.', '.
			$arg_hash{'size_statistics'}->{'page_count'}.' pages from '.
			$self->{'_min_page_count'}.' pages minimum required.'),
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_skipping_reindex_min_free_percent {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Skipping reindex: '.$arg_hash{'ident'}.', '.
			$arg_hash{'bloat_statistics'}->{'free_percent'}.
			'% space to compact from '.$self->{'_min_free_percent'}.
			'% minimum required.'),
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_reindex {
	my ($self, %arg_hash) = @_;

	my $free_percent = 100 *(
		1 - $arg_hash{'size_statistics'}->{'size'} /
		$arg_hash{'initial_size_statistics'}->{'size'});

	my $free_space = (
		$arg_hash{'initial_size_statistics'}->{'size'} -
		$arg_hash{'size_statistics'}->{'size'});

	$self->{'_logger'}->write(
		message => (
			'Reindex'.($self->{'_force'} ? ' forced' : '').': '.
			$arg_hash{'ident'}.', '.
			($arg_hash{'size_statistics'} ? 'initial size '.
			 $arg_hash{'size_statistics'}->{'page_count'}.' pages ('.
			 PgToolkit::Utils->get_size_pretty(
				 size => $arg_hash{'size_statistics'}->{'size'}).
			 '), has been reduced by '.
			 int($free_percent).'% ('.
			 PgToolkit::Utils->get_size_pretty(
				 size => int($free_space)).'), ' : '').
			'duration '.sprintf("%.3f", $arg_hash{'duration'}).' seconds.'),
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_reindex_queries {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Reindex queries'.($self->{'_force'} ? ' forced' : '').': '.
			$arg_hash{'ident'}.
			($arg_hash{'initial_size_statistics'} ? ', initial size '.
			 $arg_hash{'initial_size_statistics'}->{'page_count'}.' pages ('.
			 PgToolkit::Utils->get_size_pretty(
				 size => $arg_hash{'initial_size_statistics'}->{'size'}).')'.
			 ($arg_hash{'bloat_statistics'} ? ', will be reduced by '.
			  $arg_hash{'bloat_statistics'}->{'free_percent'}.'% ('.
			  PgToolkit::Utils->get_size_pretty(
				  size => $arg_hash{'bloat_statistics'}->{'free_space'}).
			  ')' : '') : '').".\n".
			($arg_hash{'data'}->{'allowed'} ?
			 $self->_get_reindex_query(data => $arg_hash{'data'})."\n".
			 $self->_get_alter_index_query(data => $arg_hash{'data'}) :
			 $self->_get_straight_reindex_query(data => $arg_hash{'data'}))),
		level => 'notice',
		target => $self->{'_log_target'});

	return;
}

sub _log_incomplete_processing {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Processing incomplete.',
		level => 'warning',
		target => $self->{'_log_target'});

	return;
}

sub _log_complete_processing {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Processing complete.',
		level => 'notice',
		target => $self->{'_log_target'});

	return;
}

sub _log_processing_results {
	my ($self, %arg_hash) = @_;

	my $can_be_compacted = (
		defined $arg_hash{'bloat_statistics'}->{'free_percent'} and
		defined $arg_hash{'bloat_statistics'}->{'effective_page_count'} and
		$arg_hash{'bloat_statistics'}->{'free_percent'} > 0 and
		$arg_hash{'size_statistics'}->{'page_count'} >
		$arg_hash{'bloat_statistics'}->{'effective_page_count'} and
		not $arg_hash{'complete'});

	$self->{'_logger'}->write(
		message => (
			'Processing results: '.
			$arg_hash{'size_statistics'}->{'page_count'}.' pages left ('.
			$arg_hash{'size_statistics'}->{'total_page_count'}.
			' pages including toasts and indexes), size reduced by '.
			PgToolkit::Utils->get_size_pretty(
				size => ($arg_hash{'base_size_statistics'}->{'size'} -
						 $arg_hash{'size_statistics'}->{'size'})).' ('.
			PgToolkit::Utils->get_size_pretty(
				size => ($arg_hash{'base_size_statistics'}->{'total_size'} -
						 $arg_hash{'size_statistics'}->{'total_size'})).
			' including toasts and indexes) in total'.
			($can_be_compacted ? ', approximately '.
			 $arg_hash{'bloat_statistics'}->{'free_percent'}.'% ('.
			 ($arg_hash{'size_statistics'}->{'page_count'} -
			  $arg_hash{'bloat_statistics'}->{'effective_page_count'}).
			 ' pages) that is '.
			 PgToolkit::Utils->get_size_pretty(
				 size => $arg_hash{'bloat_statistics'}->{'free_space'}).
			 ' more were expected to be compacted after this attempt' :
			 '').'.'),
		level => 'notice',
		target => $self->{'_log_target'});
}

sub _log_deadlock_detected {
	my $self = shift;

	$self->{'_logger'}->write(
		message => 'Detected deadlock during cleaning.',
		level => 'notice',
		target => $self->{'_log_target'});

	return;
}

sub _log_cannot_extract_system_attribute {
	my $self = shift;

	$self->{'_logger'}->write(
		message => ('System attribute extraction error has occurred, '.
					'processing stopped.'),
		level => 'warning',
		target => $self->{'_log_target'});

	return;
}

sub _log_relation_does_not_exist {
	my $self = shift;

	$self->{'_logger'}->write(
		message => ('Relation does not exist error has occurred, '.
					'processing stopped.'),
		level => 'warning',
		target => $self->{'_log_target'});

	return;
}

sub _log_data_error {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => $arg_hash{'message'}.', processing stopped.',
		level => 'warning',
		target => $self->{'_log_target'});

	return;
}

sub _log_pgstattuple_duration {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => ('Bloat statistics with pgstattuple: duration '.
					sprintf("%.3f", $arg_hash{'duration'}).' seconds.'),
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _sleep {
	my ($self, $time) = @_;

	sleep($time);

	return;
}

sub _time {
	return time();
}

sub _has_special_triggers {
	my $self = shift;

	my $result = $self->_execute_and_log(
		sql => <<SQL
SELECT count(1) FROM pg_catalog.pg_trigger
WHERE
    tgrelid = '$self->{'_ident'}'::regclass AND
    tgenabled IN ('A', 'R') AND
    (tgtype & 16)::boolean
SQL
		);

	return $result->[0]->[0];
}

sub _get_max_tupples_per_page {
	my $self = shift;

	my $result = $self->_execute_and_log(
		sql => <<SQL
SELECT ceil(current_setting('block_size')::real / sum(attlen))
FROM pg_catalog.pg_attribute
WHERE
    attrelid = '$self->{'_ident'}'::regclass AND
    attnum < 0;
SQL
		);

	if (not defined $result->[0]->[0]) {
		die('DataError Can not get max tupples per page.');
	}

	return $result->[0]->[0];
}

sub _get_bloat_statistics {
	my $self = shift;

	my $result;
	if ($self->{'_pgstattuple_schema_ident'}) {
		$result = $self->_execute_and_log(
			sql => <<SQL
SELECT
    ceil((size - free_space) * 100 / fillfactor / bs) AS effective_page_count,
    round(
        (100 * (1 - (100 - free_percent) / fillfactor))::numeric, 2
    ) AS free_percent,
    ceil(size - (size - free_space) * 100 / fillfactor) AS free_space
FROM (
    SELECT
        current_setting('block_size')::integer AS bs,
        pg_catalog.pg_relation_size(pg_catalog.pg_class.oid) AS size,
        coalesce(
            (
                SELECT (
                    regexp_matches(
                        reloptions::text, E'.*fillfactor=(\\\\d+).*'))[1]),
            '100')::real AS fillfactor,
        pgst.*
    FROM pg_catalog.pg_class
    CROSS JOIN
        $self->{'_pgstattuple_schema_ident'}.pgstattuple(
            '$self->{'_ident'}') AS pgst
    WHERE pg_catalog.pg_class.oid = '$self->{'_ident'}'::regclass
) AS sq
SQL
			);
	} else {
		$result = $self->_execute_and_log(
			sql => <<SQL
SELECT
    ceil(pure_page_count * 100 / fillfactor) AS effective_page_count,
    CASE WHEN size::real > 0 THEN
        round(
            100 * (
                1 - (pure_page_count * 100 / fillfactor) / (size::real / bs)
            )::numeric, 2
        )
    ELSE 0 END AS free_percent,
    ceil(size::real - bs * pure_page_count * 100 / fillfactor) AS free_space
FROM (
    SELECT
        bs, size, fillfactor,
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
            )::real / (bs - 24)
        ) AS pure_page_count
    FROM (
        SELECT
            pg_catalog.pg_class.oid AS class_oid,
            reltuples,
            23 AS header_width, 8 AS ma,
            current_setting('block_size')::integer AS bs,
            pg_catalog.pg_relation_size(pg_catalog.pg_class.oid) AS size,
            coalesce(
                (
                    SELECT (
                        regexp_matches(
                            reloptions::text, E'.*fillfactor=(\\\\d+).*'))[1]),
                '100')::real AS fillfactor
        FROM pg_catalog.pg_class
        WHERE pg_catalog.pg_class.oid = '$self->{'_ident'}'::regclass
    ) AS const
    LEFT JOIN pg_catalog.pg_statistic ON starelid = class_oid
    GROUP BY bs, class_oid, fillfactor, ma, size, reltuples, header_width
) AS sq
SQL
			);
	}

	$result = {
		'effective_page_count' => $result->[0]->[0],
		'free_percent' => (defined $result->[0]->[1] and
						   $result->[0]->[1] > 0) ? $result->[0]->[1] : 0,
		'free_space' => (defined $result->[0]->[2] and
						 $result->[0]->[2] > 0) ? $result->[0]->[2] : 0};

	if (not defined $result->{'effective_page_count'} or
		not defined $result->{'free_percent'} or
		not defined $result->{'free_space'})
	{
		die('DataError Can not get bloat statistics.');
	}

	return $result;
}

sub _get_size_statistics {
	my $self = shift;

	my $result = $self->_execute_and_log(
		sql => <<SQL
SELECT
    size,
    total_size,
    ceil(size::real / bs) AS page_count,
    ceil(total_size::real / bs) AS total_page_count
FROM (
    SELECT
        current_setting('block_size')::integer AS bs,
        pg_catalog.pg_relation_size('$self->{'_ident'}') AS size,
        pg_catalog.pg_total_relation_size('$self->{'_ident'}') AS total_size
) AS sq
SQL
		);

	$result = {
		'size' => $result->[0]->[0],
		'total_size' => $result->[0]->[1],
		'page_count' => $result->[0]->[2],
		'total_page_count' => $result->[0]->[3]};

	if (not defined $result->{'size'} or
		not defined $result->{'total_size'} or
		not defined $result->{'page_count'} or
		not defined $result->{'total_page_count'})
	{
		die('DataError Can not get size statistics.');
	}

	return $result;
}

sub _do_vacuum {
	my ($self, %arg_hash) = @_;

	$self->_execute_and_log(
		sql => ('VACUUM '.($arg_hash{'analyze'} ? 'ANALYZE ' : '').
				$self->{'_ident'}));

	return;
}

sub _do_analyze {
	my ($self, %arg_hash) = @_;

	$self->_execute_and_log(sql => 'ANALYZE '.$self->{'_ident'});

	return;
}

sub _get_update_column {
	my $self = shift;

	my $result = $self->_execute_and_log(
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
            FROM pg_catalog.pg_index
            WHERE indrelid = '$self->{'_ident'}'::regclass)),
    -- Preferably smaller attributes
    attlen,
    attnum
LIMIT 1;
SQL
		);

	if (not defined $result->[0]->[0]) {
		die('DataError Can not get update column.');
	}

	return $result->[0]->[0];
}

sub _clean_pages {
	my ($self, %arg_hash) = @_;

	my $result = $self->_execute_and_log(
		level => 'debug1',
		sql => <<SQL
SELECT public._clean_pages(
    '$self->{'_ident'}', '$arg_hash{'column_ident'}', $arg_hash{'to_page'},
    $arg_hash{'pages_per_round'}, $arg_hash{'max_tupples_per_page'})
SQL
		);

	return $result->[0]->[0];
}

sub _get_index_data_list {
	my $self = shift;

	my $result = $self->_execute_and_log(
		sql => <<SQL
SELECT
    indexname, tablespace, indexdef,
    regexp_replace(indexdef, E'.* USING (\\\\w+) .*', E'\\\\1') AS indmethod,
    conname,
    CASE
        WHEN contype = 'p' THEN 'PRIMARY KEY'
        WHEN contype = 'u' THEN 'UNIQUE'
        ELSE NULL END AS contypedef,
    (
        SELECT
            bool_and(
                deptype IN ('n', 'a', 'i') AND
                NOT (refobjid = indexoid AND deptype = 'n') AND
                NOT (
                    objid = indexoid AND deptype = 'i' AND
                    (version < array[9,1] OR contype NOT IN ('p', 'u'))))
        FROM pg_catalog.pg_depend
        LEFT JOIN pg_catalog.pg_constraint ON
            pg_catalog.pg_constraint.oid = refobjid
        WHERE objid = indexoid OR refobjid = indexoid
    )::integer AS allowed,
    pg_catalog.pg_relation_size(indexoid)
FROM (
    SELECT
        indexname, tablespace, indexdef,
        (
            quote_ident(schemaname) || '.' ||
            quote_ident(indexname))::regclass AS indexoid,
        string_to_array(
            regexp_replace(
                version(), E'.*PostgreSQL (\\\\d+\\\\.\\\\d+).*', E'\\\\1'),
            '.')::integer[] AS version
    FROM pg_catalog.pg_indexes
    WHERE
        schemaname = '$self->{'_schema_name'}' AND
        tablename = '$self->{'_table_name'}'
) AS sq
LEFT JOIN pg_catalog.pg_constraint ON
    conindid = indexoid AND contype IN ('p', 'u')
ORDER BY 8;
SQL
		);

	return [
		map(
			{'name' => $_->[0],
			 'tablespace' => $_->[1],
			 'definition' => $_->[2],
			 'method' => $_->[3],
			 'conname' => $_->[4],
			 'contypedef' => $_->[5],
			 'allowed' => $_->[6]},
			@{$result})];
}

sub _get_index_size_statistics {
	my ($self, %arg_hash) = @_;

	my $result = $self->_execute_and_log(
		sql => <<SQL
SELECT size, ceil(size / bs) AS page_count
FROM (
    SELECT
        pg_catalog.pg_relation_size('$arg_hash{'ident'}'::regclass) AS size,
        current_setting('block_size')::real AS bs
) AS sq
SQL
		);

	$result = {
		'size' => $result->[0]->[0],
		'page_count' => $result->[0]->[1]};

	if (not defined $result->{'size'} or
		not defined $result->{'page_count'})
	{
		die('DataError Can not get index size statistics.');
	}

	return $result;
}

sub _get_index_bloat_statistics {
	my ($self, %arg_hash) = @_;

	my $result = $self->_execute_and_log(
		sql => <<SQL
SELECT
    CASE
        WHEN avg_leaf_density = 'NaN' THEN 0
        ELSE
            round(
                (100 * (1 - avg_leaf_density / fillfactor))::numeric, 2
            )
        END AS free_percent,
    CASE
        WHEN avg_leaf_density = 'NaN' THEN 0
        ELSE
            ceil(
                index_size * (1 - avg_leaf_density / fillfactor)
            )
        END AS free_space
FROM (
    SELECT
        coalesce(
            (
                SELECT (
                    regexp_matches(
                        reloptions::text, E'.*fillfactor=(\\\\d+).*'))[1]),
            '90')::real AS fillfactor,
        pgsi.*
    FROM pg_catalog.pg_class
    CROSS JOIN $self->{'_pgstattuple_schema_ident'}.pgstatindex(
        '$arg_hash{'ident'}') AS pgsi
    WHERE pg_catalog.pg_class.oid = '$arg_hash{'ident'}'::regclass
) AS oq
SQL
		);

	$result = {
		'free_percent' => $result->[0]->[0],
		'free_space' => $result->[0]->[1]};

	if (not defined $result->{'free_percent'} or
		not defined $result->{'free_space'})
	{
		die('DataError Can not get index bloat statistics.');
	}

	return $result;
}

sub _get_reindex_query {
	my ($self, %arg_hash) = @_;

	my $sql = $arg_hash{'data'}->{'definition'};
	$sql =~ s/INDEX (\S+)/INDEX CONCURRENTLY pgcompactor_tmp$$/;
	if (defined $arg_hash{'data'}->{'tablespace'}) {
		$sql =~ s/(WHERE .*)?$/TABLESPACE $arg_hash{'data'}->{'tablespace'} $1/;
	}
	$sql .= ';';

	return $sql;

}

sub _get_alter_index_query {
	my ($self, %arg_hash) = @_;

	my $schema_ident = $self->{'_database'}->quote_ident(
		string => $self->{'_schema_name'});
	my $index_ident = $self->{'_database'}->quote_ident(
		string => $arg_hash{'data'}->{'name'});
	my $constraint_ident;
	if ($arg_hash{'data'}->{'conname'}) {
		$constraint_ident = $self->{'_database'}->quote_ident(
			string => $arg_hash{'data'}->{'conname'});
	}

	return
		'BEGIN; '.
		($constraint_ident ?
		 ('ALTER TABLE '.$self->{'_ident'}.
		  ' DROP CONSTRAINT '.$constraint_ident.'; '.
		  'ALTER TABLE '.$self->{'_ident'}.
		  ' ADD CONSTRAINT '.$constraint_ident.' '.
		  $arg_hash{'data'}->{'contypedef'}.
		  ' USING INDEX pgcompactor_tmp'.$$.'; ') :
		 ('DROP INDEX '.$schema_ident.'.'.$index_ident.'; '.
		  'ALTER INDEX '.$schema_ident.'.pgcompactor_tmp'.$$.
		  ' RENAME TO '.$index_ident.'; ')
		).'END;';
}

sub _get_straight_reindex_query {
	my ($self, %arg_hash) = @_;

	my $schema_ident = $self->{'_database'}->quote_ident(
		string => $self->{'_schema_name'});
	my $index_ident = $self->{'_database'}->quote_ident(
		string => $arg_hash{'data'}->{'name'});

	return
		'REINDEX INDEX '.$schema_ident.'.'.$index_ident.'; -- '.
		$self->{'_database'}->quote_ident(
			string => $self->{'_database'}->get_dbname());

}

sub _reindex {
	my ($self, %arg_hash) = @_;

	$self->_execute_and_log(
		sql => $self->_get_reindex_query(data => $arg_hash{'data'}));

	return;
}

sub _alter_index {
	my ($self, %arg_hash) = @_;

	$self->_execute_and_log(
		sql => $self->_get_alter_index_query(data => $arg_hash{'data'}));

	return;
}

sub _get_pages_per_round {
	my ($self, %arg_hash) = @_;

	my $result = ceil(
		(sort {$a <=> $b}
		 (sort {$b <=> $a}
		  $arg_hash{'page_count'} /
		  $self->{'_pages_per_round_divisor'},
		  1)[0],
		 $self->{'_max_pages_per_round'})[0]);

	$result = (sort {$a <=> $b} $result, $arg_hash{'to_page'})[0];

	return $result;
}

sub _get_pages_before_vacuum {
	my ($self, %arg_hash) = @_;

	return ceil(
		(sort {$b <=> $a}
		 (sort {$a <=> $b}
		  $arg_hash{'page_count'} /
		  $self->{'_pages_before_vacuum_lower_divisor'},
		  $self->{'_pages_before_vacuum_lower_threshold'})[0],
		 $arg_hash{'expected_page_count'} /
		 $self->{'_pages_before_vacuum_upper_divisor'},
		 1)[0]);
}

=head1 SEE ALSO

=over 4

=item L<PgToolkit::Class>
=item L<PgToolkit::Utils>

=back

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2012, PostgreSQL-Consulting.com

=head1 AUTHOR

=over 4

=item L<Sergey Konoplev|mailto:sergey.konoplev@postgresql-consulting.com>

=back

=cut

1;
