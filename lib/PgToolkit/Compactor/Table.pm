package PgToolkit::Compactor::Table;

use base qw(PgToolkit::Compactor);

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
	my $self = shift;

	eval {
		$self->_process();
	};
	if ($@) {
		my $name = $self->{'_schema_name'}.'.'.$self->{'_table_name'};
		if ($@ =~ ('relation "'.$name.'" does not exist')) {
			$self->_log_relation_does_not_exist();
			$self->{'_is_processed'} = 1;
		} else {
			my $error = $@;
			$self->_wrap(code => sub { die($error); });
		}
	}
}

sub _process {
	my $self = shift;

	if ($self->_has_special_triggers()) {
		$self->_log_can_not_process_ar_triggers();
		$self->{'_is_processed'} = 1;
	}

	my $timing;
	if (not $self->{'_is_processed'}) {
		$self->{'_bloat_statistics'} = $self->_get_bloat_statistics();

		if (not defined
			$self->{'_bloat_statistics'}->{'effective_page_count'})
		{
			$self->_do_analyze(timing => \ $timing);
			$self->_log_analyze_complete(
				timing => $timing, phrase => 'required initial');
			$self->{'_bloat_statistics'} = $self->_get_bloat_statistics();
		}

		$self->{'_size_statistics'} = $self->_get_size_statistics();

		if (not defined $self->{'_base_size_statistics'}) {
			$self->{'_base_size_statistics'} = {%{$self->{'_size_statistics'}}};
		}

		if (not $self->{'_force'} and
			$self->{'_size_statistics'}->{'page_count'} <
			$self->{'_min_page_count'})
		{
			$self->_log_skipping_min_page_count(
				page_count => $self->{'_size_statistics'}->{'page_count'});
			$self->{'_is_processed'} = 1;
		}
	}

	if (not $self->{'_is_processed'} and not $self->{'_no_initial_vacuum'}) {
		$self->_do_vacuum(timing => \ $timing);
		$self->{'_bloat_statistics'} = $self->_get_bloat_statistics();
		$self->{'_size_statistics'} = $self->_get_size_statistics();
		$self->_log_vacuum_complete(
			page_count => $self->{'_size_statistics'}->{'page_count'},
			timing => $timing,
			to_page => $self->{'_size_statistics'}->{'page_count'} - 1,
			pages_before_vacuum => $self->{'_size_statistics'}->{'page_count'},
			phrase => 'initial');

		if (not $self->{'_force'}) {
			if ($self->{'_size_statistics'}->{'page_count'} <
				$self->{'_min_page_count'})
			{
				$self->_log_skipping_min_page_count(
					page_count => $self->{'_size_statistics'}->{'page_count'});
				$self->{'_is_processed'} = 1;
			}

			if ($self->{'_bloat_statistics'}->{'free_percent'} <
				$self->{'_min_free_percent'} and not $self->{'_is_processed'})
			{
				$self->_log_skipping_min_free_percent(
					free_percent => (
						$self->{'_bloat_statistics'}->{'free_percent'}));
				$self->{'_is_processed'} = 1;
			}
		}
	}

	if (not $self->{'_is_processed'}) {
		if ($self->{'_force'}) {
			$self->_log_forced_processing(
				size_statistics => $self->{'_size_statistics'},
				bloat_statistics => $self->{'_bloat_statistics'});
		} else {
			$self->_log_processing(
				size_statistics => $self->{'_size_statistics'},
				bloat_statistics => $self->{'_bloat_statistics'});
		}

		my $expected_page_count = $self->{'_size_statistics'}->{'page_count'};
		my $column_ident = $self->{'_database'}->quote_ident(
			string => $self->_get_update_column());
		my $pages_per_round = $self->_get_pages_per_round(
			page_count => $self->{'_size_statistics'}->{'page_count'});
		my $pages_before_vacuum = $self->_get_pages_before_vacuum(
			expected_page_count => $expected_page_count,
			page_count => $self->{'_size_statistics'}->{'page_count'});
		$self->_log_column(name => $column_ident);
		$self->_log_pages_per_round(value => $pages_per_round);
		$self->_log_pages_before_vacuum(value => $pages_before_vacuum);

		my $vacuum_page_count = 0;
		my $initial_size_statistics = {%{$self->{'_size_statistics'}}};
		my $to_page = $self->{'_size_statistics'}->{'page_count'} - 1;
		my $progress_report_time = $self->_time();
		my $clean_pages_total_timing = 0;
		my $last_loop = $self->{'_size_statistics'}->{'page_count'} + 1;
		my $max_tupples_per_page = $self->_get_max_tupples_per_page();
		my $expected_error_occurred = 0;

		my $loop;
		for ($loop = $self->{'_size_statistics'}->{'page_count'};
			 $loop > 0 ; $loop--)
		{
			my $start_time = $self->_time();

			my $last_to_page = $to_page;
			eval {
				$to_page = $self->_clean_pages(
					timing => \ $timing,
					column_ident => $column_ident,
					to_page => $last_to_page,
					pages_per_round => $pages_per_round,
					max_tupples_per_page => $max_tupples_per_page);
				$clean_pages_total_timing = $clean_pages_total_timing + $timing;
			};
			if ($@) {
				if ($@ =~ 'No more free space left in the table') {
					# Normal cleaning completion
				} elsif ($@ =~ 'deadlock detected') {
					$self->_log_deadlock_detected();
					$expected_error_occurred = 1;
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
					timing => $clean_pages_total_timing / ($last_loop - $loop));
				$clean_pages_total_timing = 0;
				$last_loop = $loop;

				$self->_do_vacuum(timing => \ $timing);
				$self->{'_size_statistics'} = $self->_get_size_statistics();
				$self->_log_vacuum_complete(
					page_count => $self->{'_size_statistics'}->{'page_count'},
					timing => $timing,
					to_page => $to_page,
					pages_before_vacuum => $pages_before_vacuum,
					phrase => 'routine');

				$vacuum_page_count = 0;

				my $last_pages_per_round = $pages_per_round;
				$pages_per_round = $self->_get_pages_per_round(
					page_count => $self->{'_size_statistics'}->{'page_count'});
				if ($last_pages_per_round != $pages_per_round) {
					$self->_log_pages_per_round(
						value => $pages_per_round);
				}

				my $last_pages_before_vacuum = $pages_before_vacuum;
				$pages_before_vacuum = $self->_get_pages_before_vacuum(
					expected_page_count => $expected_page_count,
					page_count => $self->{'_size_statistics'}->{'page_count'});
				if ($last_pages_before_vacuum != $pages_before_vacuum) {
					$self->_log_pages_before_vacuum(
						value => $pages_before_vacuum);
				}

				if ($to_page >
					$self->{'_size_statistics'}->{'page_count'} - 1)
				{
					$to_page = $self->{'_size_statistics'}->{'page_count'} - 1;
				}
			}
		}

		if ($loop == 0) {
			$self->_log_max_loops();
		}

		$self->_do_vacuum(timing => \ $timing);
		$self->{'_bloat_statistics'} = $self->_get_bloat_statistics();
		$self->{'_size_statistics'} = $self->_get_size_statistics();
		$self->_log_vacuum_complete(
			page_count => $self->{'_size_statistics'}->{'page_count'},
			timing => $timing,
			to_page => $to_page + $pages_per_round,
			pages_before_vacuum => $pages_before_vacuum,
			phrase => 'final');

		if (not $self->{'_no_final_analyze'}) {
			$self->_do_analyze(timing => \ $timing);
			$self->_log_analyze_complete(timing => $timing, phrase => 'final');
		}

		$pages_before_vacuum = $self->_get_pages_before_vacuum(
			expected_page_count => $expected_page_count,
			page_count => $self->{'_size_statistics'}->{'page_count'});
		$self->{'_is_processed'} = (
			($self->{'_size_statistics'}->{'page_count'} <=
			 $to_page + 1 + $pages_before_vacuum) and
			not $expected_error_occurred);

		if ($self->{'_is_processed'} and $self->{'_reindex'}) {
			$self->_reindex(timing => \ $timing);
			$self->_log_reindex_complete(timing => $timing);
			$self->{'_size_statistics'} = $self->_get_size_statistics();
		}

		if ($self->{'_is_processed'} and $self->{'_print_reindex_queries'}) {
			$self->_log_reindex_queries();
		}

		if ($self->{'_is_processed'}) {
			$self->_log_complete_processing(
				size_statistics => $self->{'_size_statistics'},
				bloat_statistics => $self->{'_bloat_statistics'},
				base_size_statistics => $self->{'_base_size_statistics'});
		} else {
			$self->_log_incomplete_processing(
				size_statistics => $self->{'_size_statistics'},
				bloat_statistics => $self->{'_bloat_statistics'},
				base_size_statistics => $self->{'_base_size_statistics'});
		}
	}

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

=head2 B<get_ident()>

Returns a table ident.

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

sub _log_can_not_process_ar_triggers {
	my $self = shift;

	$self->{'_logger'}->write(
		message => (
			'Can not process: "always" or "replica" triggers are on.'),
		level => 'warning',
		target => $self->{'_log_target'});

	return;
}

sub _log_skipping_min_page_count {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Skipping processing: '.$arg_hash{'page_count'}.' pages from '.
			$self->{'_min_page_count'}.' minimum required.'),
		level => 'notice',
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
				'Vacuum '.$arg_hash{'phrase'}.': '.
				sprintf("%.3f", $arg_hash{'timing'}).'s, can not clean '.
				($arg_hash{'page_count'} - $arg_hash{'to_page'} - 1).' pages, '.
				$arg_hash{'page_count'}.' pages left.'),
			level => $level,
			target => $self->{'_log_target'});
	} else {
		$self->{'_logger'}->write(
			message => (
				'Vacuum '.$arg_hash{'phrase'}.': '.
				sprintf("%.3f", $arg_hash{'timing'}).'s, '.
				$arg_hash{'page_count'}.' pages left.'),
			level => 'info',
			target => $self->{'_log_target'});
	}

	return;
}

sub _log_skipping_min_free_percent {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Skipping processing: '.$arg_hash{'free_percent'}.
			'% space to compact from '.$self->{'_min_free_percent'}.
			'% minimum required.'),
		level => 'notice',
		target => $self->{'_log_target'});

	return;
}

sub _log_forced_processing {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Forced processing: '.
			$self->_get_log_processing_expectations(
				size_statistics => $arg_hash{'size_statistics'},
				bloat_statistics => $arg_hash{'bloat_statistics'}).'.'),
		level => 'notice',
		target => $self->{'_log_target'});

	return;
}

sub _log_processing {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Processing: '.
			$self->_get_log_processing_expectations(
				size_statistics => $arg_hash{'size_statistics'},
				bloat_statistics => $arg_hash{'bloat_statistics'}).'.'),
		level => 'notice',
		target => $self->{'_log_target'});

	return;
}

sub _get_log_processing_expectations {
	my ($self, %arg_hash) = @_;

	return
		'contains '.$arg_hash{'size_statistics'}->{'page_count'}.' pages ('.
		$arg_hash{'size_statistics'}->{'total_page_count'}.
		' including toasts and indexes), approximately '.
		$arg_hash{'bloat_statistics'}->{'free_percent'}.'% ('.
		($arg_hash{'size_statistics'}->{'page_count'} -
		 $arg_hash{'bloat_statistics'}->{'effective_page_count'}).
		' pages) are expected to be compacted reducing the size by '.
		$arg_hash{'bloat_statistics'}->{'free_space'}.
		' bytes after this attempt';
}

sub _log_column {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Column to perform updates by: '.$arg_hash{'name'}.'.',
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_pages_per_round {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Pages to process per round: '.$arg_hash{'value'}.'.',
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_pages_before_vacuum {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Pages to process before vacuum: '.$arg_hash{'value'}.'.',
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_clean_pages_average {
	my ($self, %arg_hash) = @_;

	my $timing = sprintf("%.3f", $arg_hash{'timing'});

	$self->{'_logger'}->write(
		message => (
			'Cleaning in average: '.
			sprintf("%.1f", $arg_hash{'pages_per_round'} / $timing).
			' pages/s ('.$timing.'s per '.$arg_hash{'pages_per_round'}.
			' pages).'),
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
		message => ('Analyze '.$arg_hash{'phrase'}.': '.
					sprintf("%.3f", $arg_hash{'timing'}).'s.'),
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_reindex_complete {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => 'Reindex: '.sprintf("%.3f", $arg_hash{'timing'}).'s.',
		level => 'info',
		target => $self->{'_log_target'});

	return;
}

sub _log_reindex_queries {
	my $self = shift;

	$self->{'_logger'}->write(
		message => ('Reindex queries:'."\n".
					join("\n", @{$self->_get_reindex_queries()})),
		level => 'notice',
		target => $self->{'_log_target'});

	return;
}

sub _log_incomplete_processing {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Processing incomplete: '.
			$self->_get_log_processing_results(
				size_statistics => $arg_hash{'size_statistics'},
				bloat_statistics => $arg_hash{'bloat_statistics'},
				base_size_statistics => $arg_hash{'base_size_statistics'}).
			'.'),
		level => 'warning',
		target => $self->{'_log_target'});

	return;
}

sub _log_complete_processing {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'}->write(
		message => (
			'Processing complete: '.
			$self->_get_log_processing_results(
				size_statistics => $arg_hash{'size_statistics'},
				bloat_statistics => $arg_hash{'bloat_statistics'},
				base_size_statistics => $arg_hash{'base_size_statistics'}).
			'.'),
		level => 'notice',
		target => $self->{'_log_target'});

	return;
}

sub _get_log_processing_results {
	my ($self, %arg_hash) = @_;

	return
		'left '.$arg_hash{'size_statistics'}->{'page_count'}.' pages ('.
		$arg_hash{'size_statistics'}->{'total_page_count'}.
		' including toasts and indexes), size reduced by '.
		($arg_hash{'base_size_statistics'}->{'size'} -
		 $arg_hash{'size_statistics'}->{'size'}).' bytes ('.
		($arg_hash{'base_size_statistics'}->{'total_size'} -
		 $arg_hash{'size_statistics'}->{'total_size'}).
		' bytes including toasts and indexes) in total'.
		($arg_hash{'bloat_statistics'}->{'free_percent'} > 0 ?
		 ', approximately '.
		 $arg_hash{'bloat_statistics'}->{'free_percent'}.'% ('.
		 ($arg_hash{'size_statistics'}->{'page_count'} -
		  $arg_hash{'bloat_statistics'}->{'effective_page_count'}).
		 ' pages) that is '.$arg_hash{'bloat_statistics'}->{'free_space'}.
		 ' bytes more were expected to be compacted after this attempt' : '');
}

sub _log_deadlock_detected {
	my $self = shift;

	$self->{'_logger'}->write(
		message => 'Stopped processing as a deadlock has been detected.',
		level => 'warning',
		target => $self->{'_log_target'});

	return;
}

sub _log_cannot_extract_system_attribute {
	my $self = shift;

	$self->{'_logger'}->write(
		message => ('Stopped processing as a system attribute extraction '.
					'error has occurred.'),
		level => 'warning',
		target => $self->{'_log_target'});

	return;
}

sub _log_relation_does_not_exist {
	my $self = shift;

	$self->{'_logger'}->write(
		message => ('Stopped processing as a relation does not exist '.
					'error has occurred.'),
		level => 'warning',
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

	my $result = $self->{'_database'}->execute(
		sql => <<SQL
SELECT count(1) FROM pg_catalog.pg_trigger
WHERE
    tgrelid = '$self->{'_ident'}'::regclass AND
    tgtype & 16 = 8 AND
    tgenabled IN ('A', 'R')
SQL
		);

	return $result->[0]->[0];
}

sub _get_max_tupples_per_page {
	my $self = shift;

	my $result = $self->{'_database'}->execute(
		sql => <<SQL
SELECT ceil(current_setting('block_size')::real / sum(attlen))
FROM pg_catalog.pg_attribute
WHERE
    attrelid = '$self->{'_ident'}'::regclass AND
    attnum < 0;
SQL
		);

	return $result->[0]->[0];
}

sub _get_bloat_statistics {
	my $self = shift;

	my $result;
	if ($self->{'_pgstattuple_schema_ident'}) {
		$result = $self->{'_database'}->execute(
			sql => <<SQL
SELECT
    CASE
        WHEN free_percent = 0 THEN page_count
        ELSE ceil(page_count * (1 - free_percent / 100))
        END AS effective_page_count,
    free_percent, free_space
FROM $self->{'_pgstattuple_schema_ident'}.pgstattuple('$self->{'_ident'}')
CROSS JOIN (
    SELECT
        pg_catalog.pg_relation_size('$self->{'_ident'}')::real /
        current_setting('block_size')::integer AS page_count
) AS sq
SQL
			);
	} else {
		$result = $self->{'_database'}->execute(
			sql => <<SQL
SELECT
    effective_page_count,
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
        ELSE round(bs * (page_count - effective_page_count))
        END AS free_space
FROM (
    SELECT
        bs,
        ceil(size / bs) AS page_count,
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
        ) AS effective_page_count
    FROM pg_catalog.pg_class
    LEFT JOIN pg_catalog.pg_statistic ON starelid = pg_catalog.pg_class.oid
    CROSS JOIN (
        SELECT
            23 AS header_width, 8 AS ma,
            current_setting('block_size')::integer AS bs,
            pg_catalog.pg_relation_size('$self->{'_ident'}')::real AS size
    ) AS const
    WHERE pg_catalog.pg_class.oid = '$self->{'_ident'}'::regclass
    GROUP BY pg_catalog.pg_class.oid, reltuples, header_width, ma, bs, size
) AS sq
SQL
			);
	}

	return {
		'effective_page_count' => $result->[0]->[0],
		'free_percent' => $result->[0]->[1],
		'free_space' => $result->[0]->[2]};
}

sub _get_size_statistics {
	my $self = shift;

	my $result = $self->{'_database'}->execute(
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

	return {
		'size' => $result->[0]->[0],
		'total_size' => $result->[0]->[1],
		'page_count' => $result->[0]->[2],
		'total_page_count' => $result->[0]->[3]};
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

sub _do_analyze {
	my ($self, %arg_hash) = @_;

	${$arg_hash{'timing'}} = $self->_time();

	$self->{'_database'}->execute(sql => 'ANALYZE '.$self->{'_ident'});

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
            FROM pg_catalog.pg_index
            WHERE indrelid = '$self->{'_ident'}'::regclass)),
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
SELECT public._clean_pages(
    '$self->{'_ident'}', '$arg_hash{'column_ident'}', $arg_hash{'to_page'},
    $arg_hash{'pages_per_round'}, $arg_hash{'max_tupples_per_page'})
SQL
		);

	${$arg_hash{'timing'}} = $self->_time() - ${$arg_hash{'timing'}};

	return $result->[0]->[0];
}

sub _get_reindex_queries {
	my $self = shift;

	my $result = $self->{'_database'}->execute(
		sql => <<SQL
SELECT indexname, tablespace, indexdef FROM pg_catalog.pg_indexes
WHERE
    schemaname = '$self->{'_schema_name'}' AND
    tablename = '$self->{'_table_name'}' AND
    NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_depend
        WHERE
            deptype='i' AND
            objid = (quote_ident(schemaname) || '.' ||
                     quote_ident(indexname))::regclass) AND
    NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_depend
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

		push(@{$query_list}, $definition.';');
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
		  $arg_hash{'page_count'} /
		  $self->{'_pages_per_round_divisor'},
		  1)[0],
		 $self->{'_max_pages_per_round'})[0]);
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
