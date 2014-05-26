package PgToolkit::Compact::Cluster;

use base qw(PgToolkit::Compact);

use strict;
use warnings;

use PgToolkit::Utils;

=head1 NAME

B<PgToolkit::Compact::Cluster> - cluster level processing for bloat
reducing.

=head1 SYNOPSIS

	my $cluster_compact = PgToolkit::Compact::Cluster->new(
		database_constructor => $database_constructor,
		logger => $logger,
		dry_run => 0,
		database_compact_constructor => $database_compact_constructor,
		dbname_list => $dbname_list,
		excluded_dbname_list => $excluded_dbname_list,
		max_retry_count => 10);

	$cluster_compact->process();

=head1 DESCRIPTION

B<PgToolkit::Compact::Cluster> class is an implementation of a cluster
level processing logic for bloat reducing mechanism.

=head3 Constructor arguments

=over 4

=item C<database_constructor>

a database constructor code reference

=item C<logger>

a logger object

=item C<dry_run>

=item C<database_compact_constructor>

a database compact constructor code reference

=item C<dbname_list>

a list of database names to process

=item C<excluded_dbname_list>

a list of database names to exclude from processing

=item C<max_retry_count>

a maximum amount of attempts to compact cluster.

=back

=cut

sub _init {
	my ($self, %arg_hash) = @_;

	$self->{'_database_constructor'} = $arg_hash{'database_constructor'};
	$self->{'_max_retry_count'} = $arg_hash{'max_retry_count'};

	$self->{'_database'} =
		$self->{'_database_constructor'}->(dbname => 'postgres');

	$self->{'_logger'}->write(
		message => ('Database connection method: '.
					$self->{'_database'}->get_adapter_name().'.'),
		level => 'info');

	my $dbname_list = $self->_get_dbname_list(
		dbname_list => $arg_hash{'dbname_list'},
		excluded_dbname_list => $arg_hash{'excluded_dbname_list'});

	$self->{'_database_compact_list'} = [];
	for my $dbname (@{$dbname_list}) {
		my $database_compact =
			$arg_hash{'database_compact_constructor'}->(
				database => $self->{'_database_constructor'}->(
					dbname => $dbname));
		push(@{$self->{'_database_compact_list'}}, $database_compact);
	}

	return;
}

sub _process {
	my $self = shift;

	if (@{$self->{'_database_compact_list'}}) {
		my $attempt = 0;
		while (not $self->is_processed() and
			   $attempt <= $self->{'_max_retry_count'})
		{
			if ($attempt != 0) {
				$self->{'_logger'}->write(
					message => ('Retrying to process, attempt: '.$attempt.
								' from '.$self->{'_max_retry_count'}.', '.
								$self->_incomplete_count().' databases left.'),
					level => 'notice');
			}

			for my $database_compact (@{$self->{'_database_compact_list'}})
			{
				if (not $database_compact->is_processed()) {
					$database_compact->process(attempt => $attempt);
				}
			}

			$attempt++;
		}

		my $databases_size_delta_report = join(
			', ',
			map(
				PgToolkit::Utils->get_size_pretty(size => $_->get_size_delta()).
				' ('.PgToolkit::Utils->get_size_pretty(
					size => $_->get_total_size_delta()).') '.
				$_->get_log_target(),
				@{$self->{'_database_compact_list'}}));

		if (not $self->{'_dry_run'}) {
			if ($self->is_processed()) {
				$self->{'_logger'}->write(
					message => (
						'Processing complete: '.
						($attempt ? ($attempt - 1).' retries from '.
						 $self->{'_max_retry_count'} : ' no attempts to '.
						 'process have been done').'.'),
					level => 'notice');
			} else {
				$self->{'_logger'}->write(
					message => (
						'Processing incomplete: '.$self->_incomplete_count().
						' databases left.'),
					level => 'warning');
			}
			$self->{'_logger'}->write(
				message => (
					'Processing results: size reduced by '.
					PgToolkit::Utils->get_size_pretty(
						size => $self->get_size_delta()).' ('.
					PgToolkit::Utils->get_size_pretty(
						size => $self->get_total_size_delta()).' '.
					'including toasts and indexes) in total, '.
					$databases_size_delta_report.'.'),
				level => 'notice');
		}
	} else {
		$self->{'_logger'}->write(
			message => 'No databases to process.',
			level => 'warning');
	}

	return;
}

=head1 METHODS

=head2 B<is_processed()>

Tests if the cluster is processed.

=head3 Returns

True or false value.

=cut

sub is_processed {
	my $self = shift;

	my $result = 1;
	map(($result &&= $_->is_processed()),
		@{$self->{'_database_compact_list'}});

	return $result;
}

=head2 B<get_size_delta()>

Returns a size delta in bytes.

=head3 Returns

A number or undef if has not been processed.

=cut

sub get_size_delta {
	my $self = shift;

	my $result = 0;
	map($result += $_->get_size_delta(),
		@{$self->{'_database_compact_list'}});

	return $result;
}

=head2 B<get_total_size_delta()>

Returns a total (including toasts and indexes) size delta in bytes.

=head3 Returns

A number or undef if has not been processed.

=cut

sub get_total_size_delta {
	my $self = shift;

	my $result = 0;
	map($result += $_->get_total_size_delta(),
		@{$self->{'_database_compact_list'}});

	return $result;
}

sub _incomplete_count {
	my $self = shift;

	my $result = 0;
	map(($result += not $_->is_processed()),
		@{$self->{'_database_compact_list'}});

	return $result;
}

sub _get_dbname_list {
	my ($self, %arg_hash) = @_;

	my $in = '';
	if (@{$arg_hash{'dbname_list'}}) {
		$in =
			'datname IN ('.
			join(', ', map("'$_'", @{$arg_hash{'dbname_list'}})).
			') AND';
	}

	my $not_in = '';
	if (@{$arg_hash{'excluded_dbname_list'}}) {
		$not_in =
			'datname NOT IN ('.
			join(', ', map("'$_'", @{$arg_hash{'excluded_dbname_list'}})).
			') AND';
	}

	my $result = $self->_execute_and_log(
			sql => <<SQL
SELECT datname FROM pg_catalog.pg_database
WHERE
    $in
    $not_in
    datname NOT IN ('template0')
ORDER BY pg_catalog.pg_database_size(datname), datname
SQL
		);

	return [map($_->[0], @{$result})];
}

=head1 SEE ALSO

=over 4

=item L<PgToolkit::Class>
=item L<PgToolkit::Utils>

=back

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2011-2014 Sergey Konoplev

PgToolkit is released under the PostgreSQL License, read COPYRIGHT.md
for additional information.

=head1 AUTHOR

=over 4

=item L<Sergey Konoplev|mailto:gray.ru@gmail.com>

=back

=cut

1;
