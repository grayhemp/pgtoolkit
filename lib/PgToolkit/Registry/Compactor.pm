package PgToolkit::Registry::Compactor;

use base qw(PgToolkit::Class);

use strict;
use warnings;

use PgToolkit::Compactor::Cluster;
use PgToolkit::Compactor::Database;
use PgToolkit::Compactor::Table;
use PgToolkit::Database::Dbi;
use PgToolkit::Database::Psql;
use PgToolkit::DatabaseChooser;
use PgToolkit::Logger;
use PgToolkit::Options;
use PgToolkit::Utils;

=head1 NAME

B<PgToolkit::Registry::Compactor> - registry of the compactor components.

=head1 SYNOPSIS

	my $registry = PgToolkit::Registry::Compactor->new();

	$registry->get_cluster_compactor()->process();

=head1 DESCRIPTION

B<PgToolkit::Registry::Compactor> is a registry class that implements all the
services and their relationships that compactor tool uses.

=cut

=head1 METHODS

=head2 B<get_cluster_compactor()>

A cluster compactor prototype service.

=cut

sub get_cluster_compactor {
	my $self = shift;

	my $options = $self->get_options();

	return PgToolkit::Compactor::Cluster->new(
		database_constructor => sub {
			my %arg_hash = @_;
			return $self->get_database_adapter(
				dbname => $arg_hash{'dbname'});
		},
		logger => $self->get_logger(),
		dry_run => $options->get(name => 'dry-run'),
		database_compactor_constructor => sub {
			my %arg_hash = @_;
			return $self->get_database_compactor(
				database => $arg_hash{'database'});
		},
		dbname_list => $options->get(name => 'dbname'),
		excluded_dbname_list => $options->get(name => 'exclude-dbname'),
		max_retry_count => $options->get(name => 'max-retry-count'));
}

=head2 B<get_database_compactor()>

A database compactor prototype service.

=cut

sub get_database_compactor {
	my ($self, %arg_hash) = @_;

	my $options = $self->get_options();

	return PgToolkit::Compactor::Database->new(
		database => $arg_hash{'database'},
		logger => $self->get_logger(),
		dry_run => $options->get(name => 'dry-run'),
		table_compactor_constructor => sub {
			my %arg_hash = @_;
			return $self->get_table_compactor(
				database => $arg_hash{'database'},
				schema_name => $arg_hash{'schema_name'},
				table_name => $arg_hash{'table_name'},
				pgstattuple_schema_name => (
					$arg_hash{'pgstattuple_schema_name'}));
		},
		schema_name_list => $options->get(name => 'schema'),
		excluded_schema_name_list => $options->get(name => 'exclude-schema'),
		table_name_list => $options->get(name => 'table'),
		excluded_table_name_list => $options->get(name => 'exclude-table'),
		no_pgstatuple => $options->get(name => 'no-pgstattuple'),
		system_catalog => $options->get(name => 'system-catalog'));
}

=head2 B<get_table_compactor()>

A table compactor prototype service.

=cut

sub get_table_compactor {
	my ($self, %arg_hash) = @_;

	my $options = $self->get_options();

	return PgToolkit::Compactor::Table->new(
		database => $arg_hash{'database'},
		logger => $self->get_logger(),
		dry_run => $options->get(name => 'dry-run'),
		toast_compactor_constructor => sub {
			return $self->get_table_compactor(%arg_hash, @_);
		},
		toast_parent_ident => $arg_hash{'toast_parent_ident'},
		schema_name => $arg_hash{'schema_name'},
		table_name => $arg_hash{'table_name'},
		min_page_count => $options->get(name => 'min-page-count'),
		min_free_percent => $options->get(name => 'min-free-percent'),
		max_pages_per_round => $options->get(name => 'max-pages-per-round'),
		no_initial_vacuum => $options->get(name => 'no-initial-vacuum'),
		no_routine_vacuum => $options->get(name => 'no-routine-vacuum'),
		no_final_analyze => $options->get(name => 'no-final-analyze'),
		delay_constant => $options->get(name => 'delay-constant'),
		delay_ratio => $options->get(name => 'delay-ratio'),
		force => $options->get(name => 'force'),
		reindex => $options->get(name => 'reindex'),
		print_reindex_queries => $options->get(name => 'print-reindex-queries'),
		progress_report_period => $options->get(
			name => 'progress-report-period'),
		pgstattuple_schema_name => $arg_hash{'pgstattuple_schema_name'},
		pages_per_round_divisor => 1000,
		pages_before_vacuum_lower_divisor => 16,
		pages_before_vacuum_lower_threshold => 1000,
		pages_before_vacuum_upper_divisor => 50,
		max_retry_count => $options->get(name => 'max-retry-count'),
		locked_alter_timeout => 1000,
		locked_alter_count => 3600);
}

=head2 B<get_database_adapter()>

An availble on the system database adapter prototype service.

=cut

sub get_database_adapter {
	my ($self, %arg_hash) = @_;

	my $options = $self->get_options();

	my %hpud_hash = (
		host => $options->get(name => 'host'),
		port => $options->get(name => 'port'),
		user => $options->get(name => 'user'),
		dbname => $arg_hash{'dbname'});

	my $hard_session_params = {
                        'lc_messages' => '\'C\'',
                        'synchronous_commit' => 'off',
                        'session_replication_role' => 'replica',
                        'statement_timeout' => '\'0\''
                        };

	my $custom_session_params = $options->get(name => 'custom-session-param');
	my $session_params = { %$custom_session_params, %$hard_session_params };

	my %param_hash = (
		password => (
			$options->get(name => 'password') or
			PgToolkit::Utils->get_pgpass_password(
				pgpassfile => (
					($ENV{'PGPASSFILE'} and -r $ENV{'PGPASSFILE'}) ?
					$ENV{'PGPASSFILE'} : undef),
				home => (
					($ENV{'HOME'} and -r $ENV{'HOME'}.'/.pgpass') ?
					$ENV{'HOME'}.'/.pgpass' : undef),
				%hpud_hash)),
		set_hash => $session_params,
		%hpud_hash);

	my $constructor_list = [
		sub {
			return PgToolkit::Database::Dbi->new(
				driver => 'Pg', %param_hash);
		},
		sub {
			return PgToolkit::Database::Dbi->new(
				driver => 'PgPP',%param_hash);
		},
		sub {
			return PgToolkit::Database::Psql->new(
				path => $options->get(name => 'path-to-psql'), %param_hash);
		}];

	return PgToolkit::DatabaseChooser->new(
		constructor_list => $constructor_list);
}

=head2 B<get_logger()>

A logger lazy loader service.

=cut

sub get_logger {
	my $self = shift;

	if (not defined $self->{'_logger'}) {
		my $options = $self->get_options();

		$self->{'_logger'} = PgToolkit::Logger->new(
			level => $options->get(name => 'verbosity'));
	}

	return $self->{'_logger'};
}

=head2 B<get_options()>

A options lazy loader service.

=cut

sub get_options {
	my $self = shift;

	if (not defined $self->{'_options'}) {
		$self->{'_options'} = PgToolkit::Options->new(
			definition_hash => {
				# connection
				'host|h:s' => undef,
				'port|p:i' => undef,
				'user|U:s' => undef,
				'password|W:s' => undef,
				'path-to-psql|P:s' => 'psql',
				# target
				'all|a:i' => 1,
				'dbname|d:s@' => [],
				'schema|n:s@' => [],
				'table|t:s@' => [],
				'exclude-dbname|D:s@' => [],
				'exclude-schema|N:s@' => [],
				'exclude-table|T:s@' => [],
				# behaviour
				'dry-run|u' => 0,
				'no-initial-vacuum|I' => 0,
				'no-routine-vacuum|R' => 0,
				'no-final-analyze|L' => 0,
				'no-pgstattuple|S' => 0,
				'system-catalog|C' => 0,
				'reindex|r' => 0,
				'print-reindex-queries|s' => 0,
				'force|f' => 0,
				'max-pages-per-round|c:i' => 10,
				'delay-constant|e:i' => 0,
				'delay-ratio|E:i' => 4,
				'max-retry-count|o:i' => 10,
				'min-page-count|x:i' => 10,
				'min-free-percent|y:i' => 20,
				'progress-report-period|z:i' => 60,
				'custom-session-param|b:s%' => {},
				# misc
				'quiet|q' => 0,
				'verbosity|v:s' => 'notice'},
			error_check_code => sub {
				my $option_hash = shift;

				my $error;

				if (exists $option_hash->{'quiet'} and
					exists $option_hash->{'verbosity'})
				{
					$error = (
						'These options can not be specified simultaniously: '.
						'quiet, verbosity');
				} elsif (
					not exists $option_hash->{'all'} and
					not exists $option_hash->{'dbname'})
				{
					$error = (
						'At least one of the options must be specified: '.
						'all, dbname');
				};

				return $error;
			},
			transform_code => sub {
				my $option_hash = shift;
				if (exists $option_hash->{'quiet'}) {
					$option_hash->{'verbosity'} = 'warning';
				}
			},
			kit => 'PgToolkit',
			version => 'v1.0.2');
	}

	return $self->{'_options'};
}

=head1 SEE ALSO

=over 4

=item L<PgToolkit::Class>

=item L<PgToolkit::Compactor::Cluster>

=item L<PgToolkit::Compactor::Database>

=item L<PgToolkit::Compactor::Schema>

=item L<PgToolkit::Compactor::Table>

=item L<PgToolkit::Database::Dbi>

=item L<PgToolkit::Database::Psql>

=item L<PgToolkit::DatabaseChooser>

=item L<PgToolkit::Logger>

=item L<PgToolkit::Options>

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
