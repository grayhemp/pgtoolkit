package Pc::Registry::Compactor;

use parent qw(Pc::Class);

use strict;
use warnings;

use Pc::Compactor::Cluster;
use Pc::Compactor::Database;
use Pc::Compactor::Schema;
use Pc::Compactor::Table;
use Pc::Database::Dbi;
use Pc::Database::Psql;
use Pc::DatabaseChooser;
use Pc::Logger;
use Pc::Options;

=head1 NAME

B<Pc::Registry::Compactor> - a registry of the compactor components.

=head1 SYNOPSIS

	my $registry = Pc::Registry::Compactor->new();

	$registry->get_cluster_compactor()->process();

=head1 DESCRIPTION

B<Pc::Registry::Compactor> is a registry class that implements all the
services and their relationships that compactor tool uses.

=cut

=head1 METHODS

=head2 B<get_cluster_compactor()>

A cluster compactor prototype service.

=cut

sub get_cluster_compactor {
	my $self = shift;

	my $options = $self->get_options();

	return Pc::Compactor::Cluster->new(
		database_constructor => sub {
			my %arg_hash = @_;
			return $self->get_database_adapter(
				dbname => $arg_hash{'dbname'});
		},
		logger => $self->get_logger(),
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

	return Pc::Compactor::Database->new(
		database => $arg_hash{'database'},
		logger => $self->get_logger(),
		schema_compactor_constructor => sub {
			my %arg_hash = @_;
			return $self->get_schema_compactor(
				database => $arg_hash{'database'},
				schema_name => $arg_hash{'schema_name'},
				use_pgstattuple => $arg_hash{'use_pgstattuple'});
		},
		schema_name_list => $options->get(name => 'schema'),
		excluded_schema_name_list => $options->get(name => 'exclude-schema'));
}

=head2 B<get_schema_compactor()>

A schema compactor prototype service.

=cut

sub get_schema_compactor {
	my ($self, %arg_hash) = @_;

	my $options = $self->get_options();

	return Pc::Compactor::Schema->new(
		database => $arg_hash{'database'},
		logger => $self->get_logger(),
		schema_name => $arg_hash{'schema_name'},
		table_compactor_constructor => sub {
			my %arg_hash = @_;
			return $self->get_table_compactor(
				database => $arg_hash{'database'},
				schema_name => $arg_hash{'schema_name'},
				table_name => $arg_hash{'table_name'},
				use_pgstattuple => $arg_hash{'use_pgstattuple'});
		},
		table_name_list => $options->get(name => 'table'),
		excluded_table_name_list => $options->get(name => 'exclude-table'),
		use_pgstattuple => $arg_hash{'use_pgstattuple'});
}

=head2 B<get_table_compactor()>

A table compactor prototype service.

=cut

sub get_table_compactor {
	my ($self, %arg_hash) = @_;

	my $options = $self->get_options();

	return Pc::Compactor::Table->new(
		database => $arg_hash{'database'},
		logger => $self->get_logger(),
		schema_name => $arg_hash{'schema_name'},
		table_name => $arg_hash{'table_name'},
		min_page_count => $options->get(name => 'min-page-count'),
		min_free_percent => $options->get(name => 'min-free-percent'),
		pages_per_round => $options->get(name => 'pages-per-round'),
		no_initial_vacuum => $options->get(name => 'no-initial-vacuum'),
		no_routine_vacuum => $options->get(name => 'no-routine-vacuum'),
		delay_constant => $options->get(name => 'delay-constant'),
		delay_ratio => $options->get(name => 'delay-ratio'),
		force => $options->get(name => 'force'),
		reindex => $options->get(name => 'reindex'),
		print_reindex_queries => $options->get(name => 'print-reindex-queries'),
		progress_report_period => $options->get(
			name => 'progress-report-period'),
		use_pgstattuple => $arg_hash{'use_pgstattuple'});
}

=head2 B<get_database_adapter()>

An availble on the system database adapter prototype service.

=cut

sub get_database_adapter {
	my ($self, %arg_hash) = @_;

	my $options = $self->get_options();

	my %param_hash = (
		dbname => $arg_hash{'dbname'},
		host => $options->get(name => 'host'),
		port => $options->get(name => 'port'),
		user => $options->get(name => 'user'),
		password => $options->get(name => 'password'));

	my $constructor_list = [
		sub { return Pc::Database::Dbi->new(driver => 'Pg', %param_hash); },
		sub { return Pc::Database::Dbi->new(driver => 'PgPP',%param_hash); },
		sub {
			return Pc::Database::Psql->new(
				path => $options->get(name => 'path-to-psql'), %param_hash);
		}];

	return Pc::DatabaseChooser->new(constructor_list => $constructor_list);
}

=head2 B<get_logger()>

A logger lazy loader service.

=cut

sub get_logger {
	my $self = shift;

	if (not defined $self->{'_logger'}) {
		my $options = $self->get_options();

		$self->{'_logger'} = Pc::Logger->new(
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
		$self->{'_options'} = Pc::Options->new(
			definition_hash => {
				# connection
				'host|h:s' => 'localhost', 'port|p:i' => '5432',
				'user|U:s' => do { `whoami` =~ /(.*?)\n/; $1 },
				'password|W:s' => undef, 'path-to-psql|P:s' => 'psql',
				# target
				'dbname|d:s@' => [], 'schema|n:s@' => [], 'table|t:s@' => [],
				'exclude-dbname|D:s@' => [], 'exclude-schema|N:s@' => [],
				'exclude-table|T:s@' => [],
				# behaviour
				'no-initial-vacuum|I' => 0, 'no-routine-vacuum|R' => 0,
				'reindex|r' => 0, 'print-reindex-queries|s' => 0,
				'force|f' => 0, 'pages-per-round|c:i' => 5,
				'delay-constant|e:i' => 0, 'delay-ratio|E:i' => 2,
				'max-retry-count|m:i' => 10, 'min-page-count|x:i' => 100,
				'min-free-percent|y:i' => 5, 'progress-report-period|z:i' => 60,
				# misc
				'quiet|q' => 0, 'verbosity|v:s' => 'notice'},
			error_check_code => sub {
				my $option_hash = shift;
				return (
					(exists $option_hash->{'quiet'} and
					 exists $option_hash->{'verbosity'}) or
					(exists $option_hash->{'dbname'} and
					 exists $option_hash->{'exclude-dbname'}) or
					(exists $option_hash->{'schema'} and
					 exists $option_hash->{'exclude-schema'}) or
					(exists $option_hash->{'table'} and
					 exists $option_hash->{'exclude-table'}));
			},
			transform_code => sub {
				my $option_hash = shift;
				if (exists $option_hash->{'quiet'}) {
					$option_hash->{'verbosity'} = 'warning';
				}
			});
	}

	return $self->{'_options'};
}

=head1 SEE ALSO

=over 4

=item L<Pc::Class>

=item L<Pc::Compactor::Cluster>

=item L<Pc::Compactor::Database>

=item L<Pc::Compactor::Schema>

=item L<Pc::Compactor::Table>

=item L<Pc::Database::Dbi>

=item L<Pc::Database::Psql>

=item L<Pc::DatabaseChooser>

=item L<Pc::Logger>

=item L<Pc::Options>

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
