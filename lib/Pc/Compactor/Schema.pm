package Pc::Compactor::Schema;

use parent qw(Pc::Class);

use strict;
use warnings;

=head1 NAME

B<Pc::Compactor::Schema> - a schema level processing for bloat reducing.

=head1 SYNOPSIS

	my $schema_compactor = Pc::Compactor::Schema->new(
		database => $database,
		logger => $logger,
		schema_name => $schema_name,
		table_compactor_constructor => $table_compactor_constructor,
		table_name_list => $table_name_list,
		excluded_table_name_list => $excluded_table_name_list,
		use_pgstattuple => 0);

	$schema_compactor->process();

=head1 DESCRIPTION

B<Pc::Compactor::Schema> class is an implementation of a schema level
processing logic for bloat reducing mechanism.

=head3 Constructor arguments

=over 4

=item C<database>

a database object

=item C<logger>

a logger object

=item C<schema_name>

a schema name to process

=item C<table_compactor_constructor>

a table compactor constructor code reference

=item C<table_name_list>

a list of table names to process

=item C<excluded_table_name_list>

a list of table names to exclude from processing

=item C<use_pgstattuple>

states whether we should use pgstattuple to get statistics or not.

=back

=head3 Throws

=over 4

=item C<SchemaCompactorError>

if there is no such schema.

=back

=cut

sub init {
	my ($self, %arg_hash) = @_;

	$self->{'_database'} = $arg_hash{'database'};
	$self->{'_logger'} = $arg_hash{'logger'};
	$self->{'_schema_name'} = $arg_hash{'schema_name'};

	$self->{'_ident'} = $self->{'_database'}->quote_ident(
		string => $self->{'_schema_name'});

	$self->{'_logger'}->write(
		message => 'Scanning the '.$self->{'_ident'}.' schema.',
		level => 'info');

	if (not $self->_has_schema()) {
		die('SchemaCompactorError There is no schema '.$self->{'_ident'}.'.');
	}

	my %table_name_hash = map(
		($_ => 1), @{$arg_hash{'table_name_list'}} ?
		@{$arg_hash{'table_name_list'}} : @{$self->_get_table_name_list()});

	delete @table_name_hash{@{$arg_hash{'excluded_table_name_list'}}};

	$self->{'_table_compactor_list'} = [];
	for my $table_name (sort keys %table_name_hash) {
		my $table_compactor;
		eval {
			$table_compactor = $arg_hash{'table_compactor_constructor'}->(
				database => $self->{'_database'},
				schema_name => $self->{'_schema_name'},
				table_name => $table_name,
				use_pgstattuple => $arg_hash{'use_pgstattuple'});
		};
		if ($@) {
			$self->{'_logger'}->write(
				message => (
					'Can not prepare the '.
					$self->{'_database'}->quote_ident(string => $table_name).
					' table in the '.$self->{'_ident'}.' schema to '.
					"compacting, the following error has occured:\n".$@),
				level => 'error');
		} else {
			push(@{$self->{'_table_compactor_list'}}, $table_compactor);
		}
	}

	return;
}

=head1 METHODS

=head2 B<process()>

Runs a bloat reducing process for the schema.

=cut

sub process {
	my $self = shift;

	$self->{'_logger'}->write(
		message => 'Processing the '.$self->{'_ident'}.' schema.',
		level => 'info');

	for my $table_compactor (@{$self->{'_table_compactor_list'}}) {
		if (not $table_compactor->is_processed()) {
			eval {
				$table_compactor->process();
			};
			if ($@) {
				$self->{'_logger'}->write(
					message => (
						'Can not process the '.$table_compactor->get_ident().
						' table in the '.$self->{'_ident'}.' schema, '.
						'the following error has occured:'."\n".$@),
					level => 'error');
			}
		}
	}

	$self->{'_logger'}->write(
		message => 'Finished processing the '.$self->{'_ident'}.' schema.',
		level => 'info');

	if (not $self->is_processed()) {
		$self->{'_logger'}->write(
			message => (
				'Processing of the '.$self->{'_ident'}.' schema has not '.
				'been completed.'),
			level => 'warning');
	}

	return;
}

=head2 B<is_processed()>

Tests if the schema is processed.

=head3 Returns

True or false value.

=cut

sub is_processed {
	my $self = shift;

	my $result = 1;
	map(($result &&= $_->is_processed()),
		@{$self->{'_table_compactor_list'}});

	return $result;
}

=head2 B<get_ident()>

Returns a schema ident.

=head3 Returns

A string representing the ident.

=cut

sub get_ident {
	my $self = shift;

	return $self->{'_ident'};
}

sub _get_table_name_list {
	my $self = shift;

	my $result = $self->{'_database'}->execute(
		sql => <<SQL
SELECT tablename FROM pg_tables
WHERE schemaname = '$self->{'_schema_name'}'
ORDER BY 1
SQL
		);

	return [map($_->[0], @{$result})];
}

sub _has_schema {
	my $self = shift;

	my $result = $self->{'_database'}->execute(
		sql => <<SQL
SELECT count(1) FROM pg_namespace WHERE nspname = '$self->{'_schema_name'}'
SQL
		);

	return $result->[0]->[0];
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
