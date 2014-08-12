package PgToolkit::Database;

use base qw(PgToolkit::Class);

use strict;
use warnings;

use POSIX;
use Time::HiRes qw(time sleep);

=head1 NAME

B<PgToolkit::Database> - database abstract class.

=head1 SYNOPSIS

	package SomeDatabase;

	use base qw(PgToolkit::Database);

	sub init {
		my ($self, %arg_hash) = @_;

		$self->SUPER::init(%arg_hash);

		# some initialization

		return;
	}

	sub _execute {
		# some implementation
	}

	1;

=head1 DESCRIPTION

B<PgToolkit::Database> is a base class for database adapters.

=head3 Constructor arguments

=over 4

=item C<dbname>

=back

=cut

sub init {
	my ($self, %arg_hash) = @_;

	$self->{'_dbname'} = $arg_hash{'dbname'};

	return;
}

=head1 METHODS

=head2 B<execute()>

Executes an SQL.

The method _execute() must be implemented in derivative classes.

=head3 Arguments

=over 4

=item C<sql>

an SQL string.

=back

=head3 Returns

An array of arrays representing the result.

=head3 Throws

=over 4

=item C<DatabaseError>

when the database raised an error during execution of the SQL.

=back

=cut

sub execute {
	my ($self, %arg_hash) = @_;

	my $time = $self->_time();
	my $result = $self->_execute(%arg_hash);
	$self->{'_duration'} = $self->_time() - $time;

	return $result;
}

sub _execute {
	die('NotImplementedError');
}

=head2 B<get_duration()>

Returns a duration of the last query.

=head3 Returns

A high resolution time in seconds.

=cut

sub get_duration {
	my $self = shift;

	return $self->{'_duration'};
}

=head2 B<get_adapter_name()>

Returns the name of the adapter.

This method must be implemented in derivative classes.

=head3 Returns

A string representing the name.

=cut

sub get_adapter_name {
	die('NotImplementedError');
}

=head2 B<get_dbname()>

Returns the database name.

=head3 Returns

A string with the name.

=cut

sub get_dbname {
	my $self = shift;

	return $self->{'_dbname'};
}

=head2 B<_get_escaped_dbname()>

Returns an escaped database name.

=head3 Returns

A database name string with all the non-word characters escaped.

=cut

sub _get_escaped_dbname {
	my $self = shift;

	my $result = $self->{'_dbname'};
	$result =~ s/(\W)/\\$1/g;

	return $result;
}

=head2 B<quote_ident()>

=head3 Arguments

=over 4

=item C<string>

=back

=head3 Returns

A quoted indentifier string.

=head3 Throws

=over 4

=item C<DatabaseError>

when nothing to ident.

=back

=cut

sub quote_ident {
	my ($self, %arg_hash) = @_;

	if (not $arg_hash{'string'}) {
		die('DatabaseError Nothing to ident.');
	}

	my $string = $arg_hash{'string'};
	$string =~ s/'/''/g;

	return $self->_quote_ident(string => $string);
}

sub _quote_ident {
	my ($self, %arg_hash) = @_;

	my $result = $self->execute(
		sql => "SELECT quote_ident('".$arg_hash{'string'}."')");

	return $result->[0]->[0];
}

sub _time {
	return time();
}

=head2 B<get_major_version()>

Returns a major version of the database.

=head3 Returns

A database version string like "9.0".

=cut

sub get_major_version {
	my $self = shift;

	if (not defined $self->{'_major_version'}) {
		$self->{'_major_version'} = $self->execute(
			sql => <<SQL
SELECT regexp_replace(
	version(),
	E'.*PostgreSQL (\\\\d+\\\\.\\\\d+).*',
	E'\\\\1');
SQL
			)->[0]->[0];
	}

	return $self->{'_major_version'};
}

=head1 SEE ALSO

=over 4

=item L<PgToolkit::Class>

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
