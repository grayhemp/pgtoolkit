package Pc::Database;

use parent qw(Pc::Class);

use strict;
use warnings;

=head1 NAME

B<Pc::Database> - a database abstract class.

=head1 SYNOPSIS

	package SomeDatabase;

	use parent qw(Pc::Database);

	sub init {
		my ($self, %arg_hash) = @_;

		$self->SUPER::init(%arg_hash);

		# some initialization

		return;
	}

	sub execute {
		# some implementation
	}

	1;

=head1 DESCRIPTION

B<Pc::Database> is a base class for database adapters.

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

The method must be implemented in derivative classes.

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

=head2 B<get_dbname()>

Returns an escaped database name.

=head3 Returns

A database name string with all the non-word characters escaped.

=cut

sub _get_escaped_dbname {
	my $self = shift;

	my $string = $self->{'_dbname'};
	$string =~ s/(\W)/\\$1/g;

	return $string;
}

=head2 B<quote_ident()>

=head3 Arguments

=over 4

=item C<string>

=back

=head3 Returns

A quoted indentifier string.

=cut

sub quote_ident {
	my ($self, %arg_hash) = @_;

	my $string = $arg_hash{'string'};
	$string =~ s/"/""/g;

	return '"'.$string.'"';
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
