package PgToolkit::Database::Dbi;

use base qw(PgToolkit::Database);

use strict;
use warnings;

=head1 NAME

B<PgToolkit::Database::Dbi> - a DBI facade class.

=head1 SYNOPSIS

	my $database = PgToolkit::Database::Dbi->new(
		driver => 'Pg', host => 'somehost', port => '5432',
		dbname => 'somedb', user => 'someuser', password => 'secret');

	my $result = $database->execute(sql => 'SELECT * FROM sometable;');

=head1 DESCRIPTION

B<PgToolkit::Database::Dbi> is a simplification of the DBI interface.

=head3 Constructor arguments

=over 4

=item C<driver>

=item C<host>

by default socket connection is used,

=item C<port>

=item C<dbname>

=item C<user>

=item C<password>

=back

For default argument values see the specific B<DBD::*> driver
documentation.

=head3 Throws

=over 4

=item C<DatabaseError>

when either the DBI module or the specified driver not found.

=back

=cut

sub init {
	my ($self, %arg_hash) = @_;

	$self->SUPER::init(%arg_hash);

	$self->{'_driver'} = $arg_hash{'driver'};

	eval { require DBI; };
	if ($@) {
		die('DatabaseError DBI module not found.');
	}

	if (not grep($_ eq $arg_hash{'driver'}, DBI->available_drivers())) {
		die('DatabaseError No driver found "'.$arg_hash{'driver'}.'".');
	}

	$self->{'dbh'} = DBI->connect(
		'dbi:'.$arg_hash{'driver'}.
		':dbname='.($arg_hash{'dbname'} ? $self->_get_escaped_dbname() : '').
		(defined $arg_hash{'host'} ? ';host='.$arg_hash{'host'} : '').
		';port='.($arg_hash{'port'} or ''),
		$arg_hash{'user'}, $arg_hash{'password'},
		{
			 RaiseError => 1, ShowErrorStatement => 1, AutoCommit => 1,
			 PrintWarn => 0, PrintError => 0,
			 pg_server_prepare => 0, pg_enable_utf8 => 0
		});

	if ($arg_hash{'set_hash'}) {
		$self->execute(
			sql => join(
				' ',
				map(
					'SET '.$_.' TO '.$arg_hash{'set_hash'}->{$_}.';',
					keys %{$arg_hash{'set_hash'}})));
	}

	return;
}

=head1 METHODS

=head2 B<execute()>

Executes an SQL.

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

sub _execute {
	my ($self, %arg_hash) = @_;

	my $result;
	eval {
		if ($arg_hash{'sql'} =~ /^SELECT/) {
			$self->{'sth'} = $self->{'dbh'}->prepare($arg_hash{'sql'});
			$self->{'sth'}->execute();
			$result = $self->{'sth'}->fetchall_arrayref();
		} else {
			$self->{'dbh'}->do($arg_hash{'sql'});
		}
	};
	if ($@) {
		die('DatabaseError '.$@);
	}

	return $result
}

=head2 B<get_adapter_name()>

Returns the name of the adapter.

=head3 Returns

A string representing the name.

=cut

sub get_adapter_name {
	my $self = shift;

	return 'DBI/'.$self->{'_driver'};
}

=head1 SEE ALSO

=over 4

=item L<DBI>

=item L<PgToolkit::Database>

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
