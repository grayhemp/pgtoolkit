package PgToolkit::Database::Psql;

use base qw(PgToolkit::Database);

use strict;
use warnings;

use IPC::Open3;

=head1 NAME

B<PgToolkit::Database::Psql> - a psql facade class.

=head1 SYNOPSIS

	my $database = PgToolkit::Database::Psql->new(
		path => '/path/to/psql', host => 'somehost', port => '5432',
		dbname => 'somedb', user => 'someuser',password => 'secret');

	my $result = $database->execute(sql => 'SELECT * FROM sometable;');

=head1 DESCRIPTION

B<PgToolkit::Database::Psql> is a psql utility adaptation class.

=head3 Constructor arguments

=over 4

=item C<path>

a path to psql, default 'psql'

=item C<host>

=item C<port>

=item C<dbname>

=item C<user>

=item C<password>

=back

For default argument values see the specific the psql documentation.

=head3 Throws

=over 4

=item C<DatabaseError>

if can not run psql.

=back

=cut

sub init {
	my ($self, %arg_hash) = @_;

	$self->SUPER::init(%arg_hash);

	my %opt_hash = ();
	$opt_hash{'password'} = (defined $arg_hash{'password'}) ?
		'PGPASSWORD='.$arg_hash{'password'}.' ' : '';
	$opt_hash{'path'} = (defined $arg_hash{'path'}) ?
		$arg_hash{'path'} : 'psql';
	$opt_hash{'host'} = (defined $arg_hash{'host'}) ?
		'-h '.$arg_hash{'host'} : '';
	$opt_hash{'port'} = (defined $arg_hash{'port'}) ?
		'-p '.$arg_hash{'port'} : '';
	$opt_hash{'dbname'} = (defined $arg_hash{'dbname'}) ?
		'-d '.$self->_get_escaped_dbname() : '';
	$opt_hash{'user'} = (defined $arg_hash{'user'}) ?
		'-U '.$arg_hash{'user'} : '';

	$self->{'_command'} = sprintf(
		'%s%s -A -t -X %s %s %s %s -P null="<NULL>"',
		@opt_hash{'password', 'path', 'host', 'port', 'dbname', 'user'});
	$self->{'_command'} =~ s/\s+/ /g;

	eval {
		$self->_run_psql(
			command => $self->{'_command'}, sql => 'SELECT 1;');
	};
	if ($@) {
		if ($@ =~ 'DatabaseError') {
			die('DatabaseError Can not run psql.');
		} else {
			die($@);
		}
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

when problems appear during statement execution.

=back

=cut

sub _execute {
	my ($self, %arg_hash) = @_;

	my $raw_data = $self->_run_psql(
		command => $self->{'_command'}, sql => $arg_hash{'sql'});

	my $result = [];
	for my $row_data (split(qr/\n/, $raw_data)) {
		my $row = [];
		for my $cell_data (split(qr/\|/, $row_data)) {
			my $cell = ($cell_data eq '<NULL>') ? undef : $cell_data;
			push(@{$row}, $cell);
		}
		push(@{$result}, $row)
	}

	return $result;
}

=head2 B<get_adapter_name()>

Returns the name of the adapter.

=head3 Returns

A string representing the name.

=cut

sub get_adapter_name {
	return 'psql';
}

sub _run_psql {
	my ($self, %arg_hash) = @_;

	my $pid = open3(\*CHLD_IN, \*CHLD_OUT, \*CHLD_ERR, $arg_hash{'command'});
	print CHLD_IN $arg_hash{'sql'};
	close CHLD_IN;
	waitpid($pid, 0);
	my $exit_status = $? >> 8;

	my $err_output = join('', <CHLD_ERR>);

	if ($exit_status or ($err_output and $err_output =~ /^ERROR: /)) {
		die(join("\n", ('DatabaseError Can not execute the command',
						$arg_hash{'command'}, $arg_hash{'sql'},
						join('', <CHLD_OUT>), $err_output)));
	}

	return join('', <CHLD_OUT>);
}

=head1 SEE ALSO

=over 4

=item L<IPC::Open3>

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
