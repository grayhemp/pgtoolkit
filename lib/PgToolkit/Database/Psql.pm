package PgToolkit::Database::Psql;

use base qw(PgToolkit::Database);

use strict;
use warnings;

use IPC::Open3;
use IO::Handle;

use PgToolkit::Utils;

=head1 NAME

B<PgToolkit::Database::Psql> - a psql facade class.

=head1 SYNOPSIS

	my $database = PgToolkit::Database::Psql->new(
		path => '/path/to/psql', host => 'somehost', port => '5432',
		dbname => 'somedb', user => 'someuser',password => 'secret',
		set_hash => {'statement_timeout' => 0});

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

=item C<set_hash>

a set of configuration parameters to set.

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

	$self->{'_set_hash'} = $arg_hash{'set_hash'};

	my $psql_command_line = sprintf(
		'%s%s -w -q -A -t -X %s %s %s %s -P null="<NULL>"',
		@opt_hash{'password', 'path', 'host', 'port', 'dbname', 'user'});
	$psql_command_line =~ s/\s+/ /g;

	$self->_start_psql(psql_command_line => $psql_command_line);

	eval {
		$self->execute(
			sql => join(
				' ',
				map(
					'SET '.$_.' TO '.$self->{'_set_hash'}->{$_}.';',
					keys %{$self->{'_set_hash'}}),
				'SELECT 1;'));
	};
	if ($@) {
		if ($@ =~ 'DatabaseError (.*)') {
			die('DatabaseError Can not execute command: '.$1);
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

	my $raw_data = $self->_send_to_psql(command => $arg_hash{'sql'});

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

sub _start_psql {
	my ($self, %arg_hash) = @_;

	$self->{'in'} = new IO::Handle();
	$self->{'out'} = new IO::Handle();
	$self->{'err'} = new IO::Handle();

	$self->{'pid'} = open3(
		$self->{'in'}, $self->{'out'}, $self->{'err'},
		$arg_hash{'psql_command_line'});

	my $exit_status = $? >> 8;

	$self->{'out'}->blocking(0);
	$self->{'err'}->blocking(0);

	$self->{'in'}->autoflush(1);
	$self->{'out'}->autoflush(1);
	$self->{'err'}->autoflush(1);

	if ($exit_status) {
		die(join(' ', ('DatabaseError Can not start psql: ',
					   $arg_hash{'psql_command_line'},
					   join('', $self->{'out'}->getlines()),
					   join('', $self->{'err'}->getlines()))));
	}

	return;
}

sub _send_to_psql {
	my ($self, %arg_hash) = @_;

	$self->{'in'}->print(
		$arg_hash{'command'}.';'."\n".'\echo pgcompactor_EOA'."\n");

	my $err_output = join('', $self->{'err'}->getlines());

	if ($err_output and $err_output =~ /ERROR:/) {
		die(join(' ', ('DatabaseError Can not execute the command: ',
						$arg_hash{'command'}, $err_output,
					   join('', $self->{'out'}->getlines()))));
	}

	my $result = '';
	while (1) {
		my $line = $self->{'out'}->getline();

		if (defined $line) {
			if ($line eq 'pgcompactor_EOA'."\n") {
				last;
			} else {
				$result .= $line;
			}
		} else {
			PgToolkit::Utils->sleep(0.001);
		}
	}

	return $result;
}

sub DESTROY {
	my $self = shift;

	if (defined $self->{'in'}) {
		close $self->{'in'};
	}

	return;
}

=head1 SEE ALSO

=over 4

=item L<IPC::Open3>

=item L<PgToolkit::Database>

=back

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2012, PostgreSQL-Consulting.com

=head1 AUTHOR

=over 4

=item L<Sergey Konoplev|mailto:sergey.konoplev@postgresql-consulting.com>

=back

=cut

1;
