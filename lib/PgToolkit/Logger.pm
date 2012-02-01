package PgToolkit::Logger;

use base qw(PgToolkit::Class);

use strict;
use warnings;

=head1 NAME

B<PgToolkit::Logger> - a logging facility class.

=head1 SYNOPSIS

	my $logger = PgToolkit::Logger->new(level => 'info');
	$logger->write(message => 'Some message', level => 'warning');

=head1 DESCRIPTION

B<PgToolkit::Logger> is a class implementing simple multilevel message
logging logic.

=head3 Constructor arguments

=over 4

=item C<out_handle>

an output filehandle, by default C<*STDOUT>

=item C<err_handle>

an error filehandle, by default C<*STDERR>, all the C<error> and
C<warning> messages are written to this filehandle

=item C<level>

a minimum logging level, allowed symbols are C<error>, C<warning>,
C<notice>, C<info>, C<debug0> and C<debug1>.

=back

=head3 Throws

=over 4

=item C<LoggerError>

when wrong logging level is specified.

=back

=cut

sub init {
	my ($self, %arg_hash) = @_;

	binmode(*STDOUT, ':encoding(UTF-8)');
	binmode(*STDERR, ':encoding(UTF-8)');

	$self->{'_out_handle'} =
		exists $arg_hash{'out_handle'} ? $arg_hash{'out_handle'} : \*STDOUT;
	$self->{'_err_handle'} =
		exists $arg_hash{'err_handle'} ? $arg_hash{'err_handle'} : \*STDERR;
	$self->{'_level_code'} = $self->_get_level_code(
		level => $arg_hash{'level'});

	# Setting autoflush
	{
		my $default = select();
		select($self->{'_out_handle'});
		$| = 1;
		select($self->{'_err_handle'});
		$| = 1;
		select($default);
	}

	if (not defined $self->{'_level_code'}) {
		die('LoggerError Wrong logging level "'.$arg_hash{'level'}.
			'" is specified in initialization.');
	}

	return;
}

=head1 METHODS

=head2 B<write()>

Loggs a message.

=head3 Arguments

=over 4

=item C<message>

=item C<level>

the level of the message, allowed symbols are C<error>, C<warning>,
C<notice>, C<info>, C<debug0> and C<debug1>

=item C<target>

an name related to the log entry, empty by default.

=back

=head3 Throws

=over 4

=item C<LoggerError>

when wrong logging level is specified.

=back

=cut

sub write {
	my ($self, %arg_hash) = @_;

	my $level_code = $self->_get_level_code(level => $arg_hash{'level'});

	if (not defined $level_code) {
		die('LoggerError Wrong logging level "'.$arg_hash{'level'}.
			'" is specified in write.');
	}

	if ($level_code <= $self->{'_level_code'}) {
		print(
			{$level_code > 0 ? $self->{'_out_handle'} : $self->{'_err_handle'}}
			scalar(localtime()).' '.
			(defined $arg_hash{'target'} ? $arg_hash{'target'}.' ' : '').
			uc($arg_hash{'level'}).' '.
			$arg_hash{'message'}."\n");
	}

	return;
}

sub _get_level_code {
	my ($self, %arg_hash) = @_;

	my $level = {
		'error' => -1,
		'warning' => 0,
		'notice' => 1,
		'info' => 2,
		'debug0' => 3,
		'debug1' => 4
	}->{$arg_hash{'level'}};

	return $level;
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
