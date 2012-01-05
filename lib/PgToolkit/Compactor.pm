package PgToolkit::Compactor;

use base qw(PgToolkit::Class);

use strict;
use warnings;

=head1 NAME

B<PgToolkit::Compactor> - a base compactor.

=head1 SYNOPSIS

	package PgToolkit::CompactorStub;

	use base qw(PgToolkit::Compactor);

	sub _init {
		my $self = shift;

		$self->{'_log_target'} = 'some_target';
		# some other initialization
	}

	sub _process {
		# some process implementation
	}

	1;

=head1 DESCRIPTION

B<PgToolkit::Compactor> is a base class for boald reducing mechanisms
implementation. You can implement _init() and must _process(). The
_logger property is defined in this methods. If you want log entries
to have target define the _log_target property.

=head3 Constructor arguments

=over 4

=item C<logger>

a logger object.

=back

=cut

sub init {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'} = $arg_hash{'logger'};

	$self->_wrap(code => sub { $self->_init(%arg_hash); });

	return;
}

=head1 METHODS

=head2 B<process()>

Runs a bloat reducing process.

=cut

sub process {
	my $self = shift;

	$self->_wrap(code => sub { $self->_process(); });

	return;
}

sub _init {
	# initialization stub
}

sub _process {
	die('NotImplementedError');
}

sub _exit {
	exit(1);
}

sub _wrap {
	my ($self, %arg_hash) = @_;

	eval {
		$arg_hash{'code'}->();
	};
	if ($@) {
		if ($@ =~ 'DatabaseError') {
			$self->{'_logger'}->write(
				message => 'A database error occurred, exiting:'."\n".$@,
				level => 'error',
				(defined $self->{'_log_target'} ?
				 (target => $self->{'_log_target'}) : ()));
			$self->_exit();
		} else {
			die($@);
		}
	}

	return;
}

sub _execute_and_log {
	my ($self, %arg_hash) = @_;

	my $result = $self->{'_database'}->execute(sql => $arg_hash{'sql'});

	my $duration = sprintf("%.3f", $self->{'_database'}->get_duration());

	$self->{'_logger'}->write(
		message => ('Executed SQL: duration '.$duration.'s, statement: '.
					"\n".$arg_hash{'sql'}),
		level => (defined $arg_hash{'level'} ? $arg_hash{'level'} : 'debug0'),
		target => $self->{'_log_target'});

	return $result;
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
