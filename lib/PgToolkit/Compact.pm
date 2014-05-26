package PgToolkit::Compact;

use base qw(PgToolkit::Class);

use strict;
use warnings;

=head1 NAME

B<PgToolkit::Compact> - base compact.

=head1 SYNOPSIS

	package PgToolkit::CompactStub;

	use base qw(PgToolkit::Compact);

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

B<PgToolkit::Compact> is a base class for boald reducing mechanisms
implementation. You can implement _init() and must _process(). The
_logger property is defined in this methods. If you want log entries
to have target define the _log_target property.

=head3 Constructor arguments

=over 4

=item C<logger>

a logger object

=item C<dry_run>

=back

=cut

sub init {
	my ($self, %arg_hash) = @_;

	$self->{'_logger'} = $arg_hash{'logger'};
	$self->{'_dry_run'} = $arg_hash{'dry_run'};

	$self->_wrap(code => sub { $self->_init(%arg_hash); });

	return;
}

=head1 METHODS

=head2 B<process()>

Runs a bloat reducing process.

=head3 Arguments

=over 4

=item C<attempt>

an attempt number of processing.

=back

=cut

sub process {
	my ($self, %arg_hash) = @_;

	$self->_wrap(code => sub { $self->_process(%arg_hash); });

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

Copyright (c) 2011-2014 Sergey Konoplev

PgToolkit is released under the PostgreSQL License, read COPYRIGHT.md
for additional information.

=head1 AUTHOR

=over 4

=item L<Sergey Konoplev|mailto:gray.ru@gmail.com>

=back

=cut

1;
