# -*- mode: Perl; -*-
package PgToolkit::CompactorTest;

use base qw(PgToolkit::Test);

use strict;
use warnings;

use Test::MockObject;
use Test::More;

use Test::Exception;

use PgToolkit::Logger;

sub setup : Test(setup) {
	my $self = shift;

	$self->{'mock'} = Test::MockObject->new();
	$self->{'mock'}->mock(
		'-is_called',
		sub {
			my ($self, $pos, $name, @arg_list) = @_;

			is($self->call_pos($pos), $name);
			is_deeply([$self->call_args($pos)], [$self, @arg_list]);
		});
	$self->{'mock'}->set_true('exit');

	$self->{'compactor_constructor'} = sub {
		return PgToolkit::CompactorStub->new(
			logger => PgToolkit::Logger->new(
				level => 'info', err_handle => \*STDOUT),
			mock => $self->{'mock'},
			die_on_init => undef,
			@_);
	}
}

sub test_init_catches_database_error_and_exits : Test(2) {
	my $self = shift;

	$self->{'compactor_constructor'}->(die_init_message => 'DatabaseError');

	$self->{'mock'}->is_called(1, 'exit');
}

sub test_process_catches_database_error_and_exits : Test(2) {
	my $self = shift;

	$self->{'compactor_constructor'}->
		(die_process_message => 'DatabaseError')->process();

	$self->{'mock'}->is_called(1, 'exit');
}

sub test_init_dies_on_other_error : Test {
	my $self = shift;

	throws_ok(
		sub {
			$self->{'compactor_constructor'}->(die_init_message => 'SomeError');
		},
		qr/SomeError/);
}

sub test_process_dies_on_other_error : Test {
	my $self = shift;

	throws_ok(
		sub {
			$self->{'compactor_constructor'}->
				(die_process_message => 'SomeError')->process();
		},
		qr/SomeError/);
}

1;

package PgToolkit::CompactorStub;

use base qw(PgToolkit::Compactor);

use strict;
use warnings;

sub _init {
	my ($self, %arg_hash) = @_;

	$self->{'mock'} = $arg_hash{'mock'};
	$self->{'_log_target'} = 'some_target';
	$self->{'_die_process_message'} = $arg_hash{'die_process_message'};

	if (defined $arg_hash{'die_init_message'}) {
		die($arg_hash{'die_init_message'});
	}
}

sub _process {
	die(shift->{'_die_process_message'});
}

sub _exit {
	shift->{'mock'}->exit();
}

1;
