# -*- mode: Perl; -*-
package PgToolkit::LoggerTest;

use parent qw(PgToolkit::Test);

use strict;
use warnings;

use Test::More;
use Test::Exception;

use PgToolkit::Logger;

sub setup : Test(setup) {
	my $self = shift;

	$self->{'out'} = '';
	$self->{'err'} = '';
	open($self->{'out_handle'}, '+<', \ $self->{'out'});
	open($self->{'err_handle'}, '+<', \ $self->{'err'});
}

sub test_write_info_info : Test(2) {
	my $self = shift;

	PgToolkit::Logger->new(
		level => 'info',
		out_handle => $self->{'out_handle'},
		err_handle => $self->{'err_handle'})
		->write(message => 'info message', level => 'info');

	like($self->{'out'}, qr/INFO info message\n/);
	is($self->{'err'}, '');
}

sub test_write_warning_notice : Test(2) {
	my $self = shift;

	PgToolkit::Logger->new(
		level => 'warning',
		out_handle => $self->{'out_handle'},
		err_handle => $self->{'err_handle'})
		->write(message => 'notice message', level => 'notice');

	unlike($self->{'out'}, qr/NOTICE notice message\n/);
	is($self->{'err'}, '');
}

sub test_write_warning_error : Test(2) {
	my $self = shift;

	PgToolkit::Logger->new(
		level => 'warning',
		out_handle => $self->{'out_handle'},
		err_handle => $self->{'err_handle'})
		->write(message => 'error message', level => 'error');

	is($self->{'out'}, '');
	like($self->{'err'}, qr/ERROR error message\n/);
}

sub test_init_wrong_level : Test {
	throws_ok(
		sub { PgToolkit::Logger->new(level => 'wrong'); },
		qr/LoggerError Wrong logging level "wrong" is specified/);
}

sub test_write_wrong_level : Test {
	throws_ok(
		sub {
			PgToolkit::Logger->new(level => 'info')
				->write(message => 'message', level => 'wrong');
		},
		qr/LoggerError Wrong logging level "wrong" is specified in write\./);
}

sub test_write_with_target : Test {
	my $self = shift;

	PgToolkit::Logger->new(
		level => 'info',
		out_handle => $self->{'out_handle'},
		err_handle => $self->{'err_handle'})->
		write(
			message => 'message',
			level => 'info',
			target => 'target');

	like($self->{'out'}, qr/INFO target message\n/);
}

1;
