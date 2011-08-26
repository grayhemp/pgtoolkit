# -*- mode: Perl; -*-
package PgToolkit::LoggerTest;

use parent qw(PgToolkit::Test);

use strict;
use warnings;

use Test::More;
use Test::Exception;

use PgToolkit::Logger;

sub test_init_wrong_level : Test {
	throws_ok(
		sub { PgToolkit::Logger->new(level => 'wrong'); },
		qr/LoggerError Wrong logging level "wrong" is specified/);
}

sub test_write : Test(6) {
	my ($out, $err) = ('', '');
	open(my $out_handle, '+<', \ $out);
	open(my $err_handle, '+<', \ $err);

	PgToolkit::Logger->new(
		level => 'info',
		out_handle => $out_handle,
		err_handle => $err_handle)
		->write(message => 'info message', level => 'info');

	like($out, qr/INFO info message\n/);
	is($err, '');

	PgToolkit::Logger->new(
		level => 'warning',
		out_handle => $out_handle,
		err_handle => $err_handle)
		->write(message => 'notice message', level => 'notice');

	unlike($out, qr/NOTICE notice message\n/);
	is($err, '');

	PgToolkit::Logger->new(
		level => 'warning',
		out_handle => $out_handle,
		err_handle => $err_handle)
		->write(message => 'error message', level => 'error');

	like($out, qr/INFO info message\n/);
	like($err, qr/ERROR error message\n/);
}

sub test_write_wrong_level : Test {
	throws_ok(
		sub {
			PgToolkit::Logger->new(level => 'info')
				->write(message => 'message', level => 'wrong');
		},
		qr/LoggerError Wrong logging level "wrong" is specified in write\./);
}

1;
