# -*- mode: Perl; -*-
package PgToolkit::UtilsTest;

use base qw(PgToolkit::Test);

use strict;
use warnings;

use Test::More;
use Test::Exception;

use PgToolkit::Utils;

sub setup : Test(setup) {
	my $self = shift;

	$self->{'utils'} = PgToolkit::Utils->new();;
}

sub test_get_size_pretty : Test(12) {
	my $self = shift;

	is($self->{'utils'}->get_size_pretty(size => 0), '0 bytes');
	is($self->{'utils'}->get_size_pretty(size => 1), '1 bytes');

	is($self->{'utils'}->get_size_pretty(
		   size => 10 * 1024 - 1), '10239 bytes');
	is($self->{'utils'}->get_size_pretty(
		   size => 10 * 1024), '10 kB');

	is($self->{'utils'}->get_size_pretty(
		   size => 10 * 1024 ** 2 - 512 - 1), '10239 kB');
	is($self->{'utils'}->get_size_pretty(
		   size => 10 * 1024 ** 2 - 512), '10 MB');

	is($self->{'utils'}->get_size_pretty(
		   size => 10 * 1024 ** 3 - 512 * 1024 - 1), '10239 MB');
	is($self->{'utils'}->get_size_pretty(
		   size => 10 * 1024 ** 3 - 512 * 1024), '10 GB');

	is($self->{'utils'}->get_size_pretty(
		   size => 10 * 1024 ** 4 - 512 * 1024 ** 2 - 1), '10239 GB');
	is($self->{'utils'}->get_size_pretty(
		   size => 10 * 1024 ** 4 - 512 * 1024 ** 2), '10 TB');

	is($self->{'utils'}->get_size_pretty(
		   size => 10 * 1024 ** 5), '10240 TB');

	is($self->{'utils'}->get_size_pretty(
		   size => -10 * 1024 ** 2 + 512), '-10 MB');
}

1;
