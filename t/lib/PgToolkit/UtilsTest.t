# -*- mode: Perl; -*-
package PgToolkit::UtilsTest;

use base qw(PgToolkit::Test);

use strict;
use warnings;

use Test::More;

use PgToolkit::Utils;

sub setup : Test(setup) {
	my $self = shift;

	$self->{'utils'} = PgToolkit::Utils->new();
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

sub test_get_pgpass_password : Test(8) {
	my $self = shift;

	my $pgpassfile = <<EOF;
somehost:*:*:*:pgpasssomepassword
*:1234:*:*:pgpassportpassword
*:*:db:me:pgpassdbmepassword
*:*:db:*:pgpassdbpassword
*:*:*:me:pgpassmepassword
#anotherhost:*:*:*:pgpasscommentedpassword
anotherhost:*:*:*:pgpassanotherpassword
*:*:*:*:pgpassyetanotherpassword
EOF

	my $home = <<EOF;
somehost:*:*:*:homesomepassword
#anotherhost:*:*:*:homecommentedpassword
anotherhost:*:*:*:homeanotherpassword
*:*:*:*:homeyetanotherpassword
EOF

	open(my $pgpassfile_handle, '<', \ $pgpassfile);
	open(my $home_handle, '<', \ $home);

	is(
		$self->{'utils'}->get_pgpass_password(),
		undef);

	is(
		$self->{'utils'}->get_pgpass_password(
			pgpassfile => \ $pgpassfile,
			home => \ $home),
		'pgpassyetanotherpassword');

	is(
		$self->{'utils'}->get_pgpass_password(
			home => \ $home),
		'homeyetanotherpassword');

	is(
		$self->{'utils'}->get_pgpass_password(
			host => 'somehost',
			pgpassfile => \ $pgpassfile),
		'pgpasssomepassword');

	is(
		$self->{'utils'}->get_pgpass_password(
			port => '1234',
			pgpassfile => \ $pgpassfile),
		'pgpassportpassword');

	is(
		$self->{'utils'}->get_pgpass_password(
			dbname => 'db',
			pgpassfile => \ $pgpassfile),
		'pgpassdbpassword');

	is(
		$self->{'utils'}->get_pgpass_password(
			user => 'me',
			pgpassfile => \ $pgpassfile),
		'pgpassmepassword');

	is(
		$self->{'utils'}->get_pgpass_password(
			dbname => 'db',
			user => 'me',
			pgpassfile => \ $pgpassfile),
		'pgpassdbmepassword');
}

sub test_cmp_versions : Test(3) {
	my $self = shift;

	is(
		$self->{'utils'}->cmp_versions(
			v1 => '1.0.0',
			v2 => '1.0.0'),
		0);

	is(
		$self->{'utils'}->cmp_versions(
			v1 => '1.1.0',
			v2 => '1.0.0'),
		1);

	is(
		$self->{'utils'}->cmp_versions(
			v1 => '1.0.0',
			v2 => '1.1.0'),
		-1);
}

1;
