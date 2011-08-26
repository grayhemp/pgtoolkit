# -*- mode: Perl; -*-
package PgToolkit::DatabaseChooserTest;

use parent qw(PgToolkit::Test);

use strict;
use warnings;

use Test::More;
use Test::Exception;

use PgToolkit::DatabaseChooser;

sub test_init : Test(2) {
	isa_ok(
		PgToolkit::DatabaseChooser->new(
			constructor_list => [
				sub { PgToolkit::DatabaseChooserTest::Stub->new(); },
				sub { PgToolkit::DatabaseChooserTest::ErrorStub->new(); }]
		),
		'PgToolkit::DatabaseChooserTest::Stub');

	isa_ok(
		PgToolkit::DatabaseChooser->new(
			constructor_list => [
				sub {
					PgToolkit::DatabaseChooserTest::AnotherErrorStub->new();
				},
				sub { PgToolkit::DatabaseChooserTest::AnotherStub->new(); }]
		),
		'PgToolkit::DatabaseChooserTest::AnotherStub');
}

sub test_nothing_has_been_created : Test {
	throws_ok(
		sub {
			PgToolkit::DatabaseChooser->new(
				constructor_list => [
					sub {
						PgToolkit::DatabaseChooserTest::AnotherErrorStub->new();
					},
					sub { PgToolkit::DatabaseChooserTest::ErrorStub->new(); }]
				);
		},
		qr/DatabaseChooserError/);
}

1;

package PgToolkit::DatabaseChooserTest::Stub;

use parent qw(PgToolkit::Class);

1;

package PgToolkit::DatabaseChooserTest::ErrorStub;

use parent qw(PgToolkit::Class);

sub init {
	die('DatabaseError Stub');
}

1;

package PgToolkit::DatabaseChooserTest::AnotherStub;

use parent qw(PgToolkit::Class);

1;

package PgToolkit::DatabaseChooserTest::AnotherErrorStub;

use parent qw(PgToolkit::Class);

sub init {
	die('DatabaseError Another stub');
}

1;
