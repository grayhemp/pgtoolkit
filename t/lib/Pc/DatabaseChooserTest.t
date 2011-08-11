# -*- mode: Perl; -*-
package Pc::DatabaseChooserTest;

use parent qw(Pc::Test);

use strict;
use warnings;

use Test::More;
use Test::Exception;

use Pc::DatabaseChooser;

sub test_init : Test(2) {
	isa_ok(
		Pc::DatabaseChooser->new(
			constructor_list => [
				sub { Pc::DatabaseChooserTest::Stub->new(); },
				sub { Pc::DatabaseChooserTest::ErrorStub->new(); }]
		),
		'Pc::DatabaseChooserTest::Stub');

	isa_ok(
		Pc::DatabaseChooser->new(
			constructor_list => [
				sub { Pc::DatabaseChooserTest::AnotherErrorStub->new(); },
				sub { Pc::DatabaseChooserTest::AnotherStub->new(); }]
		),
		'Pc::DatabaseChooserTest::AnotherStub');
}

sub test_nothing_has_been_created : Test {
	throws_ok(
		sub {
			Pc::DatabaseChooser->new(
				constructor_list => [
					sub { Pc::DatabaseChooserTest::AnotherErrorStub->new(); },
					sub { Pc::DatabaseChooserTest::ErrorStub->new(); }]
				);
		},
		qr/DatabaseChooserError/);
}

1;

package Pc::DatabaseChooserTest::Stub;

use parent qw(Pc::Class);

1;

package Pc::DatabaseChooserTest::ErrorStub;

use parent qw(Pc::Class);

sub init {
	die('DatabaseError Stub');
}

1;

package Pc::DatabaseChooserTest::AnotherStub;

use parent qw(Pc::Class);

1;

package Pc::DatabaseChooserTest::AnotherErrorStub;

use parent qw(Pc::Class);

sub init {
	die('DatabaseError Another stub');
}

1;
