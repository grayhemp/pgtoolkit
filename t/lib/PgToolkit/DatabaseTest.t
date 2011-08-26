# -*- mode: Perl; -*-
package PgToolkit::DatabaseTest;

use parent qw(PgToolkit::Test);

use strict;
use warnings;

use Test::More;
use Test::Exception;

use PgToolkit::Database;

sub test_init : Test(2) {
	is(PgToolkit::DatabaseTest::Database->new(dbname => 'somedb')->get_dbname(),
	   'somedb');
	is(
		PgToolkit::DatabaseTest::Database->new(
			dbname => 'anotherdb')->get_dbname(),
	   'anotherdb');
}

sub test_quote_ident : Test(2) {
	my $db = PgToolkit::DatabaseTest::Database->new(dbname => 'somedb');

	is($db->quote_ident(string => 'some_ident'), '"some_ident"');
	is($db->quote_ident(string => 'some"ident'), '"some""ident"');
}

sub test_escaped_dbname : Test(2) {
	is(PgToolkit::DatabaseTest::Database->new(dbname => 'some db')->
	   get_escaped_dbname(),
	   'some\ db');
	is(PgToolkit::DatabaseTest::Database->new(dbname => 'another&db')->
	   get_escaped_dbname(),
	   'another\&db');
}

1;

package PgToolkit::DatabaseTest::Database;

use parent qw(PgToolkit::Database);

sub get_escaped_dbname {
	return shift->_get_escaped_dbname();
}

1;
