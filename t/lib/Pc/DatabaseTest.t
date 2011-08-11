# -*- mode: Perl; -*-
package Pc::DatabaseTest;

use parent qw(Pc::Test);

use strict;
use warnings;

use Test::More;
use Test::Exception;

use Pc::Database;

sub test_init : Test(2) {
	is(Pc::DatabaseTest::Database->new(dbname => 'somedb')->get_dbname(),
	   'somedb');
	is(Pc::DatabaseTest::Database->new(dbname => 'anotherdb')->get_dbname(),
	   'anotherdb');
}

sub test_quote_ident : Test(2) {
	my $db = Pc::DatabaseTest::Database->new(dbname => 'somedb');

	is($db->quote_ident(string => 'some_ident'), '"some_ident"');
	is($db->quote_ident(string => 'some"ident'), '"some""ident"');
}

sub test_escaped_dbname : Test(2) {
	is(Pc::DatabaseTest::Database->new(dbname => 'some db')->
	   get_escaped_dbname(),
	   'some\ db');
	is(Pc::DatabaseTest::Database->new(dbname => 'another&db')->
	   get_escaped_dbname(),
	   'another\&db');
}

1;

package Pc::DatabaseTest::Database;

use parent qw(Pc::Database);

sub get_escaped_dbname {
	return shift->_get_escaped_dbname();
}

1;
