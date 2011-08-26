# -*- mode: Perl; -*-
package PgToolkit::DatabaseDbiNoDbiTest;

use parent qw(PgToolkit::Test);

use strict;
use warnings;

use Test::More;
use Test::Exception;

use PgToolkit::Database::Dbi;

sub test_no_dbi : Test {
	throws_ok(
		sub {
			local @INC;
			PgToolkit::Database::Dbi->new(
				driver => 'pg', host => 'host', port => '5432',
				dbname => 'db', user => 'user', password => 'password');
		},
		qr/DatabaseError DBI module not found\./);
}

1;
