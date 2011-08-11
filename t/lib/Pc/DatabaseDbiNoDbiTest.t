# -*- mode: Perl; -*-
package Pc::DatabaseDbiNoDbiTest;

use parent qw(Pc::Test);

use strict;
use warnings;

use Test::More;
use Test::Exception;

use Pc::Database::Dbi;

sub test_no_dbi : Test {
	throws_ok(
		sub {
			local @INC;
			Pc::Database::Dbi->new(
				driver => 'pg', host => 'host', port => '5432',
				dbname => 'db', user => 'user', password => 'password');
		},
		qr/DatabaseError DBI module not found\./);
}

1;
