# -*- mode: Perl; -*-
package Pc::DatabaseDbiTest;

use parent qw(Pc::Test);

use strict;
use warnings;

use Test::More;
use Test::Exception;

use Pc::DbiStub;

use Pc::Database::Dbi;

sub test_init : Test(8) {
	my $db = Pc::Database::Dbi->new(
		driver => 'somepg', host => 'somehost', port => '5432',
		dbname => 'somedb', user => 'someuser', password => 'somepassword');

	isa_ok($db->{'dbh'}, 'DBI');

	ok($db->{'dbh'}->call_pos(-1), 'connect');
	is_deeply(
		[$db->{'dbh'}->call_args(-1)],
		[$db->{'dbh'}, 'DBI',
		 'dbi:somepg:dbname=somedb;host=somehost;port=5432',
		 'someuser', 'somepassword',
		 {
			 RaiseError => 1, ShowErrorStatement => 1, AutoCommit => 1,
			 PrintWarn => 0, PrintError => 0,
			 pg_server_prepare => 1, pg_enable_utf8 => 1
		 }]);
	is($db->get_dbname(), 'somedb');

	$db = Pc::Database::Dbi->new(
		driver => 'anotherpg', host => 'anotherhost', port => '6432',
		dbname => 'anotherdb', user => 'anotheruser',
		password => 'anotherpassword');

	isa_ok($db->{'dbh'}, 'DBI');

	ok($db->{'dbh'}->call_pos(-1), 'connect');
	is_deeply(
		[$db->{'dbh'}->call_args(-1)],
		[$db->{'dbh'}, 'DBI',
		 'dbi:anotherpg:dbname=anotherdb;host=anotherhost;port=6432',
		 'anotheruser', 'anotherpassword',
		 {
			 RaiseError => 1, ShowErrorStatement => 1, AutoCommit => 1,
			 PrintWarn => 0, PrintError => 0,
			 pg_server_prepare => 1, pg_enable_utf8 => 1
		 }]);
	is($db->get_dbname(), 'anotherdb');
}

sub test_no_driver : Test {
	throws_ok(
		sub {
			local @INC;
			Pc::Database::Dbi->new(
				driver => 'wrongpg', host => 'host', port => '5432',
				dbname => 'db', user => 'user', password => 'password');
		},
		qr/DatabaseError No driver found "wrongpg"\./);
}

sub test_execute : Test(12) {
	my $self = shift;

	my $db = Pc::Database::Dbi->new(
		driver => 'somepg', host => 'somehost', port => '5432',
		dbname => 'somedb', user => 'someuser', password => 'somepassword');

	my $data_hash = {
		'SELECT 1 WHERE false;' => [],
		'SELECT 1;' => [[1]],
		'SELECT 1, \'text\';' => [[1, 'text']],
		'SELECT column1, column2 '.
		'FROM (VALUES (1, \'text1\'), (2, \'text2\'))_;' => [
			[1, 'text1'], [2, 'text2']]
	};

	for my $sql (keys %{$data_hash}) {
		is_deeply($db->execute(sql => $sql), $data_hash->{$sql});

		is($db->{'sth'}->call_pos(1), 'execute');
		is_deeply([$db->{'sth'}->call_args(1)], [$db->{'sth'}]);
	}
}

1;
