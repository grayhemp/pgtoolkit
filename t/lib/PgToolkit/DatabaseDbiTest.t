# -*- mode: Perl; -*-
package PgToolkit::DatabaseDbiTest;

use base qw(PgToolkit::Test);

use strict;
use warnings;

use Test::More;
use Test::Exception;

use PgToolkit::DbiStub;

use PgToolkit::Database::Dbi;

sub setup : Test(setup) {
	my $self = shift;

	$self->{'database_constructor'} = sub {
		return PgToolkit::Database::Dbi->new(
			driver => 'somepg', port => '5432', host => 'somehost',
			dbname => 'somedb', user => 'someuser', password => 'somepassword',
			@_);
	}
}

sub test_init : Test(6) {
	my $self = shift;

	my $data_hash_list = [
		{'database_arg_hash' => {'host' => undef},
		 'dbh_arg_list' => [
			 'dbi:somepg:dbname=somedb;port=5432',
			 'someuser', 'somepassword'],
		 'dbname' => 'somedb'},
		{'database_arg_hash' => {
			'driver' => 'anotherpg', 'host' => 'anotherhost', 'port' => '6432',
			'dbname' => 'anotherdb', 'user' => 'anotheruser',
			'password' => 'anotherpassword'},
		 'dbh_arg_list' => [
			 'dbi:anotherpg:dbname=anotherdb;host=anotherhost;port=6432',
			 'anotheruser', 'anotherpassword'],
		 'dbname' => 'anotherdb'}];

	for my $data_hash (@{$data_hash_list}) {
		my $db = $self->{'database_constructor'}->(
			%{$data_hash->{'database_arg_hash'}});

		ok($db->{'dbh'}->call_pos(-1), 'connect');
		is_deeply(
			[$db->{'dbh'}->call_args(-1)],
			[$db->{'dbh'}, 'DBI', @{$data_hash->{'dbh_arg_list'}},
			 {
				 RaiseError => 1, ShowErrorStatement => 1, AutoCommit => 1,
				 PrintWarn => 0, PrintError => 0,
				 pg_server_prepare => 0, pg_enable_utf8 => 1
			 }]);
		is($db->get_dbname(), $data_hash->{'dbname'});
	}
}

sub test_no_driver : Test {
	my $self = shift;

	throws_ok(
		sub {
			local @INC;
			$self->{'database_constructor'}->(
				driver => 'wrongpg', host => 'host', port => '5432',
				dbname => 'db', user => 'user', password => 'password');
		},
		qr/DatabaseError No driver found "wrongpg"\./);
}

sub test_execute : Test(12) {
	my $self = shift;

	my $db = $self->{'database_constructor'}->();

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

sub test_adapter_name : Test {
	my $self = shift;

	is($self->{'database_constructor'}->()->get_adapter_name(), 'DBI/somepg');
}

1;
