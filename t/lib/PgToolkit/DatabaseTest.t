# -*- mode: Perl; -*-
package PgToolkit::DatabaseTest;

use base qw(PgToolkit::Test);

use strict;
use warnings;

use Test::More;
use Test::Exception;

use PgToolkit::DatabaseStub;

sub setup : Test(setup) {
	my $self = shift;

	$self->{'database_constructor'} = sub {
		return PgToolkit::DatabaseTest::DatabaseStub->new(
			dbname => 'somedb',
			@_);
	};
}

sub test_init : Test(2) {
	my $self = shift;

	is($self->{'database_constructor'}->()->get_dbname(), 'somedb');
	is(
		$self->{'database_constructor'}->(dbname => 'anotherdb')->get_dbname(),
		'anotherdb');
}

sub test_quote_ident_nothing_to_ident : Test(2) {
	my $self = shift;

	throws_ok(
		sub { $self->{'database_constructor'}->()->quote_ident(string => ''); },
		qr/DatabaseError Nothing to ident\./);
	throws_ok(
		sub {
			$self->{'database_constructor'}->()->quote_ident(
				string => undef);
		},
		qr/DatabaseError Nothing to ident\./);
}

sub test_escaped_dbname : Test(2) {
	my $self = shift;

	is(
		$self->{'database_constructor'}->(dbname => 'some db')
		->get_escaped_dbname(),
		'some\ db');
	is(
		$self->{'database_constructor'}->(dbname => 'another&db')
		->get_escaped_dbname(),
		'another\&db');
}

1;

package PgToolkit::DatabaseTest::DatabaseStub;

use base qw(PgToolkit::DatabaseStub);

sub get_escaped_dbname {
	return shift->_get_escaped_dbname();
}

1;
