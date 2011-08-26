# -*- mode: Perl; -*-
package PgToolkit::ClassTest;

use parent qw(PgToolkit::Test);

use strict;
use warnings;

use Test::More;

use PgToolkit::Class;

sub test_bless : Test(2) {
	my $dummy = PgToolkit::ClassTest::Dummy->new();
	my $another_dummy = $dummy->new();

	isa_ok($dummy, 'PgToolkit::Class');
	isnt($another_dummy, $dummy);
}

sub test_init : Test(2) {
	for my $v (1..2) {
		my $dummy = PgToolkit::ClassTest::Dummy->new(v => 1);
		is($dummy->get_v(), 1);
	}
}

1;

package PgToolkit::ClassTest::Dummy;

use parent qw(PgToolkit::Class);

sub init {
	my ($self, %args) = @_;

	$self->{'v'} = $args{'v'};
}

sub get_v {
	my $self = shift;

	return $self->{'v'};
}

1;
