# -*- mode: Perl; -*-
package Pc::ClassTest;

use parent qw(Pc::Test);

use strict;
use warnings;

use Test::More;

use Pc::Class;

sub test_bless : Test(2) {
	my $dummy = Pc::ClassTest::Dummy->new();
	my $another_dummy = $dummy->new();

	isa_ok($dummy, 'Pc::Class');
	isnt($another_dummy, $dummy);
}

sub test_init : Test(2) {
	for my $v (1..2) {
		my $dummy = Pc::ClassTest::Dummy->new(v => 1);
		is($dummy->get_v(), 1);
	}
}

1;

package Pc::ClassTest::Dummy;

use parent qw(Pc::Class);

sub init {
	my ($self, %args) = @_;

	$self->{'v'} = $args{'v'};
}

sub get_v {
	my $self = shift;

	return $self->{'v'};
}

1;
