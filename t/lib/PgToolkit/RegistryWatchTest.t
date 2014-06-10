# -*- mode: Perl; -*-
package PgToolkit::RegistryWatchTest;

use base qw(PgToolkit::RegistryTest);

use strict;
use warnings;

use Test::More;

use PgToolkit::Registry::Watch;

sub setup : Test(setup) {
	my $self = shift;

	push(@ARGV, '-a');
	$self->{'registry'} = PgToolkit::Registry::Watch->new();
}

sub test_get_options : Test(2) {
	my $self = shift;

	$self->is_lazy(
		code_reference => sub {
			return $self->{'registry'}->get_options();
		},
		class_name => 'PgToolkit::Options');
}

sub test_get_logger : Test(2) {
	my $self = shift;

	$self->is_lazy(
		code_reference => sub { return $self->{'registry'}->get_logger(); },
		class_name => 'PgToolkit::Logger');
}

sub test_get_database_adapter : Test(2) {
	my $self = shift;

	$self->is_prototype(
		code_reference => sub {
			return $self->{'registry'}->get_database_adapter(
				dbname => 'postgres');
		},
		class_name => 'PgToolkit::Database');
}

1;
