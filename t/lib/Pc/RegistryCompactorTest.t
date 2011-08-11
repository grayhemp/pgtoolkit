# -*- mode: Perl; -*-
package Pc::RegistryCompactorTest;

use parent qw(Pc::Test);

use strict;
use warnings;

use Test::More;

use Pc::DbiStub;
use Pc::DatabaseStub;

use Pc::Registry::Compactor;

sub is_lazy {
	my (undef, %arg_hash) = @_;

	isa_ok($arg_hash{'code_reference'}->(), $arg_hash{'class_name'});
	is($arg_hash{'code_reference'}->(), $arg_hash{'code_reference'}->());

	return;
}

sub is_prototype {
	my (undef, %arg_hash) = @_;

	isa_ok($arg_hash{'code_reference'}->(), $arg_hash{'class_name'});
	isnt($arg_hash{'code_reference'}->(), $arg_hash{'code_reference'}->());

	return;
}

sub setup : Test(setup) {
	my $self = shift;

	$self->{'registry'} = Pc::Registry::Compactor->new();
	$self->{'database'} = Pc::DatabaseStub->new(dbname => 'dbname');
}

sub test_get_options : Test(2) {
	my $self = shift;

	$self->is_lazy(
		code_reference => sub {
			return $self->{'registry'}->get_options();
		},
		class_name => 'Pc::Options');
}

sub test_get_logger : Test(2) {
	my $self = shift;

	$self->is_lazy(
		code_reference => sub { return $self->{'registry'}->get_logger(); },
		class_name => 'Pc::Logger');
}

sub test_get_database_adapter : Test(2) {
	my $self = shift;

	$self->is_prototype(
		code_reference => sub {
			return $self->{'registry'}->get_database_adapter(
				dbname => 'postgres');
		},
		class_name => 'Pc::Database');
}

sub test_get_table_compactor : Test(2) {
	my $self = shift;

	$self->is_prototype(
		code_reference => sub {
			return $self->{'registry'}->get_table_compactor(
				database => $self->{'database'},
				schema_name => 'schema',
				table_name => 'table',
				use_pgstattuple => 0);
		},
		class_name => 'Pc::Compactor::Table');
}

sub test_get_schema_compactor : Test(2) {
	my $self = shift;

	$self->is_prototype(
		code_reference => sub {
			return $self->{'registry'}->get_schema_compactor(
				database => $self->{'database'},
				schema_name => 'schema',
				use_pgstattuple => 0);
		},
		class_name => 'Pc::Compactor::Schema');
}

sub test_get_database_compactor : Test(2) {
	my $self = shift;

	$self->is_prototype(
		code_reference => sub {
			return $self->{'registry'}->get_database_compactor(
				database => $self->{'database'});
		},
		class_name => 'Pc::Compactor::Database');
}

sub test_get_cluster_compactor : Test(2) {
	my $self = shift;

	$self->is_prototype(
		code_reference => sub {
			return $self->{'registry'}->get_cluster_compactor();
		},
		class_name => 'Pc::Compactor::Cluster');
}

1;
