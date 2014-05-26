# -*- mode: Perl; -*-
package PgToolkit::RegistryCompactTest;

use base qw(PgToolkit::RegistryTest);

use strict;
use warnings;

use PgToolkit::DbiStub;
use PgToolkit::DatabaseStub;

use PgToolkit::Registry::Compact;

sub setup : Test(setup) {
	my $self = shift;

	push(@ARGV, '-a');
	$self->{'registry'} = PgToolkit::Registry::Compact->new();
	$self->{'database'} = PgToolkit::DatabaseStub->new(dbname => 'dbname');
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

	$self->is_normal(
		code_reference => sub {
			return $self->{'registry'}->get_database_adapter(
				dbname => 'postgres');
		},
		class_name => 'PgToolkit::Database');
}

sub test_get_table_compact : Test(2) {
	my $self = shift;

	$self->is_normal(
		code_reference => sub {
			return $self->{'registry'}->get_table_compact(
				database => $self->{'database'},
				schema_name => 'schema',
				table_name => 'table',
				use_pgstattuple => 0);
		},
		class_name => 'PgToolkit::Compact::Table');
}

sub test_get_database_compact : Test(2) {
	my $self = shift;

	$self->is_normal(
		code_reference => sub {
			return $self->{'registry'}->get_database_compact(
				database => $self->{'database'});
		},
		class_name => 'PgToolkit::Compact::Database');
}

sub test_get_cluster_compact : Test(2) {
	my $self = shift;

	$self->is_normal(
		code_reference => sub {
			return $self->{'registry'}->get_cluster_compact();
		},
		class_name => 'PgToolkit::Compact::Cluster');
}

1;
