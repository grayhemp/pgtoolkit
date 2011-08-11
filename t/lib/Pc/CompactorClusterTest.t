# -*- mode: Perl; -*-
package Pc::CompactorClusterTest;

use parent qw(Pc::Test);

use strict;
use warnings;

use Test::MockObject;
use Test::More;

use Test::Exception;

use Pc::DatabaseStub;

use Pc::Logger;
use Pc::Compactor::Cluster;

sub setup : Test(setup) {
	my $self = shift;

	$self->{'database'} = Pc::DatabaseStub->new(dbname => 'dummy');

	$self->{'cluster_compactor_constructor'} = sub {
		$self->{'database_mock_list'} = [];
		$self->{'database_compactor_mock_list'} = [];

		Pc::Compactor::Cluster->new(
			database_constructor => sub {
				return Pc::DatabaseStub->new(@_);
			},
			logger => Pc::Logger->new(level => 'info', err_handle => \*STDOUT),
			database_compactor_constructor => sub {
				return $self->create_database_compactor_mock(@_);
			},
			dbname_list => [],
			excluded_dbname_list => [],
			max_retry_count => 0,
			@_);
	};
}

sub create_database_compactor_mock {
	my ($self, @arg_list) = @_;
	my %arg_hash = @arg_list;

	my $mock = Test::MockObject->new();
	$mock->set_true('init');
	$mock->set_true('process');
	$mock->set_false('-is_processed');
	$mock->set_always('-get_dbname', $arg_hash{'dbname'});

	$mock->init(@arg_list);
	push(@{$self->{'database_compactor_mock_list'}}, $mock);

	return $mock;
}

sub test_init_creates_database_compactors : Test(12) {
	my $self = shift;

	$self->{'dbname_list'} = [
		map(
			$_->[0],
			@{$self->{'database'}->{'mock'}->{'data_hash'}
			  ->{'get_dbname_list'}->{'row_list'}})];

	my $data_hash_list = [
		{'arg' => {
			'dbname_list' => $self->{'dbname_list'},
			'excluded_dbname_list' => []},
		 'expected' => $self->{'dbname_list'}},
		{'arg' => {
			'dbname_list' => [],
			'excluded_dbname_list' => []},
		 'expected' => $self->{'dbname_list'}},
		{'arg' => {
			'dbname_list' => $self->{'dbname_list'},
			'excluded_dbname_list' => [$self->{'dbname_list'}->[0]]},
		 'expected' => [$self->{'dbname_list'}->[1]]},
		{'arg' => {
			'dbname_list' => [],
			'excluded_dbname_list' => [$self->{'dbname_list'}->[1]]},
		 'expected' => [$self->{'dbname_list'}->[0]]}];

	for my $data_hash (@{$data_hash_list}) {
		$self->{'cluster_compactor_constructor'}->(
			dbname_list => $data_hash->{'arg'}->{'dbname_list'},
			excluded_dbname_list => (
				$data_hash->{'arg'}->{'excluded_dbname_list'}));

		for my $i (0 .. @{$self->{'database_compactor_mock_list'}} - 1) {
			my $mock = $self->{'database_compactor_mock_list'}->[$i];
			is($mock->call_pos(1), 'init');
			is({'self', $mock->call_args(1)}->{'database'}->get_dbname(),
			   $data_hash->{'expected'}->[$i]);
		}
	}
}

sub test_process_processes_database_compactors : Test(2) {
	my $self = shift;

	$self->{'cluster_compactor_constructor'}->()->process();

	for my $i (0 .. @{$self->{'database_compactor_mock_list'}} - 1) {
		is($self->{'database_compactor_mock_list'}->[$i]->call_pos(2),
		   'process');
	}
}

sub test_stop_retrying_on_max_retries_count : Test(6) {
	my $self = shift;

	$self->{'cluster_compactor_constructor'}->(max_retry_count => 1)->process();

	for my $i (0 .. @{$self->{'database_compactor_mock_list'}} - 1) {
		is($self->{'database_compactor_mock_list'}->[$i]->call_pos(2),
		   'process');
		is($self->{'database_compactor_mock_list'}->[$i]->call_pos(3),
		   'process');
		is($self->{'database_compactor_mock_list'}->[$i]->call_pos(4),
		   undef);
	}
}

sub test_stop_retrying_after_everything_is_processed : Test(4) {
	my $self = shift;

	my $cluster_compactor =
		$self->{'cluster_compactor_constructor'}->(max_retry_count => 1);

	for my $i (0 .. @{$self->{'database_compactor_mock_list'}} - 1) {
		$self->{'database_compactor_mock_list'}->[$i]->mock(
			'process', sub { shift->set_true('-is_processed'); });
	}

	$cluster_compactor->process();

	for my $i (0 .. @{$self->{'database_compactor_mock_list'}} - 1) {
		is($self->{'database_compactor_mock_list'}->[$i]->call_pos(2),
		   'process');
		is($self->{'database_compactor_mock_list'}->[$i]->call_pos(3),
		   undef);
	}
}

sub test_init_catches_error_when_getting_dbname_list : Test {
	my $self = shift;

	$self->{'database'}->{'mock'}->{'data_hash'}->{'get_dbname_list'}
	->{'row_list'} = 'SomeError';

	ok($self->{'cluster_compactor_constructor'}->());
}

sub test_init_skips_database_if_cannot_create_its_compactor : Test {
	my $self = shift;

	my $try_count = 0;
	$self->{'cluster_compactor_constructor'}->(
		database_compactor_constructor => sub {
			if ($try_count == 1) {
				die('SomeError');
			}
			$try_count++;
			return $self->create_database_compactor_mock(@_);
		});

	is(@{$self->{'database_compactor_mock_list'}}, 1);
}

1;
