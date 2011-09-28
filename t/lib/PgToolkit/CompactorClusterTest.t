# -*- mode: Perl; -*-
package PgToolkit::CompactorClusterTest;

use parent qw(PgToolkit::Test);

use strict;
use warnings;

use Test::MockObject;
use Test::More;

use Test::Exception;

use PgToolkit::DatabaseStub;

use PgToolkit::Logger;
use PgToolkit::Compactor::Cluster;

sub setup : Test(setup) {
	my $self = shift;

	$self->{'database'} = PgToolkit::DatabaseStub->new(dbname => 'dummy');

	$self->{'cluster_compactor_constructor'} = sub {
		$self->{'database_mock_list'} = [];
		$self->{'database_compactor_mock_list'} = [];
		$self->{'database_compactor_mock_process_counter'} = 0;

		PgToolkit::Compactor::Cluster->new(
			database_constructor => sub {
				return PgToolkit::DatabaseStub->new(@_);
			},
			logger => PgToolkit::Logger->new(
				level => 'info', err_handle => \*STDOUT),
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
	$mock->mock(
		'process',
		sub {
			$mock->set_always(
				'-process_order',
				$self->{'database_compactor_mock_process_counter'});
			$self->{'database_compactor_mock_process_counter'}++;

			return;
		});
	$mock->set_false('-is_processed');
	$mock->set_always('-get_dbname', $arg_hash{'dbname'});

	$mock->init(@arg_list);
	push(@{$self->{'database_compactor_mock_list'}}, $mock);

	return $mock;
}

sub test_init_creates_database_compactors_in_the_returning_order : Test(16) {
	my $self = shift;

	my $dbname_list1 = [
		map($_->[0],
			@{$self->{'database'}->{'mock'}->{'data_hash'}->
			  {'get_dbname_list1'}->{'row_list'}})];
	my $dbname_list2 = [
		map($_->[0],
			@{$self->{'database'}->{'mock'}->{'data_hash'}->
			  {'get_dbname_list2'}->{'row_list'}})];

	my $data_hash_list = [
		{'args' => {
			'dbname_list' => [reverse @{$dbname_list2}],
			'excluded_dbname_list' => []},
		 'expected' => $dbname_list2},
		{'args' => {
			'dbname_list' => [],
			'excluded_dbname_list' => []},
		 'expected' => $dbname_list1},
		{'args' => {
			'dbname_list' => [reverse @{$dbname_list2}],
			'excluded_dbname_list' => [$dbname_list2->[0]]},
		 'expected' => [$dbname_list2->[1]]},
		{'args' => {
			'dbname_list' => [],
			'excluded_dbname_list' => [$dbname_list1->[1]]},
		 'expected' => [$dbname_list1->[0]]}];

	for my $data_hash (@{$data_hash_list}) {
		$self->{'cluster_compactor_constructor'}->(%{$data_hash->{'args'}});

		is(@{$self->{'database_compactor_mock_list'}},
		   @{$data_hash->{'expected'}});
		for my $i (0 .. @{$self->{'database_compactor_mock_list'}} - 1) {
			my $mock = $self->{'database_compactor_mock_list'}->[$i];
			is($mock->call_pos(1), 'init');
			is({'self', $mock->call_args(1)}->{'database'}->get_dbname(),
			   $data_hash->{'expected'}->[$i]);
		}
	}
}

sub test_process_processes_database_compactors_in_their_order : Test(4) {
	my $self = shift;

	$self->{'cluster_compactor_constructor'}->()->process();

	for my $i (0 .. @{$self->{'database_compactor_mock_list'}} - 1) {
		is($self->{'database_compactor_mock_list'}->[$i]->call_pos(2),
		   'process');
		is($self->{'database_compactor_mock_list'}->[$i]->process_order(), $i);
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
