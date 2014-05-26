# -*- mode: Perl; -*-
package PgToolkit::CompactClusterTest;

use base qw(PgToolkit::Test);

use strict;
use warnings;

use Test::MockObject;
use Test::More;

use Test::Exception;

use PgToolkit::DatabaseStub;

use PgToolkit::Logger;
use PgToolkit::Compact::Cluster;

sub setup : Test(setup) {
	my $self = shift;

	$self->{'database'} = PgToolkit::DatabaseStub->new(dbname => 'dummy');

	$self->{'cluster_compact_constructor'} = sub {
		$self->{'database_compact_mock_list'} = [];
		$self->{'database_compact_mock_process_counter'} = 0;

		PgToolkit::Compact::Cluster->new(
			database_constructor => sub {
				return PgToolkit::DatabaseStub->new(@_);
			},
			logger => PgToolkit::Logger->new(
				level => 'info', err_handle => \*STDOUT),
			dry_run => 0,
			database_compact_constructor => sub {
				return $self->create_database_compact_mock(@_);
			},
			dbname_list => [],
			excluded_dbname_list => [],
			max_retry_count => 0,
			@_);
	};
}

sub create_database_compact_mock {
	my ($self, @arg_list) = @_;
	my %arg_hash = @arg_list;

	my $mock = Test::MockObject->new();
	$mock->set_true('init');
	$mock->mock(
		'process',
		sub {
			$mock->set_always(
				'-process_order',
				$self->{'database_compact_mock_process_counter'});
			$self->{'database_compact_mock_process_counter'}++;

			return;
		});
	$mock->set_false('-is_processed');
	$mock->set_always('-get_size_delta', int(rand() * 1000));
	$mock->set_always('-get_total_size_delta', int(rand() * 1000));
	$mock->set_always('-get_log_target', $arg_hash{'database'}->get_dbname());

	$mock->init(@arg_list);
	push(@{$self->{'database_compact_mock_list'}}, $mock);

	return $mock;
}

sub test_init_creates_database_compacts_in_the_returning_order : Test(20) {
	my $self = shift;

	my $dbname_list = [
		map($_->[0],
			@{$self->{'database'}->{'mock'}->{'data_hash'}
			  ->{'get_dbname_list1'}->{'row_list'}})];

	my $data_hash_list = [
		{'args' => {
			'dbname_list' => [],
			'excluded_dbname_list' => []},
		 'expected' => $dbname_list},
		{'args' => {
			'dbname_list' => [@{$dbname_list}[0, 1]],
			'excluded_dbname_list' => []},
		 'expected' => [@{$dbname_list}[0, 1]]},
		{'args' => {
			'dbname_list' => [],
			'excluded_dbname_list' => [@{$dbname_list}[0, 1]]},
		 'expected' => [@{$dbname_list}[2, 3]]},
		{'args' => {
			'dbname_list' => [@{$dbname_list}[0, 2]],
			'excluded_dbname_list' => [@{$dbname_list}[1, 3]]},
		 'expected' => [@{$dbname_list}[0, 2]]}];

	for my $data_hash (@{$data_hash_list}) {
		$self->{'cluster_compact_constructor'}->(%{$data_hash->{'args'}});

		for my $i (0 .. @{$self->{'database_compact_mock_list'}} - 1) {
			my $mock = $self->{'database_compact_mock_list'}->[$i];
			is($mock->call_pos(1), 'init');
			is({'self', $mock->call_args(1)}->{'database'}->get_dbname(),
			   $data_hash->{'expected'}->[$i]);
		}
	}
}

sub test_process_processes_database_compacts_in_their_order : Test(8) {
	my $self = shift;

	$self->{'cluster_compact_constructor'}->()->process();

	for my $i (0 .. @{$self->{'database_compact_mock_list'}} - 1) {
		is($self->{'database_compact_mock_list'}->[$i]->call_pos(2),
		   'process');
		is($self->{'database_compact_mock_list'}->[$i]->process_order(), $i);
	}
}

sub test_stop_retrying_on_max_retries_count : Test(20) {
	my $self = shift;

	$self->{'cluster_compact_constructor'}->(max_retry_count => 1)->process();

	for my $i (0 .. @{$self->{'database_compact_mock_list'}} - 1) {
		is($self->{'database_compact_mock_list'}->[$i]->call_pos(2),
		   'process');
		is(
			{'self',
			 $self->{'database_compact_mock_list'}->[$i]->call_args(2)
			}->{'attempt'}, 0);
		is($self->{'database_compact_mock_list'}->[$i]->call_pos(3),
		   'process');
		is(
			{'self',
			 $self->{'database_compact_mock_list'}->[$i]->call_args(3)
			}->{'attempt'}, 1);
		is($self->{'database_compact_mock_list'}->[$i]->call_pos(4),
		   undef);
	}
}

sub test_stop_retrying_after_everything_is_processed : Test(8) {
	my $self = shift;

	my $cluster_compact =
		$self->{'cluster_compact_constructor'}->(max_retry_count => 1);

	for my $i (0 .. @{$self->{'database_compact_mock_list'}} - 1) {
		$self->{'database_compact_mock_list'}->[$i]->mock(
			'process', sub { shift->set_true('-is_processed'); });
	}

	$cluster_compact->process();

	for my $i (0 .. @{$self->{'database_compact_mock_list'}} - 1) {
		is($self->{'database_compact_mock_list'}->[$i]->call_pos(2),
		   'process');
		is($self->{'database_compact_mock_list'}->[$i]->call_pos(3),
		   undef);
	}
}

sub test_get_size_delta : Test {
	my $self = shift;

	my $cluster_compact = $self->{'cluster_compact_constructor'}->();

	for my $database_compact_mock (@{$self->{'database_compact_mock_list'}})
	{
		$database_compact_mock->mock(
			'-is_processed',
			sub {
				shift->set_true('-is_processed');
				return 0;
			});
	}

	$cluster_compact->process();

	my $result = 0;
	map($result += $_->get_size_delta(),
		@{$self->{'database_compact_mock_list'}});

	is($cluster_compact->get_size_delta(), $result);
}

sub test_get_total_size_delta : Test {
	my $self = shift;

	my $cluster_compact = $self->{'cluster_compact_constructor'}->();

	for my $database_compact_mock (@{$self->{'database_compact_mock_list'}})
	{
		$database_compact_mock->mock(
			'-is_processed',
			sub {
				shift->set_true('-is_processed');
				return 0;
			});
	}

	$cluster_compact->process();

	my $result = 0;
	map($result += $_->get_total_size_delta(),
		@{$self->{'database_compact_mock_list'}});

	is($cluster_compact->get_total_size_delta(), $result);
}

1;
