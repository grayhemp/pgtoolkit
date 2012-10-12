# -*- mode: Perl; -*-
package PgToolkit::OptionsTest;

use base qw(PgToolkit::Test);

use strict;
use warnings;

use Test::Exception;
use Test::More;

use PgToolkit::Options;

=head1 NAME

Some name

=head1 SYNOPSIS

Some synopsis

=head1 DESCRIPTION

Some description

=head1 OPTIONS

Some options

=cut

sub test_help_and_man : Test(4) {
	my $data_hash_list = [
		{'argv' => ['-?'],
		 'out' => <<EOF
Name:
    Some name

Usage:
    Some synopsis

EOF
		},
		{'argv' => ['--help'],
		 'out' => <<EOF
Name:
    Some name

Usage:
    Some synopsis

EOF
		},
		{'argv' => ['-m'],
		 'out' => <<EOF
Name:
    Some name

Usage:
    Some synopsis

Description:
    Some description

Options:
    Some options

EOF
		},
		{'argv' => ['--man'],
		 'out' => <<EOF
Name:
    Some name

Usage:
    Some synopsis

Description:
    Some description

Options:
    Some options

EOF
		}];

	for my $data_hash (@{$data_hash_list}) {
		my $out = '';
		open(my $out_handle, '+<', \ $out);

		PgToolkit::Options->new(
			out_handle => $out_handle,
			argv => $data_hash->{'argv'});

		is($out, $data_hash->{'out'});
	}
}

sub test_extract_options : Test(6) {
	my $data_list = [
		{'argv' => ['-a'],
		 'values' => {'option1' => 'some', 'option2' => 1234}},
		{'argv' => ['-o', 'another', '-p', '4321'],
		 'values' => {'option1' => 'another', 'option2' => 4321}},
		{'argv' => ['-o', 'yet-another'],
		 'values' => {'option1' => 'yet-another', 'option2' => 5678}}];

	for my $data (@{$data_list}) {
		my $options = PgToolkit::Options->new(
			argv => $data->{'argv'},
			definition_hash => {
				'a|i' => 1, 'option1|o:s' => 'some', 'option2|p:i' => 1234},
			transform_code => sub {
				my $option_hash = shift;
				if (defined $option_hash->{'option1'} and
					$option_hash->{'option1'} eq 'yet-another')
				{
					$option_hash->{'option2'} = 5678;
				}
			});

		for my $name (keys %{$data->{'values'}}) {
			is($data->{'values'}->{$name}, $options->get(name => $name));
		}
	}
}

sub test_get_wrong_name : Test {
	throws_ok(
		sub {
			PgToolkit::Options->new(
				argv => ['-a'], definition_hash => {'a|i' => 1})->
				get(name => 'wrong-name');
		},
		qr/OptionsError Wrong name "wrong-name" is supplied in get\./);
}

sub test_get_wrong_definition : Test {
	throws_ok(
		sub {
			PgToolkit::Options->new(
				definition_hash => {'wrong-definition' => 0});
		},
		qr/OptionsError Wrong definition "wrong-definition"\./);
}

sub test_error_check : Test {
	my $out = '';
	open(my $out_handle, '+<', \ $out);

	PgToolkit::Options->new(
		out_handle => $out_handle,
		argv => ['-a', '5'],
		definition_hash => {'aaa|a:i' => 1},
		error_check_code => sub {
			my $option_hash = shift;
			return 'Some error '.$option_hash->{'aaa'};
		});

	is(
		$out,
		<<EOF
lib/PgToolkit/OptionsTest.t: Some error 5
Try --help for short help, --man for full manual.
EOF
		);
}

sub test_unknown_options : Test {
	my $out = '';
	open(my $out_handle, '+<', \ $out);

	PgToolkit::Options->new(
		out_handle => $out_handle,
		argv => ['--bla', '5'],
		definition_hash => {'aaa|a:i' => 1});

	is(
		$out,
		<<EOF
lib/PgToolkit/OptionsTest.t: Unknown option: bla
Try --help for short help, --man for full manual.
EOF
		);
}

1;
