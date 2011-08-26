# -*- mode: Perl; -*-
package PgToolkit::OptionsTest;

use parent qw(PgToolkit::Test);

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

sub test_print_usage : Test(3) {
	my $data_hash_list = [
		{'argv' => [],
		 'out' => ''},
		#{'argv' => ['--wrong-option'],
		# 'out' => "Usage:\n    Some synopsis\n\n"},
		{'argv' => ['-?'],
		 'out' => <<EOF
NAME
    Some name

SYNOPSIS
    Some synopsis

DESCRIPTION
    Some description

OPTIONS
    Some options

EOF
		},
		{'argv' => ['--help'],
		 'out' => <<EOF
NAME
    Some name

SYNOPSIS
    Some synopsis

DESCRIPTION
    Some description

OPTIONS
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
		{'argv' => [''],
		 'values' => {'option1' => 'some', 'option2' => 1234}},
		{'argv' => ['-o', 'another', '-p', '4321'],
		 'values' => {'option1' => 'another', 'option2' => 4321}},
		{'argv' => ['-o', 'yet-another'],
		 'values' => {'option1' => 'yet-another', 'option2' => 5678}}];

	for my $data (@{$data_list}) {
		my $options = PgToolkit::Options->new(
			argv => $data->{'argv'},
			definition_hash => {
				'option1|o:s' => 'some', 'option2|p:i' => 1234},
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
			PgToolkit::Options->new()->get(name => 'wrong-name');
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

1;
