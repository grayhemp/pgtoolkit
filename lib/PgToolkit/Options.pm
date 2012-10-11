package PgToolkit::Options;

use base qw(PgToolkit::Class);

use strict;
use warnings;

use Getopt::Long qw(:config bundling no_ignore_case);
use Pod::Usage ();

=head1 NAME

B<PgToolkit::Options> - a generic ad-hoc options parsing and processing
class.

=head1 SYNOPSIS

	my $options = PgToolkit::Options->new(
		definition_hash => {'quiet|q' => 0, 'verbosity|v:s' => 'notice'},
		error_check_code => sub {
			my $option_hash = shift;
			return (
				exists $option_hash->{'quiet'} and
				exists $option_hash->{'verbosity'});
		},
		transform_code => sub {
			my $option_hash = shift;
			if (exists $option_hash->{'quiet'}) {
				$option_hash->{'verbosity'} = 'warning';
			}
		});

	my $verbosity = $options->get(name => 'verbosity');

=head1 DESCRIPTION

B<PgToolkit::Options> encapsulates generic options, default values and
interdependencies mechanisms. The 'help|?' option is implemented by
default.

=head3 Constructor arguments

=over 4

=item C<argv>

an options listref, by default C<\@ARGV>

=item C<out_handle>

an ouptut filehandle, by default C<*STDOUT>

=item C<definition_hash>

an option definitions as keys and default values as values

=item C<error_check_code>

an error checker code reference supplied with an option hash reference
as argument and expected to return either error message or undef

=item C<transform_code>

if you need to do some manipulations with options do it inside this
code.

=back

=head3 Throws

=over 4

=item OptionsError

when an option definitions does not meet naming conditions.

=back

=cut

sub init {
	my ($self, %arg_hash) = @_;

	$self->{'_out_handle'} =
		exists $arg_hash{'out_handle'} ? $arg_hash{'out_handle'} : \*STDOUT;
	$self->{'_argv'} =
		exists $arg_hash{'argv'} ? $arg_hash{'argv'} : \@ARGV;

	my $default_hash = {};
	for my $key (keys %{$arg_hash{'definition_hash'}}) {
		if ($key =~ /(.*?)\|/) {
			$default_hash->{$1} = $arg_hash{'definition_hash'}->{$key};
		} else {
			die('OptionsError Wrong definition "'.$key.'".');
		}
	}

	my $option_hash = {};
	my $result;
	{
		local @ARGV = @{$self->{'_argv'}};
		$result = Getopt::Long::GetOptions(
			$option_hash, 'help|?', 'man|m',
			(keys %{$arg_hash{'definition_hash'}}));
	}

	my $error;
	if (not (exists $option_hash->{'help'} or exists $option_hash->{'man'}) and
		defined $arg_hash{'error_check_code'}) {
		$error = $arg_hash{'error_check_code'}->($option_hash);

		if ($error) {
			$0 =~ /\/(.*?)$/;
			$error = (
				$1.': '.$error."\n".'Try --help for short help, --man for '.
				'full manual.');
		}
	}

	if (defined $arg_hash{'transform_code'}) {
		$arg_hash{'transform_code'}->($option_hash);
	}

	if (not $result or $error or not keys %{$option_hash}) {
		$self->_print_help(
			out_handle_specified => exists $arg_hash{'out_handle'},
			result => $result,
			error => $error,
			sections => ['_']);
	} elsif ($option_hash->{'help'}) {
		$self->_print_help(
			out_handle_specified => exists $arg_hash{'out_handle'},
			result => $result,
			error => $error,
			sections => ['NAME', 'SYNOPSIS']);
	} elsif ($option_hash->{'man'}) {
		$self->_print_help(
			out_handle_specified => exists $arg_hash{'out_handle'},
			result => $result,
			error => $error,
			sections => ['NAME', 'SYNOPSIS', 'DESCRIPTION', 'OPTIONS',
						 'LICENSE AND COPYRIGHT', 'VERSION', 'AUTHOR']);
	}

	$self->{'_option_hash'} = {
		'help' => 0, 'man' => 0, %{$default_hash}, %{$option_hash}};

	return;
}

sub _print_help {
	my ($self, %arg_hash) = @_;

	my ($output, $exitval) =
		$arg_hash{'out_handle_specified'} ?
		($self->{'_out_handle'}, 'NOEXIT') :
		(undef, ($arg_hash{'result'} ? 1 : 2));

	Pod::Usage::pod2usage(
		-message => $arg_hash{'error'},
		-verbose => 99,
		-output => $output,
		-exitval => $exitval,
		-sections => $arg_hash{'sections'});
}

=head1 METHODS

=head2 B<get()>

Returns an option.

=head3 Arguments

=over 4

=item C<name>

=back

=head3 Returns

The value for the name.

=head3 Throws

=over 4

=item C<CompactorOptionsError>

when wrong name supplied in get.

=back

=cut

sub get {
	my ($self, %arg_hash) = @_;

	if (not exists $self->{'_option_hash'}->{$arg_hash{'name'}}) {
		die('OptionsError Wrong name "'.$arg_hash{'name'}.
			'" is supplied in get.');
	}

	return $self->{'_option_hash'}->{$arg_hash{'name'}};
}

=head1 SEE ALSO

=over 4

=item L<Getopt::Long>

=item L<PgToolkit::Class>

=item L<Pod::Usage>

=back

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2012, PostgreSQL-Consulting.com

=head1 AUTHOR

=over 4

=item L<Sergey Konoplev|mailto:sergey.konoplev@postgresql-consulting.com>

=back

=cut

1;
