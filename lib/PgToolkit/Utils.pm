package PgToolkit::Utils;

use base qw(PgToolkit::Class);

use strict;
use warnings;

=head1 NAME

B<PgToolkit::Utils> - a utility functions class.

=head1 SYNOPSIS

	my $utils = PgToolkit::Logger->new();
	$utils->get_size_pretty(size => 12345);

=head1 DESCRIPTION

B<PgToolkit::Utils> is a class providing a set of functions solving
different problems.

=head1 METHODS

=head2 B<get_size_pretty()>

Converts size into human readable format. It works exactly like
C<pg_size_pretty()> works.

=head3 Arguments

=over 4

=item C<size>

=back

=head3 Returns

A string representing the size in bytes, kB, MB, GB or TB.

=cut

sub get_size_pretty {
	my ($self, %arg_hash) = @_;

	my $size = $arg_hash{'size'};

	my $postfix_list = ['bytes', 'kB', 'MB', 'GB', 'TB'];
	my $index = 0;
	my $step = 10 * 1024;

	while (
		int(abs($size) + 0.5) >= $step and
		exists $postfix_list->[$index + 1])
	{
		$size /= 1024;
		$index++;
	}

	return
		($size < 0 ? -1 : 1) * int(abs($size) + 0.5).
		' '.$postfix_list->[$index];
}

=head2 B<get_pgpass_password()>

Parses the C<.pgpass> file and returns the password that matches
connection parameters. The file is looked up by the custom location
first and than if not found by the user's home location.

=head3 Arguments

=over 4

=item C<pgpassfile>

a custom location password file

=item C<home>

a user home password file

=item C<host>

=item C<port>

=item C<user>

=item C<dbname>

=back

=head3 Returns

A password string or undef if there is no appropriate entry in the
file.

=cut

sub get_pgpass_password {
	my ($self, %arg_hash) = @_;

	my $pgpass;
	if ($arg_hash{'pgpassfile'}) {
		open($pgpass, '<', $arg_hash{'pgpassfile'});
	} elsif ($arg_hash{'home'}) {
		open($pgpass, '<', $arg_hash{'home'});
	}

	my $password;
	if (defined $pgpass) {
		my $template = join(
			':', map(
				do {
					if (defined $_) {
						s/([\\:])/\\$1/g;
						'(?:'.quotemeta($_).'|\*)';
					} else {
						'\*';
					}
				},
				$arg_hash{'host'}, $arg_hash{'port'}, $arg_hash{'dbname'},
				$arg_hash{'user'}));

		while (<$pgpass>) {
			if (/^$template:(.*)$/) {
				$password = $1;
				last;
			}
		}

		close($pgpass);
	}

	return $password;
}

=head1 SEE ALSO

=over 4

=item L<PgToolkit::Class>

=back

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2012, PostgreSQL-Consulting.com

=head1 AUTHOR

=over 4

=item L<Sergey Konoplev|mailto:sergey.konoplev@postgresql-consulting.com>

=back

=cut

1;
