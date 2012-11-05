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
pg_size_pretty() works.

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
