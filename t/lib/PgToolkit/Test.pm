package PgToolkit::Test;

use base qw(Test::Class);

use strict;
use warnings;

=head1 NAME

B<PgToolkit::Test> - base class for all test cases.

=head1 SYNOPSIS

	use base qw(PgToolkit::Test);

=head1 DESCRIPTION

B<PgToolkit::Test> is a base class for unit tests. It houses some
testing automatization.

To avoid running tests in abstract test classes call the SKIP_CLASS
method like this.

	PgToolkit::SomeAbstractClassTest->SKIP_CLASS(1);

=cut

INIT { Test::Class->runtests() }

=head1 SEE ALSO

=over 4

=item L<Test::Class>

=back

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2011-2014 Sergey Konoplev

PgToolkit is released under the PostgreSQL License, read COPYRIGHT.md
for additional information.

=head1 AUTHOR

=over 4

=item L<Sergey Konoplev|mailto:gray.ru@gmail.com>

=back

=cut

1;
