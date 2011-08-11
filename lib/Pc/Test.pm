package Pc::Test;

use parent qw(Test::Class);

use strict;
use warnings;

=head1 NAME

B<Pc::Test> - a base class for all test cases.

=head1 SYNOPSIS

	use parent qw(Pc::Test);

=head1 DESCRIPTION

B<Pc::Test> a base class for unit tests. It houses some testing
automatization.

To avoid running tests in abstract test classes call the SKIP_CLASS
method like this.

	Pc::SomeAbstractClassTest->SKIP_CLASS(1);

=cut

INIT { Test::Class->runtests() }

=head1 LICENSE AND COPYRIGHT

Copyright 2010-2011 postgresql-consulting.com

TODO Licence boilerplate

=head1 AUTHOR

=over 4

=item L<Sergey Konoplev|mailto:sergey.konoplev@postgresql-consulting.com>

=back

=cut

1;
