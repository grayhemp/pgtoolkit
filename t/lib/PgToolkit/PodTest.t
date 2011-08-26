# -*- mode: Perl; -*-
package PodTest;

use strict;
use warnings;

use Test::Pod 1.00;

all_pod_files_ok(all_pod_files('.'));

1;
