# PgToolkit - tools for PostgreSQL maintenance

Currently the package contains the only tool `pgcompact`, we are
planning to add much more in the future. Stay tuned.

The list of changes can be found in [CHANGES.md]. The To-Do List is in
[TODO.md].

## Installation

The easiest way to deploy the toolkit is to download the [latest
stable][1] version, unpack the archive and copy the autonomous scripts
from the `fatpack` directory to your `bin` or to just add this
directory to your `PATH`. The autonomous scripts are packed with all
the dependencies and need `Perl >=5.8.8` to be installed only.

If you need a specific version or branch of the tools, replace the
`stable` with its name string in [the URL][1].

Another way is to `git clone` the repository

    git clone https://github.com/grayhemp/pgtoolkit.git

or to `svn checkout` it

    svn checkout https://github.com/grayhemp/pgtoolkit

**Do not forget to switch to the necessary version branch afterwards.**

You can also use the autonomous scripts in the `fatpack` directory or
the non-autonomous versions of them in the `bin` directory. For the
latter you need the `lib` directory either to be in the same
sub-directory with `bin` or to be in your `PERL5LIB`.

## pgcompact

A tool to reduce bloat for tables and indexes without heavy locks and
full table rebuilding.

Initially the tool is an automation of the solutions proposed in these
publications:

- [Reducing bloat without locking][2] by Joshua Tolley
- [Reduce bloat of table without long/exclusive locks][3] by Hubert
  Lubaczewski.

If [pgstattuple] is installed `pgcompact` uses it to get a better
statistics. It is highly recommended to be for `TOAST`ed tables and
indexes.

### Usage examples

Shows user manual.

    pgcompact --man

Compacts all the bloated tables in all the databases in the cluster
plus their bloated indexes. Prints additional progress information.

    pgcompact --all --reindex --verbosity info

Compacts all the bloated tables in the billing database and their
bloated indexes excepts ones that are in the `pgq` schema.

    pgcompact --dbname billing --exclude-schema pgq --reindex

### Features

- Requires no dependencies except `Perl >=5.8.8`, so it can just be
  copied to server and run
- Works via `DBD::Pg`, `DBD::PgPP` or even `psql` if there are no
  former ones, detects and chooses the best option automatically
- Processes either whole cluster or specified tables, schemes,
  databases only
- Has an ability to exclude tables, schemes or databases from
  processing
- Performs bloat analysis and processes those tables that have it
  only. We recommend to install [pgstattuple] for more precise
  estimations.
- Uses non blocking reindex techniques
- Performs indexes bloat analysis and processes only the required ones
- Analyses and rebuilds bloated unique constraints and primary keys
  where possible
- Provides TOAST tables and their indexes bloat information and
  rebuilding instructions
- Incremental processing, in other words one can stop the process and
  continue it at any time later
- Dynamically adjusts behavior for current load of database to not
  affect its performance
- Can be run in several parallel sessions on the same instance to
  process the tables faster
- Instructs administrators, supplying them with ready to use DDL, to
  manually rebuild database objects that can not be rebuilt
  automatically

## See Also

- [PgCookbook](https://github.com/grayhemp/pgcookbook) - a PostgreSQL
  documentation project

## License and Copyright

Copyright &copy; 2011-2014 Sergey Konoplev, Maxim Boguk

PgToolkit is released under the PostgreSQL License, read
[LICENSE.md] for additional information.

## Authors

- [Sergey Konoplev](mailto:gray.ru@gmail.com)

## Contributors

Thank you:

- DenisBY for bug reports and testing
- [PostgreSQL-Consulting.com](http://www.postgresql-consulting.com)
  for a huge amount of ideas and lots of testing
- [Maxim Boguk](mailto:maxim.boguk@gmail.com) for ideas, testing and
  useful hints
- Lonni Friedman for your ideas
- Rural Hunter for ideas and testing
- Hubert "depesz" Lubaczewski for testing, useful hints and code
  contributions
- Gonzalo Gil for testing.

[CHANGES.md]: CHANGES.md
[TODO.md]: TODO.md
[LICENSE.md]: LICENSE.md
[pgstattuple]: http://www.postgresql.org/docs/current/static/pgstattuple.html
[1]: http://github.com/grayhemp/pgtoolkit/archive/stable.tar.gz
[2]: http://blog.endpoint.com/2010/09/reducing-bloat-without-locking.html
[3]: http://depesz.com/index.php/2010/10/17/reduce-bloat-of-table-without-longexclusive-locks
