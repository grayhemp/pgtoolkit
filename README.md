# PgToolkit - tools for PostgreSQL maintenance

Currently the package contains the only tool `pgcompact`, we are
planning to add much more in the future.  Stay tuned.

The list of changes can be found in [CHANGES.md]. The To-Do List is in
[TODO.md].

## Installation

The easiest way to deploy the toolkit is to get the [latest stable][1]
version of autonomous scripts. The scripts require `Perl >=5.8.8` to
be installed only. Just unpack the archive to your `bin` directory and
enjoy. If you need a specific version or branch of the tools, replace
the `stable` with its name string in [the URL][1].

Another way is to `git clone` the repository:

    git clone git@github.com:grayhemp/pgtoolkit.git

Or to `svn checkout` it:

    svn checkout https://github.com/grayhemp/pgtoolkit

Do not forget to switch to the necessary version branch afterwards.

It contains a `fatpack` directory that also houses the autonomous
scripts so you can use them straight away. If you want to use
non-autonomous versions of these scripts you will find them in the
`bin` directory. To make them work the `PERL5LIB` environment variable
must be set to the `lib` directory.

## pgcompact

A tool to reduce bloat for tables and indexes without heavy locks and
full table rebuilding.

Initially the tool is an automation of the solutions proposed in these
publications:

- [Reducing bloat without locking][2] by Joshua Tolley
- [Reduce bloat of table without long/exclusive locks][3] by Hubert
  Lubaczewski.

If [pgstattuple] is installed `pgcompact` uses it to get a better
statistics. It is highly recommended to be for TOASTed tables and
indexes.

### Usage examples

Shows user manual.

    pgcompact --man

Compacts all the bloated tables in all the databases in the cluster
plus their bloated indexes. Prints additional progress information.

    pgcompact --all --reindex --verbose info

Compacts all the bloated tables in the billing database and their
bloated indexes excepts ones that are in the `pgq` schema.

    pgcompact --dbname billing --exclude-schema pgq --reindex

### Features

- Requires no dependencies except `Perl >=5.8.8`, so it can just be
  copied to server and run
- Works with `DBD::Pg`, `DBD::PgPP` or even using `psql` if there are
  no former ones, detects and chooses the best option automatically
- Can process specified tables, schemes, databases or the whole
  cluster
- Has an ability to exclude tables, schemes or databases from
  processing
- Bloat percentage analysis and processing of those tables that need
  it only, we recommend to install [pgstattuple] for more precise
  estimations
- Indexes bloat analysis and non blocking reindex of those that need
  it
- Analysis and rebuilding of bloated unique constraints and primary
  keys where possible
- Incremental processing, in other words one can stop the process and
  continue at any time later
- Dynamic adjustment to current load of database, to not affect its
  performance
- Instructs administrators, supplying them with ready to use DDL, to
  manually rebuild database objects which can not be rebuilt
  automatically.

## License and Copyright

Copyright &copy; 2011-2013 Sergey Konoplev, Maxim Boguk

PgToolkit is released under the PostgreSQL License, read
[LICENSE.md] for additional information.

## Authors

- [Sergey Konoplev](mailto:gray.ru@gmail.com)
- [Maxim Boguk](mailto:maxim.boguk@gmail.com)

## Contributors

Thank you:

- [PostgreSQL-Consulting.com](http://www.postgresql-consulting.com)
  for a huge amount of ideas and lots of testing
- Lonni Friedman for your ideas
- Rural Hunter for ideas and testing.

[CHANGES.md]: CHANGES.md
[TODO.md]: TODO.md
[LICENSE.md]: LICENSE.md
[pgstattuple]: http://www.postgresql.org/docs/current/static/pgstattuple.html
[1]: http://github.com/grayhemp/pgtoolkit/archive/stable.tar.gz
[2]: http://blog.endpoint.com/2010/09/reducing-bloat-without-locking.html
[3]: http://depesz.com/index.php/2010/10/17/reduce-bloat-of-table-without-longexclusive-locks
