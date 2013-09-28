# PgToolkit - tools for PostgreSQL maintenance

## Changes

### 2013-02-01 - PgToolkit v1.0rc1

- Refactored information files, `PgToolkit` is released under the
  PostgreSQL License now
- Improved error messages, help hints and options' warnings.
- Added `-V` (`--version`) functionality
- Fixed the bug with storage parameters on tables and indexes
- Removed useless information from compacting results
- Added bloat information to the messages about reindex impossibility
- Made sizes pretty printed (kB, MB, GB, TB)
- Moved skipping messages to the `INFO` level
- Fixed the infinity loop on the size change check bug
- Fixed the bug when reindex is skipped if table was not compacted but
  will be skipped the next round
- Fixed the bug of reindexing when `--dry-run` is specified
- Optimized the pgstattuple based bloat calculation
- Refactored autonomous scripts building facilities, now the scripts
  are available straight from the `fatpack` directory
- Fixed the error when 0 or 1 pages left
- Fix the silent `--man` and `--help` problem
- Separated completion statistics and warnings
- Added a basic processing of the cases with tables/indexes deletion
  in the process of compacting
- Fixed the reindex syntax and added a comment with database name
- Fixed the partial indexes reindexing
- Increased verbosity on connection errors (thanks to Rural Hunter).
- Made it use `.pgpass` and environment variables (thanks to Rural
  Hunter)
- Refactored the psql adapter to bidirectional communication what
  increased processing speed dramatically
- Got rid of the final exception in the cleaning stored function
  (thanks to Lonni Friedman).

### 2012-04-09 - PgToolkit v1.0beta3

- Added reindexing when table is skipped and `pgstattuple` installed
- Fixed wrong `fillfactor` calculation for tables and indexes
- Added the minimum pages restriction for indexes
- Added a check of base restrictions after processing
- Added a `session_replication_role` check in the cleaning function
- Made reindexing of non `btree` indexes only when `--force` is
  specified
- Refactored the index dependencies check to not rebuild indexes that
  require heavy locks
- Solved the `psql` adapter quietness problem (thanks to `e.sergey`
  for the bug report).

### 2012-02-21 - PgToolkit v1.0beta2

- Fixed the error when `-d` and/or `-t` are specified
- Fixed the bug with conforming strings
- Fixed the unicode name index altering bug
- Fixed the error output recognition in the psql adapter
- Prevented processing from interupting after deadlocks
- Fixed the "can not get bloat statistics" warning on empty tables
- Disabled printing incomplete statistics if table is processed
- Set `synchronous_commit` to off and `session_replication_role` to
  replica on the database level.

### 2012-02-02 - PgToolkit v1.0beta1

Take into consideration the new name of the tool.

- Renamed `pg_compactor` tool to `pgcompactor`
- Added `--dry-run`
- Added fillfactor to all the statistics calculations
- Fixed the reindex duration bug
- Got rid of the schema as a middle level
- Tables are sorted by size inside their database
- Added the ability of reindexing internal indexes like `PRIMARY KEY`
  and `UNIQUE`
- Added the reindexing necessity check based on the `pgstattuple`
  statistics
- A lot of minor changes.

### 2012-01-11 - PgToolkit v1.0alpha7

- Reworked reindex to be performed in any case when a table has been
  compacted or it has not (after the last iteration)
- Fixed the query for testing "always" and "replica" triggers
- Refactored the logic of bloat statistics to lower the amount of
  heavy `pgstattuple` calls
- Fixed the final statistics
- Added bloat statistics requests duration.

### 2012-01-06 - PgToolkit v1.0alpha6

- Refactored out useless resource consuming `pgstattuple` calls
- Added `DEBUG0` and `DEBUG1` logging levels and applied to SQL
  queries logging
- Turned off server side prepares for `DBI` drivers
- Made some minor changes.

### 2011-11-02 - PgToolkit v1.0alpha5

### 2011-10-26 - PgToolkit v1.0alpha4

### 2011-10-12 - PgToolkit v1.0alpha3

### 2011-10-11 - PgToolkit v1.0alpha2

### 2011-08-11 - PgToolkit v1.0alpha1
