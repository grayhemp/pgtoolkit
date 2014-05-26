# PgToolkit - tools for PostgreSQL maintenance

## To-Do List

### PgToolkit v1.0.2

- Use `DROP INDEX CONCURRENTLY` when possible
- Print `TOAST` tables bloat warnings
- Use the `INSERT/DELETE` technique when possible
- Use approximation for not `TOAST`'ed tables as an experimental
  feature

### PgToolkit v1.1alpha1

- Create a `pganalyze` prototype
- Create a `pgwatch` prototype
- Create a `pgasync` prototype
- Create a `pgbackup` prototype
- Create a `pgcheck` prototype
- Create a `pgrestore` prototype
- Create a `pgpitr` prototype
- Think of a way to compact `TOAST` tables
- Refactor the `Compact` classes and tests
- Implement a normal exceptions mechanism
- Re-index cases with complex foreign key dependencies
- Create a full-update mode for post- column manipulation cleaning
- Add the `--reindex-nonbtree` option
- Add an ability to collect manual re-index commands into a file
- Add a default database using system user name
- Add a restricted run time option
- Fix the `TOAST`'ed tables stats approximation
- Clean up interruption consequences

### PgToolkit v1.2alpha1

- Create a `pgpool` prototype
- Create a `pgbalance` prototype
- Create a `pgha` prototype

### PgToolkit v2.0alpha1

- Implement it on C/C++ as a set of extensions and background workers
