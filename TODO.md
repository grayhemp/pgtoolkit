# PgToolkit - tools for PostgreSQL maintenance

## To-Do List

### PgToolkit v1.0.3

- Fix the incorrect cleaning result bug

### PgToolkit v1.1alpha1

- Create a `pgwatch` prototype
- Create a `pgaudit` prototype
- Create a `pgstatement` prototype
- Create a `pgasync` prototype
- Create a `pgbackup` prototype
- Create a `pgrestore` prototype
- Create a `pgpitr` prototype
- Think of a way to compact `TOAST` tables
- Refactor the `Compactor` classes and tests
- Implement a normal exceptions mechanism
- Re-index cases with complex foreign key dependencies
- Create a full-update mode for post- column manipulation cleaning
- Add the `--reindex-nonbtree` option
- Add an ability to collect manual re-index commands into a file
- Add a default database using system user name
- Add a restricted run time option
- Fix the `TOAST`'ed tables stats approximation
- Clean up interruption consequences
- Use the `INSERT/DELETE` technique when possible
- Add index bloat approximation
- Add a `--no-toast` option
- Make reindex on by default and add a `--no-reindex` option
- Use approximation for not `TOAST`'ed tables
- Make routine vacuum off by default and add a `--routine-vacuum`
  option
- Find the best value of `--delay-ratio` and set it as default

### PgToolkit v1.2alpha1

- Create a `pgpool` prototype
- Create a `pgbalance` prototype
- Create a `pgha` prototype

### Release v1.3alpha1

- Create a `pgconfig` prototype

### Release v1.4alpha1

- Create a `pgring` prototype

### PgToolkit v2.0alpha1

- Implement it on C/C++ as a set of extensions and background workers
