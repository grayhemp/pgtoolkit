# PgToolkit - tools for PostgreSQL maintenance

## To-Do List

### PgToolkit v1.0.1

- Create a full-update mode for post- column manipulation cleaning
- Add --reindex-nonbtree
- Add an ability to collect manual reindex commands into a script
- Make a normal exceptions mechanism
- Reindex cases with complex FK dependencies
- Use DROP INDEX CONCURRENTLY
- Make a non blocking reindex
- Print an informative message for "Died" cases
- Use approximation for not TOASTed tables
- Fix the TOASTed tables stats approximation
- Fix the issue with completion message on incomplete tables

### PgToolkit v1.1alpha1

- Create a pganalyze prototype
- Create a pgwatch prototype
- Create a pgasync prototype
- Create a pgbackup prototype
- Create a pgcheck prototype
- Create a pgrestore prototype
- Create a pgpitr prototype
- Think of a way to compact TOAST tables

### PgToolkit v1.2alpha1

- Create a pgpool prototype
- Create a pgbalance prototype
- Create a pgha prototype

### PgToolkit v2.0alpha1

- Implement it on C/C++ as a set of extensions and background workers
