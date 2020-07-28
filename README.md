<h1 align=center>Migrate</h1>
<p align=center>Migrating V3 Sources to the V4 Platform - oa/batch</p>

This repo will be archived once the migration is complete

## Approach

This repo has a 2 stage approach for migration. It starts by creating an oa/oa branch
which converts all sources to be valid v2 sources, but with data & conform objects
that reference our own internal cache

The OA stack is then pointed to ingest these cached sources. Once all of the cached
sources are processed and ingested, a second branch is created by updating all
sources to v2, but using their live data endpoints. This branch can then be merged into
master when we are ready for production migration.

## Steps

```
node cache.js
```

```
node upgrade.js
```
