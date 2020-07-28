# Scylla CQL extensions

Scylla extends the CQL language to provide a few extra features. This document
lists those extensions.

## BYPASS CACHE clause

The `BYPASS CACHE` clause on `SELECT` statements informs the database that the data
being read is unlikely to be read again in the near future, and also
was unlikely to have been read in the near past; therefore no attempt
should be made to read it from the cache or to populate the cache with
the data. This is mostly useful for range scans; these typically
process large amounts of data with no temporal locality and do not
benefit from the cache.

The clause is placed immediately after the optional `ALLOW FILTERING`
clause:

    SELECT ... FROM ...
    WHERE ...
    ALLOW FILTERING          -- optional
    BYPASS CACHE

## PRUNE MATERIALIZED VIEW statements

A special statement is dedicated for pruning ghost rows from materialized views.
Ghost row is an inconsistency issue which manifests itself by having rows
in a materialized view which do not correspond to any base table rows.
Such inconsistencies should be prevented altogether and Scylla is striving to avoid
them, but *if* they happen, this statement can be used to restore a materialized view
to a fully consistent state without rebuilding it from scratch.

Example usages:
```cql
  PRUNE MATERIALIZED VIEW my_view;
  PRUNE MATERIALIZED VIEW my_view WHERE token(v) > 7 AND token(v) < 1535250;
  PRUNE MATERIALIZED VIEW my_view WHERE v = 19;
```

The statement works by fetching requested rows from a materialized view
and then trying to fetch their corresponding rows from the base table.
If it turns out that the base row does not exist, the row is considered
a ghost row and is thus deleted. The statement implicitly works with
consistency level ALL when fetching from the base table to avoid false
positives. As the example shows, a materialized view can be pruned
in one go, but one can also specify specific primary keys or token ranges,
which is recommended in order to make the operation less heavyweight
and allow for running multiple parallel pruning statements for non-overlapping
token ranges.

