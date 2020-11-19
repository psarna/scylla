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

## "Paxos grace seconds" per-table option

The `paxos_grace_seconds` option is used to set the amount of seconds which
are used to TTL data in paxos tables when using LWT queries against the base
table.

This value is intentionally decoupled from `gc_grace_seconds` since,
in general, the base table could use completely different strategy to garbage
collect entries, e.g. can set `gc_grace_seconds` to 0 if it doesn't use
deletions and hence doesn't need to repair.

However, paxos tables still rely on repair to achieve consistency, and
the user is required to execute repair within `paxos_grace_seconds`.

Default value is equal to `DEFAULT_GC_GRACE_SECONDS`, which is 10 days.

The option can be specified at `CREATE TABLE` or `ALTER TABLE` queries in the same
way as other options by using `WITH` clause:

    CREATE TABLE tbl ...
    WITH paxos_grace_seconds=1234

## ALTER SESSION

The `ALTER SESSION` statement can be used to configure session-specific settings,
e.g. custom latency limits.

Examples:
```cql
ALTER SESSION
	SET latency_limit_for_reads = 50ms
	AND latency_limit_for_writes = 20ms
```
```cql
ALTER SESSION
	DELETE latency_limit_for_reads
```

`ALTER SESSION` allows setting and deleting key-value pairs of predefined
session-specific configuration. This configuration affects only a single
connection on which the statement was executed (similarly to `USE <keyspace>`),
so users and drivers need to take care to transmit the configuration each time
a connection is established.

Currently supported parameters:
 * `latency_limit_for_reads` - takes a value represented as CQL duration and applies custom timeout to read operations,
    similarly to what `read_request_timeout_in_ms` does in scylla.yaml configuration file
 * `latency_limit_for_writes` - takes a value represented as CQL duration and applies custom timeout to write operations
    similarly to what `write_request_timeout_in_ms` does in scylla.yaml configuration file

The current state can be checked via querying the local `system.clients` table:
```cql
SELECT params FROM system.clients;
```

