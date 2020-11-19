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
e.g. custom timeouts.

Examples:
```cql
ALTER SESSION
	SET read_timeout = 50ms
```
```cql
ALTER SESSION
	SET read_timeout = NULL
```

### Driver support
`ALTER SESSION` allows setting and deleting key-value pairs of predefined
session-specific configuration. This configuration affects only a single
connection on which the statement was executed (similarly to `USE <keyspace>`),
so users and drivers need to take care to transmit the configuration each time
a connection is established.
Possible implementations include:
 - parsing selected CQL statements in the driver (that's how cqlsh deals with `USE <keyspace>`)
 - disallowing raw `ALTER SESSION` statements and instead exposing an API for configuring
   session params (that's how gocql deals with `USE <keyspace>`)
 - reading the `system.clients` local table after sending a statement which looks like `ALTER SESSION`
   and extracting the map of params from it - the map can be remembered and retransmitted to new connections

It would also be possible to return specific metadata from Scylla, which could then be interpreted
by the driver - such metadata could either be a flag which indicates that the parameters changed,
or a serialized map of all session parameters, ready to be retransmitted to other connections.
These are not implemented for now, because they require potentially backward incompatible changes
in Scylla protocol, and it's not certain if the changes are worth it.

Due to the fact that the burden of properly propagating per-session parameters is currently on the driver,
there are important caveats to remember for the future:
 - it's not safe to put `ALTER SESSION` in a stored procedure (should we have any in the future)
 - it's not safe to put `ALTER SESSION` in a batch (should we allow something else than modification statements in a batch in the future)
 - it's not safe to use `ALTER SESSION` by drivers which are not capable of propagating the information
   to all connections. It will not cause crashes, but it will also most likely work as expected, since the parameters
   cannot be guaranteed to be known by all connections used by a single session

### Supported parameters
 * `read_timeout` - takes a value represented as CQL duration and applies custom timeout to read operations,
    similarly to what `read_request_timeout_in_ms` does in scylla.yaml configuration file
 * `write_timeout` - takes a value represented as CQL duration and applies custom timeout to write operations
    similarly to what `write_request_timeout_in_ms` does in scylla.yaml configuration file

Timeout parameters take precedence over global values set up in scylla.yaml configuration file.
If a session-specific timeout is specified, it will be used instead of the global value.
In order to go back to using the default, it's sufficient to remove the per-session timeout
by calling `ALTER SESSION SET timeout_we_do_not_want_anymore = NULL`.

### Checking current values

The current state can be checked via querying the local `system.clients` table:
```cql
SELECT address, port, params FROM system.clients;

 address   | port  | params
-----------+-------+------------------------------------------------
 127.0.0.1 | 58216 |                                           null
 127.0.0.1 | 58218 | {'read_timeout': '1s', 'write_timeout': '50ms'}

```

