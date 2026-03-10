# scylla-bench-java

A Java port of [scylla-bench](https://github.com/scylladb/scylla-bench) — a benchmarking tool for [ScyllaDB](https://github.com/scylladb/scylla) that uses the [ScyllaDB Java Driver](https://github.com/scylladb/java-driver) instead of the Go gocql driver.

## Features

- Full port of scylla-bench's benchmark modes: `write`, `read`, `counter_update`, `counter_read`, `scan`, `mixed`
- All workloads: `sequential`, `uniform`, `timeseries`
- HDR histogram latency reporting (raw + coordinated-omission corrected)
- Rate limiting (`-max-rate`)
- Data validation (`-validate-data`)
- TLS / mutual TLS support
- Configurable consistency levels, retry policy, compression
- **New**: Choose your ScyllaDB Java driver version at build time

## Install

### Prerequisites

- Java 11+
- Maven 3.6+

### Build

```bash
git clone https://github.com/scylladb/scylla-bench-java
cd scylla-bench-java/
mvn package -DskipTests
```

This produces `target/scylla-bench-java.jar` (a fat/uber JAR with all dependencies).

### Specifying the ScyllaDB Java Driver Version

The driver version is chosen **at build time** using the `scylla.driver.version` Maven property:

```bash
# Default version (see pom.xml)
mvn package

# Build with a specific driver version
mvn package -Dscylla.driver.version=4.18.0.0
mvn package -Dscylla.driver.version=4.15.0.0
mvn package -Dscylla.driver.version=4.10.0.0
```

All versions of `com.scylladb:java-driver-core` published on Maven Central are supported.

### Show Version Information

```bash
java -jar target/scylla-bench-java.jar --version
```

Output example:
```
scylla-bench-java 1.0.0-SNAPSHOT
ScyllaDB Java Driver: 4.18.0.0
Build time: 2026-03-08 12:00:00

To build with a specific driver version:
  mvn package -Dscylla.driver.version=<version>
```

## Docker

```bash
# Build the Docker image
docker build -t scylla-bench-java:latest .

# Or build with a specific driver version
docker build --build-arg DRIVER_VERSION=4.18.0.0 -t scylla-bench-java:4.18.0.0 .

# Run
docker run --rm --network=host scylla-bench-java:latest \
  -mode write -workload sequential -nodes 127.0.0.1
```

## Usage

```bash
java -jar target/scylla-bench-java.jar [options]
```

### Quick Examples

```bash
# Sequential write benchmark
java -jar target/scylla-bench-java.jar \
  -mode write -workload sequential \
  -nodes 127.0.0.1 \
  -partition-count 10000 \
  -clustering-row-count 100 \
  -clustering-row-size fixed:4 \
  -concurrency 16

# Uniform read benchmark with latency measurement, 60 second run
java -jar target/scylla-bench-java.jar \
  -mode read -workload uniform \
  -nodes 127.0.0.1 \
  -duration 60s \
  -concurrency 32 \
  -measure-latency

# Mixed read/write (50/50) with rate limit
java -jar target/scylla-bench-java.jar \
  -mode mixed -workload uniform \
  -nodes scylla-node1,scylla-node2 \
  -duration 5m \
  -concurrency 64 \
  -max-rate 10000

# Full-table scan
java -jar target/scylla-bench-java.jar \
  -mode scan \
  -nodes 127.0.0.1 \
  -range-count 900 \
  -concurrency 9

# Counter updates
java -jar target/scylla-bench-java.jar \
  -mode counter_update -workload sequential \
  -nodes 127.0.0.1 \
  -partition-count 1000

# Timeseries write
java -jar target/scylla-bench-java.jar \
  -mode write -workload timeseries \
  -nodes 127.0.0.1 \
  -partition-count 100 \
  -max-rate 1000 \
  -duration 10m

# Validate data integrity
java -jar target/scylla-bench-java.jar \
  -mode write -workload sequential \
  -nodes 127.0.0.1 -validate-data
java -jar target/scylla-bench-java.jar \
  -mode read -workload sequential \
  -nodes 127.0.0.1 -validate-data
```

## Options Reference

### Mode & Workload
| Option | Default | Description |
|--------|---------|-------------|
| `-mode` | *(required)* | `write`, `counter_update`, `read`, `counter_read`, `scan`, `mixed`, `user` |
| `-workload` | *(required)* | `sequential`, `uniform`, `timeseries` (not used in `user` mode) |
| `-profile` | | Path to YAML profile file (required when `-mode user`) |

### Connection
| Option | Default | Description |
|--------|---------|-------------|
| `-nodes` | `127.0.0.1` | Comma-separated contact points |
| `-port` | `9042` | CQL native port |
| `-datacenter` | `datacenter1` | Local datacenter name for DC-aware routing |
| `-username` | | CQL username |
| `-password` | | CQL password |
| `-connection-count` | `4` | Connections per host |
| `-client-compression` | `true` | Enable lz4 compression |

### Datacenter

The `-datacenter` option sets the **local datacenter** for the ScyllaDB Java Driver's DC-aware load balancing policy. The driver will prefer replicas in the specified datacenter and only fall back to remote datacenters when local replicas are unavailable.

To find your datacenter name, run on the ScyllaDB node:
```sql
SELECT data_center FROM system.local;
```
Or via `nodetool`:
```bash
nodetool status | grep Datacenter
```

Example — connecting to a cluster with a datacenter named `us-east`:
```bash
java -jar target/scylla-bench-java.jar \
  -mode write -workload sequential \
  -nodes scylla-node1,scylla-node2,scylla-node3 \
  -datacenter us-east \
  -consistency-level local_quorum
```

> **Tip:** When using `-consistency-level local_quorum` or `local_one`, the `-datacenter` value must exactly match the datacenter name in the cluster, otherwise the driver will throw a `NoNodeAvailableException`.

### TLS
| Option | Default | Description |
|--------|---------|-------------|
| `-tls` | `false` | Enable TLS |
| `-tls-host-verification` | `false` | Verify server certificate |
| `-tls-ca-cert-file` | | Path to CA cert (PEM) |
| `-tls-client-cert-file` | | Path to client cert (PEM) |
| `-tls-client-key-file` | | Path to client key (PEM) |

### Benchmark Control
| Option | Default | Description |
|--------|---------|-------------|
| `-consistency-level` | `quorum` | CQL consistency level (see below) |
| `-replication-factor` | `1` | Keyspace replication factor |
| `-concurrency` | `16` | Parallel worker threads |
| `-max-rate` | `0` | Max ops/s (0=unlimited) |
| `-duration` | `0` | Test duration (0=unlimited, e.g. `30s`, `5m`, `1h`) |
| `-iterations` | `1` | Workload iterations (sequential/scan only; 0=unlimited) |
| `-timeout` | `10s` | Base request timeout (used when no per-type timeout is set) |
| `-read-timeout` | `10s` | Timeout for SELECT queries |
| `-write-timeout` | `5s` | Timeout for INSERT/UPDATE/DELETE queries |
| `-cas-timeout` | `2s` | Timeout for lightweight transaction (LWT/CAS) queries |
| `-lwt` | `no` | Enable lightweight transactions (`yes`/`no`) |

### Consistency Level

Controls how many replicas must acknowledge a read or write before it is considered successful.

| Value | Description |
|-------|-------------|
| `any` | Any replica (writes only) |
| `one` | One replica |
| `two` | Two replicas |
| `three` | Three replicas |
| `quorum` | Majority of replicas across all DCs *(default)* |
| `all` | All replicas |
| `local_quorum` | Majority of replicas in the local DC |
| `each_quorum` | Majority of replicas in each DC |
| `local_one` | One replica in the local DC |

Example:
```bash
java -jar target/scylla-bench-java.jar \
  -mode write -workload sequential \
  -nodes scylla-node1,scylla-node2 \
  -datacenter us-east \
  -consistency-level local_quorum
```

> **Note:** `local_quorum` and `local_one` require `-datacenter` to be set to the correct datacenter name.
> For LWT (`-lwt yes`), the serial consistency is automatically set to `LOCAL_SERIAL` and cannot be changed via CLI.

### Schema
| Option | Default | Description |
|--------|---------|-------------|
| `-keyspace` | `scylla_bench` | Keyspace name |
| `-table` | `test` | Table name |
| `-truncate-table` | `false` | Truncate table before running |

### Data Shape
| Option | Default | Description |
|--------|---------|-------------|
| `-partition-count` | `10000` | Number of partitions |
| `-partition-offset` | `0` | Starting partition key offset |
| `-clustering-row-count` | `100` | Clustering rows per partition |
| `-clustering-row-size` | `fixed:4` | Row size: `fixed:N` or `uniform:MIN..MAX` |
| `-rows-per-request` | `1` | Rows per request (>1 = unlogged batch) |

### Read Options
| Option | Default | Description |
|--------|---------|-------------|
| `-page-size` | `1000` | Result page size |
| `-provide-upper-bound` | `false` | Add `AND ck < ?` upper bound |
| `-in-restriction` | `false` | Use `IN (...)` restriction |
| `-no-lower-bound` | `false` | Omit `AND ck >= ?` lower bound |
| `-select-order-by` | `none` | ORDER BY: `none`, `asc`, `desc`, or comma-separated list |
| `-bypass-cache` | `false` | Add `BYPASS CACHE` clause |

### Scan
| Option | Default | Description |
|--------|---------|-------------|
| `-range-count` | `1` | Token space sub-ranges (recommended: nodes × cores × 300) |

### Timeseries
| Option | Default | Description |
|--------|---------|-------------|
| `-write-rate` | `0` | Per-second write rate (needed for timeseries reads) |
| `-start-timestamp` | `0` | Write start timestamp ns (needed for timeseries reads) |
| `-distribution` | `uniform` | Key distribution: `uniform`, `hnormal` |

### Latency
| Option | Default | Description |
|--------|---------|-------------|
| `-measure-latency` | `true` | Measure and report latency |
| `-latency-type` | `raw` | Display: `raw` or `fixed-coordinated-omission` |
| `-hdr-latency-file` | | HDR histogram log file |
| `-hdr-latency-units` | `ns` | Histogram time units: `ns`, `us`, `ms` |
| `-hdr-latency-sig` | `3` | HDR significant figures (1-5) |

### Errors & Retries
| Option | Default | Description |
|--------|---------|-------------|
| `-retry-number` | `10` | Retries per request |
| `-retry-interval` | `80ms,1s` | Retry backoff: single value or `min,max` |
| `-retry-handler` | `sb` | `sb` (application-level) or `driver` |
| `-error-at-row-limit` | `0` | Max consecutive errors per thread (0=unlimited) |
| `-error-limit` | `0` | Total error limit (0=unlimited) |

### Other
| Option | Default | Description |
|--------|---------|-------------|
| `-validate-data` | `false` | Write and verify data integrity |
| `--version` / `-V` | | Show version info and driver version |
| `--help` / `-h` | | Show help |

## User Mode

User mode lets you run stress tests against your own schema using a custom YAML profile — similar to the `cassandra-stress user` mode. Instead of the built-in `pk`/`ck`/`v` table, the driver reads from your existing (or auto-created) table and executes parameterised queries you define.

### Quick Example

```bash
java -jar target/scylla-bench-java.jar \
  -mode user \
  -profile my_profile.yaml \
  -nodes 127.0.0.1 \
  -concurrency 32
```

### YAML Profile Format

```yaml
# ---------------------------------------------------------------------------
# Keyspace (required)
# ---------------------------------------------------------------------------
keyspace: my_ks

# DDL to create the keyspace (optional — omit if the keyspace already exists)
keyspace_definition: |
  CREATE KEYSPACE IF NOT EXISTS my_ks
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

# ---------------------------------------------------------------------------
# Table (required)
# ---------------------------------------------------------------------------
table: my_table

# DDL to create the table (optional — omit if the table already exists)
table_definition: |
  CREATE TABLE IF NOT EXISTS my_ks.my_table (
    user_id uuid,
    ts      timeuuid,
    payload text,
    PRIMARY KEY (user_id, ts)
  ) WITH CLUSTERING ORDER BY (ts DESC);

# Extra DDL statements executed after keyspace/table creation (optional)
extra_definitions:
  - "CREATE INDEX IF NOT EXISTS ON my_ks.my_table (payload);"

# ---------------------------------------------------------------------------
# Column generators (optional)
# Describe how values are generated for each column when auto-INSERT is used.
# ---------------------------------------------------------------------------
columnspec:
  - name: user_id
    population: uniform:1..10000    # draw from 10 000 distinct UUIDs
  - name: ts
    population: fixed:1             # single seed (timeuuid auto-advances)
  - name: payload
    size:       uniform:64..256     # random blob/text length in bytes
    population: uniform:1..100000

# ---------------------------------------------------------------------------
# INSERT tuning (optional)
# ---------------------------------------------------------------------------
insert:
  partitions:   fixed:1            # partitions touched per batch
  batchtype:    UNLOGGED           # UNLOGGED | LOGGED | COUNTER

# ---------------------------------------------------------------------------
# Queries (at least one recommended; omit to run auto-generated INSERT only)
# ---------------------------------------------------------------------------
queries:
  insert_row:
    cql: "INSERT INTO my_ks.my_table (user_id, ts, payload) VALUES (?, now(), ?)"
    fields: samerow      # all bind values come from the same generated row
    ratio: 5             # weight relative to other queries

  read_row:
    cql: "SELECT * FROM my_ks.my_table WHERE user_id = ? LIMIT 10"
    fields: samerow
    ratio: 3

  delete_row:
    cql: "DELETE FROM my_ks.my_table WHERE user_id = ? AND ts = ?"
    fields: samerow
    ratio: 1
```

### Distribution Syntax

Distributions control how values (sizes, population seeds) are picked at random.

| Syntax | Description |
|--------|-------------|
| `fixed:N` | Always returns N |
| `uniform:MIN..MAX` | Uniformly random between MIN and MAX |
| `gaussian:MIN..MAX` | Gaussian (normal) distribution, μ at midpoint, 3σ covers the range |
| `seq:MIN..MAX` | Sequential counter wrapping between MIN and MAX |

Magnitude suffixes: `k` (×1 000), `m` (×1 000 000), `b` (×1 000 000 000).  
Example: `uniform:1k..1m` → random between 1 000 and 1 000 000.

### How `fields` Works

| Value | Meaning |
|-------|---------|
| `samerow` | All `?` placeholders are bound from one freshly-generated row |
| `multirow` | Each `?` placeholder gets an independently generated value |

### Operation Selection

Each query's `ratio` is its relative weight. If `ratio` is omitted it defaults to 1. Operations are chosen proportionally — e.g. ratios `5 : 3 : 1` in the example above mean 55 % inserts, 33 % reads, 11 % deletes.

If no queries are defined, user mode falls back to a single auto-generated `INSERT INTO … VALUES (…)` covering all columns.

## Schema

The tool creates the following schema automatically:

**Regular table** (write, read, mixed modes):
```sql
CREATE TABLE IF NOT EXISTS scylla_bench.test (
    pk bigint,
    ck bigint,
    v blob,
    PRIMARY KEY(pk, ck)
) WITH compression = { }
```

**Counter table** (counter_update, counter_read modes):
```sql
CREATE TABLE IF NOT EXISTS scylla_bench.test_counters (
    pk bigint,
    ck bigint,
    c1 counter, c2 counter, c3 counter, c4 counter, c5 counter,
    PRIMARY KEY(pk, ck)
) WITH compression = { }
```

## Key Differences from scylla-bench (Go)

| Feature | scylla-bench (Go) | scylla-bench-java |
|---------|------------------|-------------------|
| Language | Go | Java 11+ |
| Driver | github.com/scylladb/gocql | com.scylladb:java-driver-core |
| Concurrency | Goroutines | OS threads |
| CLI parsing | `flag` package | picocli |
| Build tool | `go build` / `go install` | Maven |
| Driver version | In go.mod | `-Dscylla.driver.version` at build time |
| Cloud config | `-cloud-config-path` | Not yet implemented |
| Host pool policy | `hostpool` | DC-aware (driver default) |

## Contributing

This project follows the same spirit as [scylla-bench](https://github.com/scylladb/scylla-bench).
Contributions, bug reports, and feature requests are welcome.
