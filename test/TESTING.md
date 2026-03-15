# ScyllaDB Cluster Testing Guide

This document provides instructions for testing scylla-bench-java against a live ScyllaDB cluster.

## Quick Start (No Java Required)

The easiest way to run tests is using the wrapper scripts — **no Java installation needed**! They automatically use Java if available or fall back to Docker.

**Windows:**
```powershell
.\scylla-bench.ps1 -mode write -workload sequential -nodes 127.0.0.1 -duration 10s
```

**Linux/macOS:**
```bash
./scylla-bench.sh -mode write -workload sequential -nodes 127.0.0.1 -duration 10s
```

See the [Manual Testing](#manual-testing) section below for detailed examples.

## Prerequisites

### 1. ScyllaDB Cluster

You need a running ScyllaDB cluster. Options:

#### Option A: Local Docker Container (Quickest)
```bash
# Start a single-node ScyllaDB cluster
docker run --name scylla -d -p 9042:9042 scylladb/scylla:latest

# Wait for ScyllaDB to be ready (~30 seconds)
docker logs scylla -f
# Wait until you see "ScyllaDB is now available"
```

#### Option B: ScyllaDB Cloud
1. Sign up at https://cloud.scylladb.com/
2. Create a free cluster
3. Get the connection details (nodes, datacenter)

#### Option C: Local Installation
```bash
# Ubuntu/Debian
curl -sSf get.scylladb.com/server | sudo bash
sudo systemctl start scylla-server
```

### 2. Choose Your Method

**Option A: Wrapper Scripts (Recommended — No Java Required)**

- Use `scylla-bench.sh` (Linux/macOS) or `scylla-bench.ps1` (Windows)
- Automatically detects Java 21+ or uses Docker
- No manual setup needed

**Option B: Direct JAR (Requires Java 21+ and Maven)**

If you prefer to build and run the JAR directly:

**Verify Java 21:**
```bash
java -version  # Should show: openjdk version "21.0.2" or similar
```

If not Java 21, set `JAVA_HOME`:

**Windows:**
```powershell
$env:JAVA_HOME = "C:\Users\<user>\.jdks\openjdk-21.0.2"
```

**Linux/macOS:**
```bash
export JAVA_HOME=/path/to/jdk-21
export PATH=$JAVA_HOME/bin:$PATH
```

**Build the JAR:**
```bash
cd <path>/scylla-bench-java
mvn clean package -DskipTests
```

## Running the Tests

### Automated Test Suite

Use the provided PowerShell script to test all workload modes:

**Using wrapper script (no Java required):**
```powershell
# From the test directory
.\test-scylla-cluster.ps1 -ScyllaNodes "127.0.0.1" -UseWrapper

# Test against remote cluster
.\test-scylla-cluster.ps1 -ScyllaNodes "node1.example.com,node2.example.com" -Datacenter "us-east" -UseWrapper
```

**Using direct JAR (requires Java 21 and Maven build):**
```powershell
# Test against local ScyllaDB
.\test-scylla-cluster.ps1 -ScyllaNodes "127.0.0.1"

# Test against remote cluster
.\test-scylla-cluster.ps1 -ScyllaNodes "node1.example.com,node2.example.com" -Datacenter "us-east"

# Longer test duration (30 seconds per test)
.\test-scylla-cluster.ps1 -ScyllaNodes "127.0.0.1" -TestDuration 30
```

The script tests:
1. ✓ Sequential Write
2. ✓ Uniform Write
3. ✓ Timeseries Write
4. ✓ Sequential Read
5. ✓ Uniform Read (with latency measurement)
6. ✓ Timeseries Read
7. ✓ Mixed Read/Write
8. ✓ Counter Update
9. ✓ Counter Read
10. ✓ Full Table Scan
11. ✓ Data Validation (Write + Read)

### Manual Testing

Choose either wrapper scripts (A) or direct JAR (B):

#### 1. Test Basic Connectivity

**Option A: Using Wrapper Scripts (No Java Required)**

**Windows:**
```powershell
.\scylla-bench.ps1 -mode write -workload sequential -nodes 127.0.0.1 -duration 5s -partition-count 100
```

**Linux/macOS:**
```bash
./scylla-bench.sh -mode write -workload sequential -nodes 127.0.0.1 -duration 5s -partition-count 100
```

**Option B: Using Direct JAR**
```bash
java -jar target/scylla-bench-java.jar -mode write -workload sequential -nodes 127.0.0.1 -duration 5s -partition-count 100
```

Expected output:
```
Creating keyspace and table...
Running write benchmark...
Results after 5s:
  Operations: 1234 total, 246.8 op/s
  ...
```

#### 2. Test Each Workload Mode

Examples below use wrapper scripts. For direct JAR, replace `./scylla-bench.sh` with `java -jar target/scylla-bench-java.jar`.

**Sequential Write:**
```bash
./scylla-bench.sh -mode write -workload sequential -nodes 127.0.0.1 -duration 10s
```

**Uniform Write:**
```bash
./scylla-bench.sh -mode write -workload uniform -nodes 127.0.0.1 -duration 10s
```

**Timeseries Write:**
```bash
./scylla-bench.sh -mode write -workload timeseries -nodes 127.0.0.1 -duration 10s
```

**Sequential Read:**
```bash
./scylla-bench.sh -mode read -workload sequential -nodes 127.0.0.1 -duration 10s
```

**Uniform Read with Latency:**
```bash
./scylla-bench.sh -mode read -workload uniform -nodes 127.0.0.1 -duration 10s -measure-latency
```

**Mixed Read/Write:**
```bash
./scylla-bench.sh -mode mixed -workload uniform -nodes 127.0.0.1 -duration 10s
```

**Counter Operations:**
```bash
# Counter update
./scylla-bench.sh -mode counter_update -workload sequential -nodes 127.0.0.1 -duration 10s

# Counter read
./scylla-bench.sh -mode counter_read -workload sequential -nodes 127.0.0.1 -duration 10s
```

**Full Table Scan:**
```bash
./scylla-bench.sh -mode scan -nodes 127.0.0.1 -range-count 100
```

**Data Validation:**
```bash
# Write data with validation
./scylla-bench.sh -mode write -workload sequential -nodes 127.0.0.1 -partition-count 100 -validate-data -duration 5s

# Read and validate data
./scylla-bench.sh -mode read -workload sequential -nodes 127.0.0.1 -partition-count 100 -validate-data -duration 5s
```

**Using Specific Driver Version:**
```bash
# Linux/macOS
DRIVER_VERSION=4.18.0.0 ./scylla-bench.sh -mode write -workload sequential -nodes 127.0.0.1

# Windows PowerShell
$env:DRIVER_VERSION="4.18.0.0"
.\scylla-bench.ps1 -mode write -workload sequential -nodes 127.0.0.1
```

## Verification Checklist

After running tests, verify:

- [ ] All workload modes execute without errors
- [ ] Operations complete successfully (ops/s > 0)
- [ ] No Java exceptions or stack traces
- [ ] Latency measurements are reported (when `-measure-latency` is used)
- [ ] Data validation passes (when `-validate-data` is used)
- [ ] Performance is comparable to Java 11 baseline (if you have historical data)

## Troubleshooting

### Connection Refused
```
All host(s) tried for query failed
```

**Solution:** Verify ScyllaDB is running and accessible:
```powershell
# Test connectivity
Test-NetConnection -ComputerName 127.0.0.1 -Port 9042

# Check ScyllaDB logs (if using Docker)
docker logs scylla | Select-Object -Last 50
```

### Wrong Datacenter
```
No node was available to execute the query
```

**Solution:** Check your datacenter name:
```bash
# Docker
docker exec scylla cqlsh -e "SELECT data_center FROM system.local;"

# Local installation
cqlsh -e "SELECT data_center FROM system.local;"
```

Update the `-datacenter` parameter accordingly.

### Java Version Error
```
UnsupportedClassVersionError: class file version 65.0
```

**Solution:** Use the wrapper scripts instead (they handle Java automatically), or ensure you're using Java 21:
```bash
java -version  # Should show 21+
```

If using direct JAR, set `JAVA_HOME` to Java 21 installation.

### Out of Memory
```
java.lang.OutOfMemoryError: Java heap space
```

**Solution:** Increase heap size:
```powershell
$env:JAVA_OPTS="-Xmx4g -Xms2g"
java $env:JAVA_OPTS -jar target\scylla-bench-java.jar ...
```

## Performance Comparison (Java 11 vs Java 21)

If you have historical Java 11 baseline data, compare:

| Metric | Java 11 | Java 21 | Change |
|--------|---------|---------|--------|
| Sequential Write (ops/s) | | | |
| Uniform Read (ops/s) | | | |
| P50 Latency (µs) | | | |
| P99 Latency (µs) | | | |
| Memory Usage (MB) | | | |

### Collecting Metrics

```powershell
# Run with latency measurement
java -jar target\scylla-bench-java.jar `
  -mode read -workload uniform `
  -nodes 127.0.0.1 `
  -duration 60s `
  -measure-latency `
  -hdr-latency-file latency.hdr

# Results will include percentile latencies
```

## Expected Results

All tests should **PASS** with:
- No exceptions or errors
- Positive throughput (ops/s > 0)
- Latency measurements (when enabled)
- Data validation success (when enabled)

If any test **FAILS**, investigate:
1. Check ScyllaDB cluster health
2. Review error messages in output
3. Check ScyllaDB logs for server-side issues
4. Verify network connectivity
5. Ensure sufficient resources (CPU, memory, disk)

## Next Steps After Successful Testing

1. **Document Results:** Save test output for reference
2. **Performance Baseline:** Record metrics for future comparisons
3. **Update CI/CD:** Configure automated testing in pipelines
4. **Production Validation:** Test in staging before production deployment
5. **Monitor Java 21 Features:** Consider using virtual threads for improved concurrency

## Additional Resources

- [ScyllaDB Documentation](https://docs.scylladb.com/)
- [ScyllaDB Java Driver](https://github.com/scylladb/java-driver)
- [Java 21 Release Notes](https://openjdk.org/projects/jdk/21/)
- [scylla-bench Original](https://github.com/scylladb/scylla-bench)

### Linux and macOS

For Linux and macOS users, use the bash versions of the scripts:

**Incremental Tuning:**

```bash
# Make script executable
chmod +x incremental-tuning.sh

# Run tuning test
./incremental-tuning.sh \
  --nodes node1,node2,node3 \
  --username scylla \
  --password mypass \
  --datacenter AWS_EU_WEST_2 \
  --port 19042

# Or with environment variables
export JAVA_HOME=/path/to/jdk-21
./incremental-tuning.sh --nodes 127.0.0.1
```

**Parallel Instance Launcher:**

```bash
# Make script executable
chmod +x parallel-launcher.sh

# Run 6 instances in parallel
./parallel-launcher.sh \
  --nodes node1,node2,node3 \
  --username scylla \
  --password mypass \
  --datacenter AWS_EU_WEST_2 \
  --instances 6 \
  --duration 10

# Customize resources
./parallel-launcher.sh \
  --nodes 127.0.0.1 \
  --instances 8 \
  --concurrency 1500 \
  --connections 20
```

**Script Options:**

Both scripts support these common options:

- `--nodes` - Comma-separated node list (required)
- `--username` - CQL username
- `--password` - CQL password
- `--port` - CQL port (default: 9042)
- `--datacenter` - Datacenter name (default: datacenter1)
- `--jar` - JAR file path (default: target/scylla-bench-java.jar)
- `--java` - Java executable path (default: \/java)
- `--help` - Show help message

**parallel-launcher.sh specific options:**

- `--instances` - Number of parallel instances (default: 6)
- `--duration` - Duration in minutes (default: 10)
- `--concurrency` - Concurrency per instance (default: 1200)
- `--connections` - Connections per instance (default: 16)

**incremental-tuning.sh specific options:**

- `--duration` - Test duration in seconds per concurrency level (default: 30)

**Example Output (Linux/macOS):**

```bash
$ ./parallel-launcher.sh --nodes 192.0.2.1 --username scylla --password *** --instances 4

=====================================
Parallel Benchmark Launcher
=====================================

Configuration:
  Instances: 4
  Nodes: 18.133.174.82
  Duration: 10m per instance
  Total Concurrency: 4800
  Total Connections: 64

Memory Requirements:
  Per instance: ~8GB heap + 2GB overhead
  Total estimated: ~40GB

Continue? (y/n) y

Starting 4 benchmark instances...

Instance 1 completed
Instance 2 completed
Instance 3 completed
Instance 4 completed

=====================================
Summary
=====================================

Instance   Ops/s      Log File
---------- ---------- --------------------
1          18234      bench-1.log
2          17891      bench-2.log
3          18456      bench-3.log
4          18012      bench-4.log

Total Throughput: ~72593 ops/s

✓ Parallel benchmark complete!
```

### Cross-Platform Notes

**Java Path:**
- **Windows:** Use full path or set `\C:\Users\allen\.jdks\openjdk-21.0.2`
- **Linux/macOS:** Use `export JAVA_HOME=/path/to/jdk-21` or let scripts use system Java

**Memory:**
- Scripts automatically calculate heap based on concurrency
- Single instance: 4-8GB recommended
- 6 parallel instances: 48-60GB system RAM recommended

**CPU Cores:**
- Recommended: 1 instance per 2-4 CPU cores
- Example: 16-core machine → 4-8 instances optimal
