# Quick Test Commands for Your ScyllaDB Cloud Cluster

## Your Cluster Details
- **Host:** node-1.aws-eu-west-2.8ae3b2e1d686d86d08db.clusters.scylla.cloud
- **Username:** scylla
- **Password:** e5HNu01GLzOArfQ
- **Region:** AWS EU West 2

## Step 1: Determine Datacenter Name

First, find your datacenter name:

```powershell
# Using cqlsh (if available)
cqlsh -u scylla -p e5HNu01GLzOArfQ node-1.aws-eu-west-2.8ae3b2e1d686d86d08db.clusters.scylla.cloud -e "SELECT data_center FROM system.local;"
```

Or check the ScyllaDB Cloud console - it's usually something like `AWS_EU_WEST_2` or `eu-west-2`.

## Step 2: Quick Connectivity Test

Test basic connectivity with a short write benchmark:

```powershell
$env:JAVA_HOME = "C:\Users\allen\.jdks\openjdk-21.0.2"

java -jar target\scylla-bench-java.jar `
  -mode write `
  -workload sequential `
  -nodes node-1.aws-eu-west-2.8ae3b2e1d686d86d08db.clusters.scylla.cloud `
  -username scylla `
  -password e5HNu01GLzOArfQ `
  -datacenter AWS_EU_WEST_2 `
  -duration 10s `
  -partition-count 1000 `
  -clustering-row-count 10 `
  -concurrency 8
```

**Note:** Update `-datacenter` based on Step 1 result.

## Step 3: Run Full Test Suite

Once connectivity is confirmed, test all workload modes:

```powershell
# Set Java 21
$env:JAVA_HOME = "C:\Users\allen\.jdks\openjdk-21.0.2"

# Test 1: Sequential Write
Write-Host "Test 1: Sequential Write" -ForegroundColor Cyan
java -jar target\scylla-bench-java.jar `
  -mode write -workload sequential `
  -nodes node-1.aws-eu-west-2.8ae3b2e1d686d86d08db.clusters.scylla.cloud `
  -username scylla -password e5HNu01GLzOArfQ `
  -datacenter AWS_EU_WEST_2 `
  -duration 10s -partition-count 1000

# Test 2: Uniform Read with Latency
Write-Host "`nTest 2: Uniform Read with Latency" -ForegroundColor Cyan
java -jar target\scylla-bench-java.jar `
  -mode read -workload uniform `
  -nodes node-1.aws-eu-west-2.8ae3b2e1d686d86d08db.clusters.scylla.cloud `
  -username scylla -password e5HNu01GLzOArfQ `
  -datacenter AWS_EU_WEST_2 `
  -duration 10s -measure-latency

# Test 3: Mixed Read/Write
Write-Host "`nTest 3: Mixed Read/Write" -ForegroundColor Cyan
java -jar target\scylla-bench-java.jar `
  -mode mixed -workload uniform `
  -nodes node-1.aws-eu-west-2.8ae3b2e1d686d86d08db.clusters.scylla.cloud `
  -username scylla -password e5HNu01GLzOArfQ `
  -datacenter AWS_EU_WEST_2 `
  -duration 10s -concurrency 16

# Test 4: Timeseries Write
Write-Host "`nTest 4: Timeseries Write" -ForegroundColor Cyan
java -jar target\scylla-bench-java.jar `
  -mode write -workload timeseries `
  -nodes node-1.aws-eu-west-2.8ae3b2e1d686d86d08db.clusters.scylla.cloud `
  -username scylla -password e5HNu01GLzOArfQ `
  -datacenter AWS_EU_WEST_2 `
  -duration 10s -max-rate 1000

# Test 5: Data Validation
Write-Host "`nTest 5: Data Validation (Write + Read)" -ForegroundColor Cyan
java -jar target\scylla-bench-java.jar `
  -mode write -workload sequential `
  -nodes node-1.aws-eu-west-2.8ae3b2e1d686d86d08db.clusters.scylla.cloud `
  -username scylla -password e5HNu01GLzOArfQ `
  -datacenter AWS_EU_WEST_2 `
  -partition-count 100 -validate-data -duration 5s

java -jar target\scylla-bench-java.jar `
  -mode read -workload sequential `
  -nodes node-1.aws-eu-west-2.8ae3b2e1d686d86d08db.clusters.scylla.cloud `
  -username scylla -password e5HNu01GLzOArfQ `
  -datacenter AWS_EU_WEST_2 `
  -partition-count 100 -validate-data -duration 5s

Write-Host "`n✓ All tests completed!" -ForegroundColor Green
```

## Step 4: Automated Test (Recommended)

Use the test script with your cluster:

```powershell
.\test-scylla-cluster.ps1 `
  -ScyllaNodes "node-1.aws-eu-west-2.8ae3b2e1d686d86d08db.clusters.scylla.cloud" `
  -Datacenter "AWS_EU_WEST_2" `
  -TestDuration 10
```

**Note:** The script needs to be updated to support authentication. Add these parameters after line 17 in `test-scylla-cluster.ps1`:

```powershell
[Parameter(Mandatory=$false)]
[string]$Username = "",

[Parameter(Mandatory=$false)]
[string]$Password = ""
```

And add to each test's `$args`:

```powershell
if ($Username) { $args += "-username", $Username }
if ($Password) { $args += "-password", $Password }
```

Then run:

```powershell
.\test-scylla-cluster.ps1 `
  -ScyllaNodes "node-1.aws-eu-west-2.8ae3b2e1d686d86d08db.clusters.scylla.cloud" `
  -Datacenter "AWS_EU_WEST_2" `
  -Username "scylla" `
  -Password "e5HNu01GLzOArfQ" `
  -TestDuration 10
```

## Common Datacenter Names for ScyllaDB Cloud

Try these if you're unsure:
- `AWS_EU_WEST_2` (most likely for eu-west-2)
- `eu-west-2`
- `datacenter1`

If you get "No node was available" error, try a different datacenter name.

## Expected Output

Successful test output should show:

```
Creating keyspace and table...
Running write benchmark...
Results after 10s:
  Operations: 12345 total, 1234.5 op/s
  Rows: 123450 total, 12345.0 rows/s
  ...
```

## Troubleshooting

### Authentication Failed
If you see authentication errors, verify credentials in ScyllaDB Cloud console.

### Connection Timeout
ScyllaDB Cloud clusters may have IP allowlist. Check your cluster's security settings and add your IP address.

### Wrong Datacenter
Try querying the datacenter name or check different variations above.
