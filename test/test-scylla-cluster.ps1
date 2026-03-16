#!/usr/bin/env pwsh
# ScyllaDB Cluster Verification Script
# Tests all workload modes to verify scylla-bench-java works correctly

param(
    [Parameter(Mandatory=$true)]
    [string]$ScyllaNodes,
    
    [Parameter(Mandatory=$false)]
    [string]$Datacenter = "datacenter1",
    
    [Parameter(Mandatory=$false)]
    [int]$TestDuration = 10,
    
    [Parameter(Mandatory=$false)]
    [string]$JarPath = "",
    
    [Parameter(Mandatory=$false)]
    [switch]$UseWrapper = $false
)

$ErrorActionPreference = "Stop"

# Get script directory and project root
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir

# Set default JAR path if not provided
if ([string]::IsNullOrEmpty($JarPath)) {
    $JarPath = Join-Path $projectRoot "target\scylla-bench-java.jar"
}

Write-Host "====================================" -ForegroundColor Cyan
Write-Host "ScyllaDB Verification Tests" -ForegroundColor Cyan
Write-Host "====================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Configuration:" -ForegroundColor Yellow
Write-Host "  ScyllaDB Nodes: $ScyllaNodes"
Write-Host "  Datacenter: $Datacenter"
Write-Host "  Test Duration: ${TestDuration}s per test"
Write-Host ""

# Detect if wrapper script is available
$wrapperPath = Join-Path $projectRoot "scylla-bench.ps1"
$useWrapperScript = $UseWrapper -or (Test-Path $wrapperPath)

if ($useWrapperScript -and (Test-Path $wrapperPath)) {
    Write-Host "Using wrapper script (no Java required): $wrapperPath" -ForegroundColor Green
    $runMethod = "wrapper"
} else {
    Write-Host "Using direct JAR: $JarPath" -ForegroundColor Yellow
    $runMethod = "jar"
    
    # Verify JAR exists
    if (-not (Test-Path $JarPath)) {
        Write-Host "ERROR: JAR file not found at $JarPath" -ForegroundColor Red
        Write-Host "Run 'mvn clean package -DskipTests' first" -ForegroundColor Yellow
        exit 1
    }
    
    # Verify Java 21
    Write-Host "Checking Java version..." -ForegroundColor Yellow
    try {
        $javaVersion = java -version 2>&1 | Select-String "version" | Out-String
        if ($javaVersion -notmatch '"21\.') {
            Write-Host "WARNING: Not running Java 21!" -ForegroundColor Red
            Write-Host $javaVersion
            Write-Host "Consider using the wrapper script instead (add -UseWrapper flag)" -ForegroundColor Yellow
        } else {
            Write-Host "✓ Java 21 detected" -ForegroundColor Green
        }
    } catch {
        Write-Host "ERROR: Java not found. Use -UseWrapper flag to use wrapper script instead" -ForegroundColor Red
        exit 1
    }
}
Write-Host ""

$testResults = @()
$testNumber = 0

function Run-BenchmarkTest {
    param(
        [string]$TestName,
        [string]$Mode,
        [string]$Workload,
        [hashtable]$ExtraArgs = @{}
    )
    
    $script:testNumber++
    Write-Host "[$script:testNumber] Running: $TestName" -ForegroundColor Cyan
    Write-Host "    Mode: $Mode | Workload: $Workload" -ForegroundColor Gray
    
    if ($script:runMethod -eq "wrapper") {
        # Use wrapper script
        $benchArgs = @(
            "-mode", $Mode,
            "-nodes", $ScyllaNodes,
            "-datacenter", $Datacenter,
            "-duration", "${TestDuration}s",
            "-concurrency", "8",
            "-partition-count", "1000",
            "-clustering-row-count", "10"
        )
        
        if ($Workload) {
            $benchArgs += "-workload", $Workload
        }
        
        foreach ($key in $ExtraArgs.Keys) {
            $benchArgs += $key, $ExtraArgs[$key]
        }
        
        Write-Host "    Command: $wrapperPath $($benchArgs -join ' ')" -ForegroundColor DarkGray
        
        $startTime = Get-Date
        try {
            $output = & $wrapperPath @benchArgs 2>&1
            $exitCode = $LASTEXITCODE
            $duration = (Get-Date) - $startTime
            
            if ($exitCode -eq 0) {
                Write-Host "    ✓ SUCCESS (${duration}s)" -ForegroundColor Green
                $script:testResults += [PSCustomObject]@{
                    Test = $TestName
                    Mode = $Mode
                    Workload = $Workload
                    Status = "PASS"
                    Duration = [math]::Round($duration.TotalSeconds, 1)
                    Error = ""
                }
            } else {
                Write-Host "    ✗ FAILED (exit code: $exitCode)" -ForegroundColor Red
                $errorMsg = ($output | Select-String "Exception|Error" | Select-Object -First 3) -join "; "
                $script:testResults += [PSCustomObject]@{
                    Test = $TestName
                    Mode = $Mode
                    Workload = $Workload
                    Status = "FAIL"
                    Duration = [math]::Round($duration.TotalSeconds, 1)
                    Error = $errorMsg
                }
            }
        } catch {
            Write-Host "    ✗ EXCEPTION: $_" -ForegroundColor Red
            $script:testResults += [PSCustomObject]@{
                Test = $TestName
                Mode = $Mode
                Workload = $Workload
                Status = "FAIL"
                Duration = 0
                Error = $_.Exception.Message
            }
        }
    } else {
        # Use direct JAR
        $args = @(
            "-jar", $JarPath,
            "-mode", $Mode,
            "-nodes", $ScyllaNodes,
            "-datacenter", $Datacenter,
            "-duration", "${TestDuration}s",
            "-concurrency", "8",
            "-partition-count", "1000",
            "-clustering-row-count", "10"
        )
        
        if ($Workload) {
            $args += "-workload", $Workload
        }
        
        foreach ($key in $ExtraArgs.Keys) {
            $args += $key, $ExtraArgs[$key]
        }
        
        Write-Host "    Command: java $($args -join ' ')" -ForegroundColor DarkGray
        
        $startTime = Get-Date
        try {
            $output = & java $args 2>&1
            $exitCode = $LASTEXITCODE
            $duration = (Get-Date) - $startTime
            
            if ($exitCode -eq 0) {
                Write-Host "    ✓ SUCCESS (${duration}s)" -ForegroundColor Green
                $script:testResults += [PSCustomObject]@{
                    Test = $TestName
                    Mode = $Mode
                    Workload = $Workload
                    Status = "PASS"
                    Duration = [math]::Round($duration.TotalSeconds, 1)
                    Error = ""
                }
            } else {
                Write-Host "    ✗ FAILED (exit code: $exitCode)" -ForegroundColor Red
                $errorMsg = ($output | Select-String "Exception|Error" | Select-Object -First 3) -join "; "
                $script:testResults += [PSCustomObject]@{
                    Test = $TestName
                    Mode = $Mode
                    Workload = $Workload
                    Status = "FAIL"
                    Duration = [math]::Round($duration.TotalSeconds, 1)
                    Error = $errorMsg
                }
            }
        } catch {
            Write-Host "    ✗ EXCEPTION: $_" -ForegroundColor Red
            $script:testResults += [PSCustomObject]@{
                Test = $TestName
                Mode = $Mode
                Workload = $Workload
                Status = "FAIL"
                Duration = 0
                Error = $_.Exception.Message
            }
        }
    }
    Write-Host ""
}

# Test Suite
Write-Host "Starting benchmark tests..." -ForegroundColor Yellow
Write-Host ""

# 1. Sequential Write
Run-BenchmarkTest -TestName "Sequential Write" -Mode "write" -Workload "sequential"

# 2. Uniform Write
Run-BenchmarkTest -TestName "Uniform Write" -Mode "write" -Workload "uniform"

# 3. Timeseries Write
Run-BenchmarkTest -TestName "Timeseries Write" -Mode "write" -Workload "timeseries"

# 4. Sequential Read
Run-BenchmarkTest -TestName "Sequential Read" -Mode "read" -Workload "sequential"

# 5. Uniform Read
Run-BenchmarkTest -TestName "Uniform Read" -Mode "read" -Workload "uniform" -ExtraArgs @{"-measure-latency" = ""}

# 6. Timeseries Read
Run-BenchmarkTest -TestName "Timeseries Read" -Mode "read" -Workload "timeseries"

# 7. Mixed Workload
Run-BenchmarkTest -TestName "Mixed Read/Write" -Mode "mixed" -Workload "uniform"

# 8. Counter Update
Run-BenchmarkTest -TestName "Counter Update" -Mode "counter_update" -Workload "sequential"

# 9. Counter Read
Run-BenchmarkTest -TestName "Counter Read" -Mode "counter_read" -Workload "sequential"

# 10. Scan
Run-BenchmarkTest -TestName "Full Table Scan" -Mode "scan" -Workload "" -ExtraArgs @{"-range-count" = "100"}

# 11. Data Validation (Write + Read)
Write-Host "[11] Running: Data Validation Test" -ForegroundColor Cyan
Write-Host "    Writing with validation..." -ForegroundColor Gray

if ($runMethod -eq "wrapper") {
    # Use wrapper script
    $writeArgs = @(
        "-mode", "write", "-workload", "sequential",
        "-nodes", $ScyllaNodes, "-datacenter", $Datacenter,
        "-partition-count", "100", "-clustering-row-count", "10",
        "-validate-data", "-duration", "5s"
    )
    $writeOutput = & $wrapperPath @writeArgs 2>&1
    $writeSuccess = ($LASTEXITCODE -eq 0)
    
    if ($writeSuccess) {
        Write-Host "    Write with validation: ✓" -ForegroundColor Green
        
        Write-Host "    Reading with validation..." -ForegroundColor Gray
        $readArgs = @(
            "-mode", "read", "-workload", "sequential",
            "-nodes", $ScyllaNodes, "-datacenter", $Datacenter,
            "-partition-count", "100", "-clustering-row-count", "10",
            "-validate-data", "-duration", "5s"
        )
        $readOutput = & $wrapperPath @readArgs 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "    Read with validation: ✓" -ForegroundColor Green
            $testResults += [PSCustomObject]@{
                Test = "Data Validation"
                Mode = "write+read"
                Workload = "sequential"
                Status = "PASS"
                Duration = 0
                Error = ""
            }
        } else {
            Write-Host "    Read validation: ✗ FAILED" -ForegroundColor Red
            $testResults += [PSCustomObject]@{
                Test = "Data Validation"
                Mode = "write+read"
                Workload = "sequential"
                Status = "FAIL"
                Duration = 0
                Error = "Read validation failed"
            }
        }
    } else {
        Write-Host "    Write validation: ✗ FAILED" -ForegroundColor Red
        $testResults += [PSCustomObject]@{
            Test = "Data Validation"
            Mode = "write+read"
            Workload = "sequential"
            Status = "FAIL"
            Duration = 0
            Error = "Write validation failed"
        }
    }
} else {
    # Use direct JAR
    $writeArgs = @(
        "-jar", $JarPath, "-mode", "write", "-workload", "sequential",
        "-nodes", $ScyllaNodes, "-datacenter", $Datacenter,
        "-partition-count", "100", "-clustering-row-count", "10",
        "-validate-data", "-duration", "5s"
    )
    $writeOutput = & java $writeArgs 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "    Write with validation: ✓" -ForegroundColor Green
        
        Write-Host "    Reading with validation..." -ForegroundColor Gray
        $readArgs = @(
            "-jar", $JarPath, "-mode", "read", "-workload", "sequential",
            "-nodes", $ScyllaNodes, "-datacenter", $Datacenter,
            "-partition-count", "100", "-clustering-row-count", "10",
            "-validate-data", "-duration", "5s"
        )
        $readOutput = & java $readArgs 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "    Read with validation: ✓" -ForegroundColor Green
            $testResults += [PSCustomObject]@{
                Test = "Data Validation"
                Mode = "write+read"
                Workload = "sequential"
                Status = "PASS"
                Duration = 0
                Error = ""
            }
        } else {
            Write-Host "    Read validation: ✗ FAILED" -ForegroundColor Red
            $testResults += [PSCustomObject]@{
                Test = "Data Validation"
                Mode = "write+read"
                Workload = "sequential"
                Status = "FAIL"
                Duration = 0
                Error = "Read validation failed"
            }
        }
    } else {
        Write-Host "    Write validation: ✗ FAILED" -ForegroundColor Red
        $testResults += [PSCustomObject]@{
            Test = "Data Validation"
            Mode = "write+read"
            Workload = "sequential"
            Status = "FAIL"
            Duration = 0
            Error = "Write validation failed"
        }
    }
}
Write-Host ""

# Summary
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "Test Summary" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host ""

$testResults | Format-Table -AutoSize

$passed = ($testResults | Where-Object { $_.Status -eq "PASS" }).Count
$failed = ($testResults | Where-Object { $_.Status -eq "FAIL" }).Count
$total = $testResults.Count

Write-Host ""
Write-Host "Results: $passed/$total passed, $failed/$total failed" -ForegroundColor $(if ($failed -eq 0) { "Green" } else { "Yellow" })

if ($failed -gt 0) {
    Write-Host ""
    Write-Host "Failed Tests:" -ForegroundColor Red
    $testResults | Where-Object { $_.Status -eq "FAIL" } | ForEach-Object {
        Write-Host "  - $($_.Test): $($_.Error)" -ForegroundColor Red
    }
}

Write-Host ""
if ($failed -eq 0) {
    Write-Host "✓ All workload modes verified successfully with Java 21!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "✗ Some tests failed. Review errors above." -ForegroundColor Red
    exit 1
}
