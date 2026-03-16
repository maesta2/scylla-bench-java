#!/usr/bin/env pwsh
# incremental-tuning.ps1
# Tests different concurrency levels to find optimal throughput for your ScyllaDB cluster

param(
    [Parameter(Mandatory=$true)]
    [string]$Nodes,
    
    [Parameter(Mandatory=$false)]
    [string]$Username = "",
    
    [Parameter(Mandatory=$false)]
    [string]$Password = "",
    
    [Parameter(Mandatory=$false)]
    [int]$Port = 9042,
    
    [Parameter(Mandatory=$false)]
    [string]$Datacenter = "datacenter1",
    
    [Parameter(Mandatory=$false)]
    [int]$TestDuration = 30,
    
    [Parameter(Mandatory=$false)]
    [string]$JarPath = "",
    
    [Parameter(Mandatory=$false)]
    [string]$JavaPath = "java"
)

$ErrorActionPreference = "Stop"

# Get script directory and project root
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir

# Set default JAR path if not provided
if ([string]::IsNullOrEmpty($JarPath)) {
    $JarPath = Join-Path $projectRoot "target\scylla-bench-java.jar"
}

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "Incremental Concurrency Tuning" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Configuration:" -ForegroundColor Yellow
Write-Host "  Nodes: $Nodes"
Write-Host "  Datacenter: $Datacenter"
Write-Host "  Port: $Port"
Write-Host "  Test Duration: ${TestDuration}s per test"
Write-Host "  JAR: $JarPath"
Write-Host ""

# Verify JAR exists
if (-not (Test-Path $JarPath)) {
    Write-Host "ERROR: JAR file not found at $JarPath" -ForegroundColor Red
    Write-Host "Run 'mvn clean package -DskipTests' first" -ForegroundColor Yellow
    exit 1
}

# Base arguments
$baseArgs = @(
    "-nodes", $Nodes,
    "-port", $Port,
    "-datacenter", $Datacenter,
    "-mode", "write",
    "-workload", "uniform",
    "-partition-count", "10000",
    "-clustering-row-count", "100",
    "-clustering-row-size", "fixed:4",
    "-duration", "${TestDuration}s"
)

if ($Username) { $baseArgs += "-username", $Username }
if ($Password) { $baseArgs += "-password", $Password }

Write-Host "Finding optimal concurrency..." -ForegroundColor Cyan
Write-Host ""

$results = @()

foreach ($concurrency in @(256, 512, 1024, 2048, 4096)) {
    $connections = [math]::Min(48, [math]::Max(16, $concurrency / 64))
    $heap = [math]::Min(24, [math]::Max(4, $concurrency / 256))
    
    Write-Host "[$concurrency] Testing: connections=$connections, heap=${heap}g" -ForegroundColor Yellow
    
    $allArgs = @("-Xms${heap}g", "-Xmx$([math]::Ceiling($heap * 2))g", "-XX:+UseG1GC", "-jar", $JarPath) + $baseArgs + @("-concurrency", $concurrency, "-connection-count", $connections)
    
    try {
        $output = & $JavaPath $allArgs 2>&1 | Out-String
        
        # Extract final ops/s
        $opsLines = $output | Select-String "ops/s"
        $lastOps = $opsLines | Select-Object -Last 1
        
        if ($lastOps) {
            Write-Host "  Result: $lastOps" -ForegroundColor Green
            
            # Try to extract numeric value
            if ($lastOps -match '(\d+)\s+ops/s') {
                $opsValue = [int]$matches[1]
            } else {
                $opsValue = 0
            }
            
            $results += [PSCustomObject]@{
                Concurrency = $concurrency
                Connections = $connections
                'Heap (GB)' = $heap
                'Ops/s' = $opsValue
                Result = $lastOps.ToString().Trim()
            }
        } else {
            Write-Host "  No ops/s data found" -ForegroundColor Red
        }
    } catch {
        Write-Host "  ERROR: $_" -ForegroundColor Red
    }
    
    Write-Host ""
}

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "Summary" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host ""

$results | Format-Table -AutoSize

if ($results.Count -gt 0) {
    $best = $results | Sort-Object 'Ops/s' -Descending | Select-Object -First 1
    Write-Host ""
    Write-Host "✓ Recommended settings:" -ForegroundColor Green
    Write-Host "  Concurrency: $($best.Concurrency)" -ForegroundColor Cyan
    Write-Host "  Connections: $($best.Connections)" -ForegroundColor Cyan
    Write-Host "  Heap: $($best.'Heap (GB)')g" -ForegroundColor Cyan
    Write-Host "  Expected throughput: ~$($best.'Ops/s') ops/s" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "✓ Tuning complete!" -ForegroundColor Green
