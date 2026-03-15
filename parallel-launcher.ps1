#!/usr/bin/env pwsh
# parallel-launcher.ps1
# Launches multiple scylla-bench-java instances in parallel for maximum cluster utilization

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
    [int]$Instances = 6,
    
    [Parameter(Mandatory=$false)]
    [int]$DurationMinutes = 10,
    
    [Parameter(Mandatory=$false)]
    [int]$Concurrency = 1200,
    
    [Parameter(Mandatory=$false)]
    [int]$Connections = 16,
    
    [Parameter(Mandatory=$false)]
    [string]$JarPath = "target/scylla-bench-java.jar",
    
    [Parameter(Mandatory=$false)]
    [string]$JavaPath = "C:\Users\allen\.jdks\openjdk-21.0.2\bin\java"
)

$ErrorActionPreference = "Stop"

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "Parallel Benchmark Launcher" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Configuration:" -ForegroundColor Yellow
Write-Host "  Instances: $Instances"
Write-Host "  Nodes: $Nodes"
Write-Host "  Datacenter: $Datacenter"
Write-Host "  Port: $Port"
Write-Host "  Duration: ${DurationMinutes}m per instance"
Write-Host "  Concurrency: $Concurrency per instance"
Write-Host "  Connections: $Connections per instance"
Write-Host "  Total Concurrency: $($Instances * $Concurrency)"
Write-Host "  Total Connections: $($Instances * $Connections)"
Write-Host ""

# Verify JAR exists
if (-not (Test-Path $JarPath)) {
    Write-Host "ERROR: JAR file not found at $JarPath" -ForegroundColor Red
    exit 1
}

# Calculate memory per instance
$heapPerInstance = 8
$totalMemoryGB = $Instances * ($heapPerInstance + 2)
Write-Host "Memory Requirements:" -ForegroundColor Yellow
Write-Host "  Per instance: ~${heapPerInstance}GB heap + 2GB overhead"
Write-Host "  Total estimated: ~${totalMemoryGB}GB"
Write-Host ""

$continue = Read-Host "Continue? (y/n)"
if ($continue -ne 'y') {
    Write-Host "Cancelled." -ForegroundColor Yellow
    exit 0
}

Write-Host ""
Write-Host "Starting $Instances benchmark instances..." -ForegroundColor Cyan
Write-Host ""

# Clean up old log files
Get-ChildItem -Path . -Filter "bench-*.log" | Remove-Item -Force -ErrorAction SilentlyContinue

$startTime = Get-Date

# Launch instances in parallel
1..$Instances | ForEach-Object -Parallel {
    $id = $_
    $java = $using:JavaPath
    $jar = $using:JarPath
    $nodes = $using:Nodes
    $port = $using:Port
    $dc = $using:Datacenter
    $user = $using:Username
    $pass = $using:Password
    $duration = $using:DurationMinutes
    $concurrency = $using:Concurrency
    $connections = $using:Connections
    
    $args = @(
        "-Xms4g", "-Xmx8g", "-XX:+UseG1GC",
        "-jar", $jar,
        "-mode", "write",
        "-workload", "uniform",
        "-nodes", $nodes,
        "-port", $port,
        "-datacenter", $dc,
        "-partition-count", "10000",
        "-clustering-row-count", "50",
        "-rows-per-request", "40",
        "-concurrency", $concurrency,
        "-connection-count", $connections,
        "-duration", "${duration}m"
    )
    
    if ($user) { $args += "-username", $user }
    if ($pass) { $args += "-password", $pass }
    
    & $java $args > "bench-$id.log" 2>&1
    
    Write-Host "Instance $id completed" -ForegroundColor Green
} -ThrottleLimit $Instances

$endTime = Get-Date
$elapsed = $endTime - $startTime

Write-Host ""
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "All Instances Completed" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Total elapsed time: $([math]::Round($elapsed.TotalMinutes, 1)) minutes" -ForegroundColor Yellow
Write-Host ""

# Aggregate results
Write-Host "Aggregating results..." -ForegroundColor Cyan
Write-Host ""

$totalOps = 0
$instanceResults = @()

1..$Instances | ForEach-Object {
    $id = $_
    $logFile = "bench-$id.log"
    
    if (Test-Path $logFile) {
        Write-Host "--- Instance $id ---" -ForegroundColor Yellow
        
        $content = Get-Content $logFile -Raw
        $opsLines = $content | Select-String "(\d+)\s+ops/s" -AllMatches
        
        if ($opsLines.Matches.Count -gt 0) {
            # Get last 3 ops/s values and average
            $lastValues = $opsLines.Matches | Select-Object -Last 3 | ForEach-Object { [int]$_.Groups[1].Value }
            $avgOps = ($lastValues | Measure-Object -Average).Average
            
            Write-Host "  Average ops/s (last 3): $([math]::Round($avgOps, 0))" -ForegroundColor Cyan
            
            $totalOps += $avgOps
            $instanceResults += [PSCustomObject]@{
                Instance = $id
                'Ops/s' = [math]::Round($avgOps, 0)
                LogFile = $logFile
            }
        } else {
            Write-Host "  No ops/s data found" -ForegroundColor Red
        }
    }
}

Write-Host ""
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "Summary" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host ""

if ($instanceResults.Count -gt 0) {
    $instanceResults | Format-Table -AutoSize
    
    Write-Host ""
    Write-Host "Total Throughput: ~$([math]::Round($totalOps, 0)) ops/s" -ForegroundColor Green -BackgroundColor Black
    Write-Host ""
}

Write-Host "Log files: bench-1.log through bench-$Instances.log" -ForegroundColor Gray
Write-Host ""
Write-Host "✓ Parallel benchmark complete!" -ForegroundColor Green
