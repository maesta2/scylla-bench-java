#!/usr/bin/env pwsh
# parallel-launcher.ps1
# Launches multiple scylla-bench instances in parallel for maximum cluster utilization
#
# Usage:
#   .\parallel-launcher.ps1 -instances <N> -mode <mode> -workload <workload> [other scylla-bench options]
#
# Example:
#   .\parallel-launcher.ps1 -instances 6 -mode write -workload uniform -nodes node1,node2 -duration 10m

param(
    [Parameter(Mandatory=$false)]
    [int]$instances = 6,
    
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$BenchArgs
)

$ErrorActionPreference = "Stop"

# Get script directory and project root
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
$wrapper = Join-Path $projectRoot "scylla-bench.ps1"

# Check if wrapper exists
if (-not (Test-Path $wrapper)) {
    Write-Host "ERROR: Wrapper script not found: $wrapper" -ForegroundColor Red
    exit 1
}

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "Parallel Benchmark Launcher" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Configuration:" -ForegroundColor Yellow
Write-Host "  Instances: $instances"
Write-Host "  Benchmark args: $($BenchArgs -join ' ')"
Write-Host ""
Write-Host "Note: Each instance runs independently with the same arguments." -ForegroundColor Yellow
Write-Host ""

$continue = Read-Host "Continue? (y/n)"
if ($continue -ne 'y') {
    Write-Host "Cancelled." -ForegroundColor Yellow
    exit 0
}

Write-Host ""
Write-Host "Starting $instances benchmark instances..." -ForegroundColor Cyan
Write-Host ""

# Clean up old log files
Get-ChildItem -Path . -Filter "bench-*.log" | Remove-Item -Force -ErrorAction SilentlyContinue

$startTime = Get-Date

# Launch instances in parallel
1..$instances | ForEach-Object -Parallel {
    $id = $_
    $wrapper = $using:wrapper
    $args = $using:BenchArgs
    
    & $wrapper $args > "bench-$id.log" 2>&1
    
    Write-Host "Instance $id completed" -ForegroundColor Green
} -ThrottleLimit $instances

$endTime = Get-Date
$elapsed = $endTime - $startTime

Write-Host ""
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "All Instances Completed" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Total elapsed time: $([math]::Round($elapsed.TotalMinutes, 1)) minutes" -ForegroundColor Yellow
Write-Host ""
Write-Host "Log files: bench-1.log through bench-$instances.log" -ForegroundColor Yellow
Write-Host ""
