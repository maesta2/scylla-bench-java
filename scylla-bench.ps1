#!/usr/bin/env pwsh
# scylla-bench wrapper script for Windows PowerShell
# Automatically uses local Java 21+ or falls back to Docker
#
# Usage:
#   .\scylla-bench.ps1 [scylla-bench options]
#
# Environment variables:
#   DRIVER_VERSION - ScyllaDB Java driver version (default: LATEST)
#   Example: $env:DRIVER_VERSION="4.18.0.0"; .\scylla-bench.ps1 -mode write ...

param(
    [Parameter(ValueFromRemainingArguments)]
    [string[]]$Arguments
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$JarPath = Join-Path $ScriptDir "target\scylla-bench-java.jar"
$DriverVersion = if ($env:DRIVER_VERSION) { $env:DRIVER_VERSION } else { "LATEST" }
$DockerImage = "scylla-bench-java:latest"

# Check if Java 21+ is available
function Test-Java21 {
    try {
        $java = Get-Command java -ErrorAction SilentlyContinue
        if (-not $java) { return $false }
        
        $versionOutput = java -version 2>&1 | Select-String "version"
        if ($versionOutput -match '"(\d+)') {
            $majorVersion = [int]$Matches[1]
            return $majorVersion -ge 21
        }
    }
    catch {
        return $false
    }
    return $false
}

# Build JAR if not present or if DRIVER_VERSION changed
function Build-Jar {
    $rebuild = $false
    $cleanBuild = $false
    
    if (-not (Test-Path $JarPath)) {
        $rebuild = $true
    }
    elseif ($DriverVersion -ne "LATEST") {
        # Force clean rebuild if specific driver version requested
        Write-Host "Building with driver version: $DriverVersion"
        $rebuild = $true
        $cleanBuild = $true
    }
    
    if ($rebuild) {
        Write-Host "Building scylla-bench-java..."
        Push-Location $ScriptDir
        try {
            if ($cleanBuild) {
                mvn clean package -DskipTests -Dscylla.driver.version="$DriverVersion" -q
            } else {
                mvn package -DskipTests -Dscylla.driver.version="$DriverVersion" -q
            }
        }
        finally {
            Pop-Location
        }
    }
}

# Run with local Java
function Invoke-Local {
    Build-Jar
    java -jar $JarPath @Arguments
}

# Run with Docker
function Invoke-Docker {
    Write-Host "Java 21+ not found. Using Docker..." -ForegroundColor Yellow
    
    $dockerTag = $DockerImage
    if ($DriverVersion -ne "LATEST") {
        $dockerTag = "scylla-bench-java:driver-$DriverVersion"
    }
    
    # Build Docker image if not present
    try {
        docker image inspect $dockerTag 2>&1 | Out-Null
    }
    catch {
        Write-Host "Building Docker image with driver $DriverVersion..." -ForegroundColor Yellow
        Push-Location $ScriptDir
        try {
            docker build --build-arg DRIVER_VERSION="$DriverVersion" -t $dockerTag .
        }
        finally {
            Pop-Location
        }
    }
    
    # Run in Docker with host network
    docker run --rm --network=host $dockerTag @Arguments
}

# Main logic
if (Test-Java21) {
    Invoke-Local
}
else {
    $dockerCmd = Get-Command docker -ErrorAction SilentlyContinue
    if ($dockerCmd) {
        Invoke-Docker
    }
    else {
        Write-Error @"
Error: Java 21+ not found and Docker is not available.
Please install Java 21+ or Docker to run scylla-bench-java.

Java 21 download: https://adoptium.net/
Docker download: https://www.docker.com/get-started
"@
        exit 1
    }
}
