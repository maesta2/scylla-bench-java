#!/usr/bin/env bash
# scylla-bench wrapper script for Linux/macOS
# Automatically uses local Java 21+ or falls back to Docker
#
# Usage:
#   ./scylla-bench.sh [scylla-bench options]
#
# Environment variables:
#   DRIVER_VERSION - ScyllaDB Java driver version (default: LATEST)
#   Example: DRIVER_VERSION=4.18.0.0 ./scylla-bench.sh -mode write ...

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_PATH="$SCRIPT_DIR/target/scylla-bench-java.jar"
DRIVER_VERSION="${DRIVER_VERSION:-LATEST}"
DOCKER_IMAGE="scylla-bench-java:latest"

# Check if Java 21+ is available
check_java() {
    if command -v java >/dev/null 2>&1; then
        version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
        if [ "$version" -ge 21 ] 2>/dev/null; then
            return 0
        fi
    fi
    return 1
}

# Build JAR if not present or if DRIVER_VERSION changed
build_jar() {
    local rebuild=false
    
    if [ ! -f "$JAR_PATH" ]; then
        rebuild=true
    elif [ "$DRIVER_VERSION" != "LATEST" ]; then
        # Rebuild if specific driver version requested
        echo "Building with driver version: $DRIVER_VERSION"
        rebuild=true
    fi
    
    if [ "$rebuild" = true ]; then
        echo "Building scylla-bench-java..."
        cd "$SCRIPT_DIR"
        mvn package -DskipTests -Dscylla.driver.version="$DRIVER_VERSION" -q
    fi
}

# Run with local Java
run_local() {
    build_jar
    java -jar "$JAR_PATH" "$@"
}

# Run with Docker
run_docker() {
    echo "Java 21+ not found. Using Docker..." >&2
    
    local docker_tag="$DOCKER_IMAGE"
    if [ "$DRIVER_VERSION" != "LATEST" ]; then
        docker_tag="scylla-bench-java:driver-$DRIVER_VERSION"
    fi
    
    # Build Docker image if not present
    if ! docker image inspect "$docker_tag" >/dev/null 2>&1; then
        echo "Building Docker image with driver $DRIVER_VERSION..." >&2
        cd "$SCRIPT_DIR"
        docker build --build-arg DRIVER_VERSION="$DRIVER_VERSION" -t "$docker_tag" .
    fi
    
    # Run in Docker with host network
    docker run --rm --network=host "$docker_tag" "$@"
}

# Main logic
if check_java; then
    run_local "$@"
else
    if command -v docker >/dev/null 2>&1; then
        run_docker "$@"
    else
        echo "Error: Java 21+ not found and Docker is not available." >&2
        echo "Please install Java 21+ or Docker to run scylla-bench-java." >&2
        echo "" >&2
        echo "Java 21 download: https://adoptium.net/" >&2
        echo "Docker download: https://www.docker.com/get-started" >&2
        exit 1
    fi
fi
