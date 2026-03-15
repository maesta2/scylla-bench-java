#!/bin/bash
# incremental-tuning.sh
# Tests different concurrency levels to find optimal throughput for your ScyllaDB cluster

set -e

# Default values
NODES=""
USERNAME=""
PASSWORD=""
PORT=9042
DATACENTER="datacenter1"
TEST_DURATION=30
JAR_PATH="target/scylla-bench-java.jar"
JAVA_PATH="${JAVA_HOME:-/usr/bin}/java"

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --nodes)
      NODES="$2"
      shift 2
      ;;
    --username)
      USERNAME="$2"
      shift 2
      ;;
    --password)
      PASSWORD="$2"
      shift 2
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    --datacenter)
      DATACENTER="$2"
      shift 2
      ;;
    --duration)
      TEST_DURATION="$2"
      shift 2
      ;;
    --jar)
      JAR_PATH="$2"
      shift 2
      ;;
    --java)
      JAVA_PATH="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 --nodes <nodes> [options]"
      echo ""
      echo "Required:"
      echo "  --nodes <nodes>         Comma-separated list of ScyllaDB nodes"
      echo ""
      echo "Optional:"
      echo "  --username <user>       Username for authentication"
      echo "  --password <pass>       Password for authentication"
      echo "  --port <port>           CQL port (default: 9042)"
      echo "  --datacenter <dc>       Datacenter name (default: datacenter1)"
      echo "  --duration <seconds>    Test duration per level (default: 30)"
      echo "  --jar <path>            JAR file path (default: target/scylla-bench-java.jar)"
      echo "  --java <path>           Java executable path (default: \$JAVA_HOME/java or /usr/bin/java)"
      echo ""
      echo "Example:"
      echo "  $0 --nodes node1,node2,node3 --username scylla --password mypass --datacenter AWS_EU_WEST_2"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Check required arguments
if [ -z "$NODES" ]; then
  echo "ERROR: --nodes is required"
  echo "Use --help for usage information"
  exit 1
fi

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}=====================================${NC}"
echo -e "${CYAN}Incremental Concurrency Tuning${NC}"
echo -e "${CYAN}=====================================${NC}"
echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo "  Nodes: $NODES"
echo "  Datacenter: $DATACENTER"
echo "  Port: $PORT"
echo "  Test Duration: ${TEST_DURATION}s per test"
echo "  JAR: $JAR_PATH"
echo "  Java: $JAVA_PATH"
echo ""

# Verify JAR exists
if [ ! -f "$JAR_PATH" ]; then
  echo -e "${RED}ERROR: JAR file not found at $JAR_PATH${NC}"
  exit 1
fi

# Base arguments
BASE_ARGS=(
  "-nodes" "$NODES"
  "-port" "$PORT"
  "-datacenter" "$DATACENTER"
  "-mode" "write"
  "-workload" "uniform"
  "-partition-count" "10000"
  "-clustering-row-count" "100"
  "-clustering-row-size" "fixed:4"
  "-duration" "${TEST_DURATION}s"
)

if [ -n "$USERNAME" ]; then
  BASE_ARGS+=("-username" "$USERNAME")
fi

if [ -n "$PASSWORD" ]; then
  BASE_ARGS+=("-password" "$PASSWORD")
fi

echo -e "${CYAN}Finding optimal concurrency...${NC}"
echo ""

# Results array
declare -a RESULTS=()

for CONCURRENCY in 256 512 1024 2048 4096; do
  CONNECTIONS=$(( CONCURRENCY / 64 ))
  if [ $CONNECTIONS -lt 16 ]; then CONNECTIONS=16; fi
  if [ $CONNECTIONS -gt 48 ]; then CONNECTIONS=48; fi
  
  HEAP=$(( CONCURRENCY / 256 ))
  if [ $HEAP -lt 4 ]; then HEAP=4; fi
  if [ $HEAP -gt 24 ]; then HEAP=24; fi
  MAX_HEAP=$(( HEAP * 2 ))
  
  echo -e "${YELLOW}[$CONCURRENCY] Testing: connections=$CONNECTIONS, heap=${HEAP}g${NC}"
  
  # Run test
  OUTPUT=$("$JAVA_PATH" -Xms${HEAP}g -Xmx${MAX_HEAP}g -XX:+UseG1GC \
    -jar "$JAR_PATH" \
    "${BASE_ARGS[@]}" \
    -concurrency "$CONCURRENCY" \
    -connection-count "$CONNECTIONS" \
    2>&1 || true)
  
  # Extract ops/s from last line with ops/s
  OPS_LINE=$(echo "$OUTPUT" | grep -E '\d+\s+ops/s' | tail -1 || echo "")
  
  if [ -n "$OPS_LINE" ]; then
    echo -e "${GREEN}  Result: $OPS_LINE${NC}"
    
    # Extract numeric ops/s value
    OPS_VALUE=$(echo "$OPS_LINE" | grep -oE '[0-9]+\s+ops/s' | grep -oE '^[0-9]+' || echo "0")
    
    RESULTS+=("$CONCURRENCY|$CONNECTIONS|$HEAP|$OPS_VALUE")
  else
    echo -e "${RED}  No ops/s data found${NC}"
  fi
  
  echo ""
done

echo -e "${CYAN}=====================================${NC}"
echo -e "${CYAN}Summary${NC}"
echo -e "${CYAN}=====================================${NC}"
echo ""

# Print results table
printf "%-12s %-12s %-10s %-10s\n" "Concurrency" "Connections" "Heap (GB)" "Ops/s"
printf "%-12s %-12s %-10s %-10s\n" "------------" "------------" "----------" "----------"

BEST_OPS=0
BEST_CONCURRENCY=0
BEST_CONNECTIONS=0
BEST_HEAP=0

for RESULT in "${RESULTS[@]}"; do
  IFS='|' read -r CONC CONN HEAP OPS <<< "$RESULT"
  printf "%-12s %-12s %-10s %-10s\n" "$CONC" "$CONN" "$HEAP" "$OPS"
  
  if [ $OPS -gt $BEST_OPS ]; then
    BEST_OPS=$OPS
    BEST_CONCURRENCY=$CONC
    BEST_CONNECTIONS=$CONN
    BEST_HEAP=$HEAP
  fi
done

if [ $BEST_OPS -gt 0 ]; then
  echo ""
  echo -e "${GREEN}✓ Recommended settings:${NC}"
  echo -e "${CYAN}  Concurrency: $BEST_CONCURRENCY${NC}"
  echo -e "${CYAN}  Connections: $BEST_CONNECTIONS${NC}"
  echo -e "${CYAN}  Heap: ${BEST_HEAP}g${NC}"
  echo -e "${CYAN}  Expected throughput: ~$BEST_OPS ops/s${NC}"
fi

echo ""
echo -e "${GREEN}✓ Tuning complete!${NC}"
