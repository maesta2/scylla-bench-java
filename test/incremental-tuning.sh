#!/bin/bash
# incremental-tuning.sh
# Tests different concurrency levels to find optimal throughput for your ScyllaDB cluster

set -e

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Default values
NODES=""
USERNAME=""
PASSWORD=""
PORT=9042
DATACENTER="datacenter1"
TEST_DURATION=60
JAR_PATH="$PROJECT_ROOT/target/scylla-bench-java.jar"
if [ -n "$JAVA_HOME" ]; then
  JAVA_PATH="$JAVA_HOME/bin/java"
else
  JAVA_PATH="java"
fi

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -nodes)
      NODES="$2"
      shift 2
      ;;
    -username)
      USERNAME="$2"
      shift 2
      ;;
    -password)
      PASSWORD="$2"
      shift 2
      ;;
    -port)
      PORT="$2"
      shift 2
      ;;
    -datacenter)
      DATACENTER="$2"
      shift 2
      ;;
    -duration)
      TEST_DURATION="$2"
      shift 2
      ;;
    -jar)
      JAR_PATH="$2"
      shift 2
      ;;
    -java)
      JAVA_PATH="$2"
      shift 2
      ;;
    -help|--help)
      echo "Usage: $0 -nodes <nodes> [options]"
      echo ""
      echo "Required:"
      echo "  -nodes <nodes>         Comma-separated list of ScyllaDB nodes"
      echo ""
      echo "Optional:"
      echo "  -username <user>       Username for authentication"
      echo "  -password <pass>       Password for authentication"
      echo "  -port <port>           CQL port (default: 9042)"
      echo "  -datacenter <dc>       Datacenter name (default: datacenter1)"
      echo "  -duration <seconds>    Test duration per level (default: 60)"
      echo "  -jar <path>            JAR file path (default: PROJECT_ROOT/target/scylla-bench-java.jar)"
      echo "  -java <path>           Java executable path (default: \$JAVA_HOME/bin/java or java)"
      echo ""
      echo "Example:"
      echo "  $0 -nodes node1,node2,node3 -username scylla -password mypass -datacenter AWS_EU_WEST_2"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use -help for usage information"
      exit 1
      ;;
  esac
done

# Check required arguments
if [ -z "$NODES" ]; then
  echo "ERROR: -nodes is required"
  echo "Use -help for usage information"
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
echo "  Test Duration: ${TEST_DURATION}s per test (${TEST_DURATION}s × 5 tests = $((TEST_DURATION * 5))s total)"
echo "  JAR: $JAR_PATH"
echo "  Java: $JAVA_PATH"
echo ""
echo -e "${YELLOW}Testing 5 concurrency levels: 4096, 8192, 16384, 32768, 65536${NC}"
echo -e "${YELLOW}This tests HIGH concurrency to fully utilize your cluster${NC}"
echo ""
echo -e "${CYAN}TIP: Monitor ScyllaDB cluster CPU during tests${NC}"
echo -e "${CYAN}     Target: 50-80% CPU for optimal throughput${NC}"
echo -e "${CYAN}     If CPU stays low, increase concurrency further${NC}"
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

TEST_NUM=0
TOTAL_TESTS=5

for CONCURRENCY in 4096 8192 16384 32768 65536; do
  TEST_NUM=$((TEST_NUM + 1))
  CONNECTIONS=$(( CONCURRENCY / 64 ))
  if [ $CONNECTIONS -lt 48 ]; then CONNECTIONS=48; fi
  if [ $CONNECTIONS -gt 256 ]; then CONNECTIONS=256; fi
  
  HEAP=$(( CONCURRENCY / 256 ))
  if [ $HEAP -lt 16 ]; then HEAP=16; fi
  if [ $HEAP -gt 64 ]; then HEAP=64; fi
  MAX_HEAP=$(( HEAP * 2 ))
  
  echo -e "${YELLOW}[Test $TEST_NUM/$TOTAL_TESTS] Concurrency=$CONCURRENCY, Connections=$CONNECTIONS, Heap=${HEAP}g${NC}"
  echo -e "${CYAN}Starting at $(date '+%H:%M:%S')... (will run for ${TEST_DURATION}s)${NC}"
  
  # Run test with timeout (add 10s buffer to test duration)
  TIMEOUT_DURATION=$((TEST_DURATION + 10))
  OUTPUT=$(timeout ${TIMEOUT_DURATION} "$JAVA_PATH" -Xms${HEAP}g -Xmx${MAX_HEAP}g -XX:+UseG1GC \
    -jar "$JAR_PATH" \
    "${BASE_ARGS[@]}" \
    -concurrency "$CONCURRENCY" \
    -connection-count "$CONNECTIONS" \
    2>&1 || echo "")
  
  # Parse the output - look for stats lines with format: "53s 503 503 0 ..."
  # Extract throughput from the second column (ops/s)
  OPS_LINE=$(echo "$OUTPUT" | grep -E '^[[:space:]]*[0-9]+s[[:space:]]+[0-9]+' | tail -1 || echo "")
  
  if [ -n "$OPS_LINE" ]; then
    # Extract the ops/s value (second column after the timestamp)
    OPS_VALUE=$(echo "$OPS_LINE" | awk '{print $2}')
    
    if [ -n "$OPS_VALUE" ] && [ "$OPS_VALUE" -gt 0 ] 2>/dev/null; then
      echo -e "${GREEN}  ✓ Completed at $(date '+%H:%M:%S') - Throughput: ${OPS_VALUE} ops/s${NC}"
      RESULTS+=("$CONCURRENCY|$CONNECTIONS|$HEAP|$OPS_VALUE")
    else
      echo -e "${RED}  ✗ Failed at $(date '+%H:%M:%S') - Could not parse throughput${NC}"
      echo "$OUTPUT" | tail -5
    fi
  else
    echo -e "${RED}  ✗ Failed at $(date '+%H:%M:%S') - No stats data found${NC}"
    # Show last few lines for debugging
    echo "$OUTPUT" | tail -5
  fi
  
  echo ""
done

echo -e "${CYAN}=====================================${NC}"
echo -e "${CYAN}Summary${NC}"
echo -e "${CYAN}=====================================${NC}"
echo ""

# Print results table
printf "%-12s %-12s %-10s %-12s %-6s\n" "Concurrency" "Connections" "Heap (GB)" "Ops/s" "Best?"
printf "%-12s %-12s %-10s %-12s %-6s\n" "------------" "------------" "----------" "------------" "------"

BEST_OPS=0
BEST_CONCURRENCY=0
BEST_CONNECTIONS=0
BEST_HEAP=0

# First pass: find the best
for RESULT in "${RESULTS[@]}"; do
  IFS='|' read -r CONC CONN HEAP OPS <<< "$RESULT"
  if [ $OPS -gt $BEST_OPS ]; then
    BEST_OPS=$OPS
    BEST_CONCURRENCY=$CONC
    BEST_CONNECTIONS=$CONN
    BEST_HEAP=$HEAP
  fi
done

# Second pass: print with highlighting
for RESULT in "${RESULTS[@]}"; do
  IFS='|' read -r CONC CONN HEAP OPS <<< "$RESULT"
  if [ "$CONC" = "$BEST_CONCURRENCY" ] && [ $OPS -eq $BEST_OPS ]; then
    printf "${GREEN}%-12s %-12s %-10s %-12s %-6s${NC}\n" "$CONC" "$CONN" "$HEAP" "$OPS" "  ✓"
  else
    printf "%-12s %-12s %-10s %-12s %-6s\n" "$CONC" "$CONN" "$HEAP" "$OPS" ""
  fi
done

if [ $BEST_OPS -gt 0 ]; then
  echo ""
  echo -e "${GREEN}✓ OPTIMAL SETTINGS FOUND:${NC}"
  echo ""
  echo -e "${CYAN}  Concurrency:  ${BEST_CONCURRENCY}${NC}"
  echo -e "${CYAN}  Connections:  ${BEST_CONNECTIONS}${NC}"
  echo -e "${CYAN}  Heap Size:    ${BEST_HEAP}GB (use -Xms${BEST_HEAP}g -Xmx$((BEST_HEAP * 2))g)${NC}"
  echo -e "${CYAN}  Throughput:   ~${BEST_OPS} ops/s${NC}"
  echo ""
  echo -e "${YELLOW}To use these settings:${NC}"
  echo -e "  ${GREEN}./scylla-bench.sh -concurrency ${BEST_CONCURRENCY} -connection-count ${BEST_CONNECTIONS} [other options]${NC}"
  echo ""
  echo -e "${YELLOW}Or set environment variable:${NC}"
  echo -e "  ${GREEN}export JAVA_OPTS=\"-Xms${BEST_HEAP}g -Xmx$((BEST_HEAP * 2))g\"${NC}"
else
  echo ""
  echo -e "${RED}✗ No valid results found. All tests failed.${NC}"
  echo -e "${YELLOW}Troubleshooting:${NC}"
  echo "  - Verify nodes are reachable: $NODES"
  echo "  - Check credentials and datacenter name"
  echo "  - Review error output above"
fi

echo ""
echo -e "${GREEN}✓ Tuning complete!${NC}"
