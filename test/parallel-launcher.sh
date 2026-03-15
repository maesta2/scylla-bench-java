#!/bin/bash
# parallel-launcher.sh
# Launches multiple scylla-bench-java instances in parallel for maximum cluster utilization

set -e

# Default values
NODES=""
USERNAME=""
PASSWORD=""
PORT=9042
DATACENTER="datacenter1"
INSTANCES=6
DURATION_MINUTES=10
CONCURRENCY=1200
CONNECTIONS=16
JAR_PATH="target/scylla-bench-java.jar"
if [ -n "$JAVA_HOME" ]; then
  JAVA_PATH="$JAVA_HOME/bin/java"
else
  JAVA_PATH="java"
fi

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
    --instances)
      INSTANCES="$2"
      shift 2
      ;;
    --duration)
      DURATION_MINUTES="$2"
      shift 2
      ;;
    --concurrency)
      CONCURRENCY="$2"
      shift 2
      ;;
    --connections)
      CONNECTIONS="$2"
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
      echo "  --instances <n>         Number of parallel instances (default: 6)"
      echo "  --duration <minutes>    Duration per instance (default: 10)"
      echo "  --concurrency <n>       Concurrency per instance (default: 1200)"
      echo "  --connections <n>       Connections per instance (default: 16)"
      echo "  --jar <path>            JAR file path (default: target/scylla-bench-java.jar)"
      echo "  --java <path>           Java executable path (default: \$JAVA_HOME/java or /usr/bin/java)"
      echo ""
      echo "Example:"
      echo "  $0 --nodes node1,node2,node3 --username scylla --password mypass --instances 6"
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
echo -e "${CYAN}Parallel Benchmark Launcher${NC}"
echo -e "${CYAN}=====================================${NC}"
echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo "  Instances: $INSTANCES"
echo "  Nodes: $NODES"
echo "  Datacenter: $DATACENTER"
echo "  Port: $PORT"
echo "  Duration: ${DURATION_MINUTES}m per instance"
echo "  Concurrency: $CONCURRENCY per instance"
echo "  Connections: $CONNECTIONS per instance"
echo "  Total Concurrency: $((INSTANCES * CONCURRENCY))"
echo "  Total Connections: $((INSTANCES * CONNECTIONS))"
echo ""

# Verify JAR exists
if [ ! -f "$JAR_PATH" ]; then
  echo -e "${RED}ERROR: JAR file not found at $JAR_PATH${NC}"
  exit 1
fi

# Calculate memory requirements
HEAP_PER_INSTANCE=8
TOTAL_MEMORY=$((INSTANCES * (HEAP_PER_INSTANCE + 2)))

echo -e "${YELLOW}Memory Requirements:${NC}"
echo "  Per instance: ~${HEAP_PER_INSTANCE}GB heap + 2GB overhead"
echo "  Total estimated: ~${TOTAL_MEMORY}GB"
echo ""

read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo -e "${YELLOW}Cancelled.${NC}"
  exit 0
fi

echo ""
echo -e "${CYAN}Starting $INSTANCES benchmark instances...${NC}"
echo ""

# Clean up old log files
rm -f bench-*.log

START_TIME=$(date +%s)

# Function to run a single instance
run_instance() {
  local ID=$1
  local ARGS=(
    "-Xms4g" "-Xmx8g" "-XX:+UseG1GC"
    "-jar" "$JAR_PATH"
    "-mode" "write"
    "-workload" "uniform"
    "-nodes" "$NODES"
    "-port" "$PORT"
    "-datacenter" "$DATACENTER"
    "-partition-count" "10000"
    "-clustering-row-count" "50"
    "-rows-per-request" "40"
    "-concurrency" "$CONCURRENCY"
    "-connection-count" "$CONNECTIONS"
    "-duration" "${DURATION_MINUTES}m"
  )
  
  if [ -n "$USERNAME" ]; then
    ARGS+=("-username" "$USERNAME")
  fi
  
  if [ -n "$PASSWORD" ]; then
    ARGS+=("-password" "$PASSWORD")
  fi
  
  "$JAVA_PATH" "${ARGS[@]}" > "bench-$ID.log" 2>&1
  echo -e "${GREEN}Instance $ID completed${NC}"
}

# Launch instances in parallel
for i in $(seq 1 $INSTANCES); do
  run_instance $i &
  PIDS[$i]=$!
done

# Wait for all instances to complete
echo -e "${YELLOW}Running... (waiting for all instances to complete)${NC}"
echo ""

for i in $(seq 1 $INSTANCES); do
  wait ${PIDS[$i]}
done

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
ELAPSED_MINUTES=$(echo "scale=1; $ELAPSED / 60" | bc)

echo ""
echo -e "${CYAN}=====================================${NC}"
echo -e "${CYAN}All Instances Completed${NC}"
echo -e "${CYAN}=====================================${NC}"
echo ""
echo -e "${YELLOW}Total elapsed time: ${ELAPSED_MINUTES} minutes${NC}"
echo ""

# Aggregate results
echo -e "${CYAN}Aggregating results...${NC}"
echo ""

TOTAL_OPS=0
declare -a INSTANCE_RESULTS=()

for i in $(seq 1 $INSTANCES); do
  LOG_FILE="bench-$i.log"
  
  if [ -f "$LOG_FILE" ]; then
    echo -e "${YELLOW}--- Instance $i ---${NC}"
    
    # Extract last 3 ops/s values and average them
    OPS_VALUES=$(grep -oE '[0-9]+\s+ops/s' "$LOG_FILE" | grep -oE '^[0-9]+' | tail -3 || echo "")
    
    if [ -n "$OPS_VALUES" ]; then
      AVG_OPS=0
      COUNT=0
      
      while IFS= read -r OPS; do
        AVG_OPS=$((AVG_OPS + OPS))
        COUNT=$((COUNT + 1))
      done <<< "$OPS_VALUES"
      
      if [ $COUNT -gt 0 ]; then
        AVG_OPS=$((AVG_OPS / COUNT))
        echo -e "${CYAN}  Average ops/s (last 3): $AVG_OPS${NC}"
        TOTAL_OPS=$((TOTAL_OPS + AVG_OPS))
        INSTANCE_RESULTS+=("$i|$AVG_OPS")
      fi
    else
      echo -e "${RED}  No ops/s data found${NC}"
    fi
  fi
done

echo ""
echo -e "${CYAN}=====================================${NC}"
echo -e "${CYAN}Summary${NC}"
echo -e "${CYAN}=====================================${NC}"
echo ""

if [ ${#INSTANCE_RESULTS[@]} -gt 0 ]; then
  printf "%-10s %-10s %-20s\n" "Instance" "Ops/s" "Log File"
  printf "%-10s %-10s %-20s\n" "----------" "----------" "--------------------"
  
  for RESULT in "${INSTANCE_RESULTS[@]}"; do
    IFS='|' read -r INST OPS <<< "$RESULT"
    printf "%-10s %-10s %-20s\n" "$INST" "$OPS" "bench-$INST.log"
  done
  
  echo ""
  echo -e "${GREEN}${CYAN}Total Throughput: ~$TOTAL_OPS ops/s${NC}${NC}"
  echo ""
fi

echo -e "${YELLOW}Log files: bench-1.log through bench-$INSTANCES.log${NC}"
echo ""
echo -e "${GREEN}✓ Parallel benchmark complete!${NC}"
