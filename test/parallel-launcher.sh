#!/bin/bash
# parallel-launcher.sh
# Launches multiple scylla-bench instances in parallel for maximum cluster utilization
#
# Usage:
#   ./parallel-launcher.sh -instances <N> [any scylla-bench options]
#
# Example:
#   ./parallel-launcher.sh -instances 6 -mode write -workload uniform -nodes node1,node2 -duration 10m

set -e

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WRAPPER="$PROJECT_ROOT/scylla-bench.sh"

# Default values
INSTANCES=6
BENCH_ARGS=()

# Parse launcher-specific arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -instances)
      INSTANCES="$2"
      shift 2
      ;;
    -help|--help)
      echo "Usage: $0 -instances <N> [scylla-bench options]"
      echo ""
      echo "Launcher Options:"
      echo "  -instances <n>    Number of parallel instances (default: 6)"
      echo "  -help             Show this help"
      echo ""
      echo "All other options are passed directly to scylla-bench-java."
      echo "Run './scylla-bench.sh -help' to see all available scylla-bench options."
      echo ""
      echo "Examples:"
      echo "  # Run 6 parallel write benchmarks"
      echo "  $0 -instances 6 -mode write -workload uniform -nodes node1,node2 -duration 10m"
      echo ""
      echo "  # Run 4 parallel mixed workloads"
      echo "  $0 -instances 4 -mode mixed -workload uniform -nodes node1,node2,node3 \\"
      echo "     -username scylla -password *** -datacenter AWS_EU_WEST_2"
      exit 0
      ;;
    *)
      # Collect all other arguments for scylla-bench
      BENCH_ARGS+=("$1")
      shift
      ;;
  esac
done

# Check if wrapper exists
if [ ! -x "$WRAPPER" ]; then
  echo "ERROR: Wrapper script not found or not executable: $WRAPPER"
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
echo "  Benchmark args: ${BENCH_ARGS[@]}"
echo ""
echo -e "${YELLOW}Note: Each instance runs independently with the same arguments.${NC}"
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

# Launch instances in parallel using background jobs
for i in $(seq 1 $INSTANCES); do
  (
    "$WRAPPER" "${BENCH_ARGS[@]}" > "bench-$i.log" 2>&1
    echo -e "${GREEN}Instance $i completed${NC}"
  ) &
done

# Wait for all background jobs
wait

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
MINUTES=$((ELAPSED / 60))
SECONDS=$((ELAPSED % 60))

echo ""
echo -e "${CYAN}=====================================${NC}"
echo -e "${CYAN}All Instances Completed${NC}"
echo -e "${CYAN}=====================================${NC}"
echo ""
echo -e "${YELLOW}Total elapsed time: ${MINUTES}m ${SECONDS}s${NC}"
echo ""
echo -e "${YELLOW}Log files: bench-1.log through bench-$INSTANCES.log${NC}"
echo ""
