#!/bin/bash
# Check if pipeline has completed

LOG_FILE="pipeline_run_12.log"

if [ ! -f "$LOG_FILE" ]; then
    echo "Log file not found"
    exit 1
fi

# Check for completion message
if grep -q "PIPELINE COMPLETED" "$LOG_FILE" 2>/dev/null; then
    echo "COMPLETED"
    exit 0
fi

# Check if log was updated in last 5 minutes
last_modified=$(stat -f "%m" "$LOG_FILE" 2>/dev/null || echo "0")
current_time=$(date +%s)
time_since_mod=$((current_time - last_modified))

if [ $time_since_mod -gt 300 ]; then
    echo "STUCK_OR_DONE"
    exit 1
fi

echo "RUNNING"
exit 2
