#!/bin/bash
# Monitor pipeline progress and check for completion

LOG_FILE="pipeline_run_10.log"
MAX_WAIT_MINUTES=30
CHECK_INTERVAL=30

echo "Monitoring pipeline: $LOG_FILE"
echo "Will check every ${CHECK_INTERVAL}s for up to ${MAX_WAIT_MINUTES} minutes"
echo ""

start_time=$(date +%s)
max_wait_seconds=$((MAX_WAIT_MINUTES * 60))

while true; do
    # Check if log file exists and was modified in last 2 minutes
    if [ ! -f "$LOG_FILE" ]; then
        echo "Log file not found. Waiting..."
        sleep $CHECK_INTERVAL
        continue
    fi
    
    last_modified=$(stat -f "%m" "$LOG_FILE" 2>/dev/null || echo "0")
    current_time=$(date +%s)
    time_since_mod=$((current_time - last_modified))
    
    # Check if pipeline completed
    if grep -q "PIPELINE COMPLETED" "$LOG_FILE" 2>/dev/null; then
        echo ""
        echo "✓ Pipeline completed!"
        echo ""
        tail -30 "$LOG_FILE" | grep -E "(Summary|rows|Successfully)"
        echo ""
        echo "Checking data quality..."
        cd "$(dirname "$0")"
        if [ -f "venv/bin/python" ]; then
            venv/bin/python check_pipeline_status.py
        else
            python3 check_pipeline_status.py
        fi
        exit 0
    fi
    
    # Check if log hasn't been updated in 2 minutes (likely stuck or done)
    if [ $time_since_mod -gt 120 ]; then
        echo ""
        echo "⚠ Log file hasn't been updated in 2+ minutes. Pipeline may be stuck or completed without 'PIPELINE COMPLETED' message."
        echo "Last log entries:"
        tail -20 "$LOG_FILE"
        echo ""
        echo "Checking current data quality..."
        python3 check_pipeline_status.py
        exit 1
    fi
    
    # Show progress
    elapsed=$((current_time - start_time))
    echo "[$(date +%H:%M:%S)] Pipeline running... (${elapsed}s elapsed, last update ${time_since_mod}s ago)"
    tail -3 "$LOG_FILE" | grep -E "(Found|Extracted|Successfully|ERROR)" | tail -1 || echo "  (no recent progress)"
    
    # Check if we've exceeded max wait time
    if [ $elapsed -gt $max_wait_seconds ]; then
        echo ""
        echo "⚠ Exceeded max wait time (${MAX_WAIT_MINUTES} minutes)"
        tail -30 "$LOG_FILE"
        exit 1
    fi
    
    sleep $CHECK_INTERVAL
done
