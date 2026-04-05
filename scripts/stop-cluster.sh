#!/bin/bash
PID_DIR=".pids"

for pid_file in "$PID_DIR"/server*.pid; do
    [ -f "$pid_file" ] || continue
    pid=$(cat "$pid_file")
    id=$(basename "$pid_file" .pid)
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid"
        echo "Stopped $id (PID $pid)"
    else
        echo "$id was not running"
    fi
    rm -f "$pid_file"
done
echo "Cluster stopped."
