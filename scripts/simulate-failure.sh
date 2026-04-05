#!/bin/bash
# Usage: ./scripts/simulate-failure.sh <1|2|3> [--restart]
set -e

if [ $# -lt 1 ]; then
    echo "Usage: $0 <server-number> [--restart]"
    echo "  e.g.: $0 2          # kill server2"
    echo "        $0 2 --restart # kill then restart server2"
    exit 1
fi

NUM=$1
PID_FILE=".pids/server${NUM}.pid"
LOG="logs/server${NUM}.log"
CFG="config/server${NUM}.json"
JAR="target/messaging-system.jar"

kill_server() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            kill -9 "$PID"
            echo "$(date '+%T') [FAILURE] server${NUM} killed (PID $PID)"
        else
            echo "server${NUM} was not running"
        fi
        rm -f "$PID_FILE"
    else
        # Try by process search as fallback
        PIDS=$(pgrep -f "server${NUM}.json" 2>/dev/null || true)
        if [ -n "$PIDS" ]; then
            kill -9 $PIDS
            echo "$(date '+%T') [FAILURE] server${NUM} killed (PID $PIDS)"
        else
            echo "server${NUM} not found"
        fi
    fi
}

kill_server

if [ "${2:-}" = "--restart" ]; then
    echo "Waiting 3s before restart..."
    sleep 3
    java -jar "$JAR" server "$CFG" >> "$LOG" 2>&1 &
    PID=$!
    echo $PID > "$PID_FILE"
    echo "$(date '+%T') [RECOVERY] server${NUM} restarted (PID $PID)"
fi
