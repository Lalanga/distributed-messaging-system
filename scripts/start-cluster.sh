#!/bin/bash
set -e

JAR="target/messaging-system.jar"
LOG_DIR="logs"
PID_DIR=".pids"

mkdir -p "$LOG_DIR" "$PID_DIR"

if [ ! -f "$JAR" ]; then
    echo "JAR not found. Building..."
    mvn -q clean package -DskipTests
fi

start_server() {
    local id=$1
    local cfg="config/server${id}.json"
    local log="$LOG_DIR/server${id}.log"
    local pid_file="$PID_DIR/server${id}.pid"

    if [ -f "$pid_file" ] && kill -0 "$(cat $pid_file)" 2>/dev/null; then
        echo "server${id} already running (PID $(cat $pid_file))"
        return
    fi

    java -jar "$JAR" server "$cfg" > "$log" 2>&1 &
    local pid=$!
    echo $pid > "$pid_file"
    echo "Started server${id}  PID=$pid  log=$log"
}

echo "=== Starting 3-node cluster ==="
start_server 1
sleep 0.5
start_server 2
sleep 0.5
start_server 3

echo ""
echo "Cluster started. Logs in $LOG_DIR/"
echo "To stop: ./scripts/stop-cluster.sh"
