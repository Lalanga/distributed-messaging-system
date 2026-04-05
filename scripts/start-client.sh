#!/bin/bash
JAR="target/messaging-system.jar"

if [ ! -f "$JAR" ]; then
    echo "JAR not found. Building..."
    mvn -q clean package -DskipTests
fi

java -jar "$JAR" client
