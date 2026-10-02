#!/bin/sh
set -e
BIN=/opt/kafka/bin/kafka-topics.sh
BS=kafka:29092
echo "Creating topics..."
$BIN --bootstrap-server $BS --create --if-not-exists --topic job_runs --partitions 6 --replication-factor 1
$BIN --bootstrap-server $BS --create --if-not-exists --topic job_runs.dlq --partitions 1 --replication-factor 1
$BIN --bootstrap-server $BS --list