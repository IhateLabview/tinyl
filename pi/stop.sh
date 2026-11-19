#!/usr/bin/env bash
# Stoppt nur die RobustEar-Bridge. Mosquitto, InfluxDB und Grafana laufen weiter.
set -euo pipefail
cd "$(dirname "$0")"
DOCKER="docker"
$DOCKER ps >/dev/null 2>&1 || DOCKER="sudo docker"
$DOCKER compose down
