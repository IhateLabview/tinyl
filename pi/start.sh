#!/usr/bin/env bash
# RobustEar auf dem Raspberry Pi starten:
#   1. Bridge-Container bauen und starten (MQTT -> InfluxDB)
#   2. Grafana-Datenquelle + Dashboard einrichten
#   3. Live-Log der Bridge anzeigen (Strg+C beendet nur die Anzeige, nicht die Bridge)
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -f .env ]; then
    cp .env.example .env
    echo ">> .env wurde angelegt. Bitte ausfuellen (nano .env) und start.sh erneut ausfuehren."
    exit 1
fi
set -a; . ./.env; set +a
if [ -z "${INFLUX_TOKEN:-}" ] || [ -z "${INFLUX_ORG:-}" ]; then
    echo ">> INFLUX_TOKEN und INFLUX_ORG in .env eintragen (nano .env)"
    exit 1
fi

# docker ohne/mit sudo
DOCKER="docker"
$DOCKER ps >/dev/null 2>&1 || DOCKER="sudo docker"
if $DOCKER compose version >/dev/null 2>&1; then
    COMPOSE="$DOCKER compose"
else
    COMPOSE="sudo docker-compose"
fi

echo ">> Container laufen:"
$DOCKER ps --format '   {{.Names}}\t{{.Status}}' | grep -E 'mosquitto|influxdb|grafana|robustear' || true

echo ">> Starte Bridge ..."
$COMPOSE up -d --build

echo ">> Richte Grafana ein ..."
./setup_grafana.sh || echo "!! Grafana-Setup fehlgeschlagen (siehe oben), Bridge laeuft trotzdem."

IP=$(hostname -I | awk '{print $1}')
echo
echo "=============================================================="
echo " Grafana:   http://$IP:3000/d/robustear   (Dashboard RobustEar)"
echo " InfluxDB:  http://$IP:8086"
echo " Bridge-Log jetzt live (Strg+C beendet nur die Anzeige):"
echo "=============================================================="
$COMPOSE logs -f --tail 20 bridge
