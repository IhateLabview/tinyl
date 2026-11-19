#!/usr/bin/env bash
# Legt in Grafana die InfluxDB-Datenquelle "RobustEar InfluxDB" an (oder aktualisiert sie)
# und importiert das Dashboard grafana/robustear-dashboard.json.
# Kann beliebig oft ausgefuehrt werden.
set -euo pipefail
cd "$(dirname "$0")"
set -a; . ./.env; set +a

PY=${PY:-python3}
GRAFANA_URL=${GRAFANA_URL:-http://localhost:3000}
GRAFANA_USER=${GRAFANA_USER:-admin}
GRAFANA_PASS=${GRAFANA_PASS:-admin}
# Grafana laeuft in einem eigenen Container: "localhost" waere dort der Grafana-Container
# selbst. Deshalb die LAN-IP des Pi verwenden (Port 8086 ist veroeffentlicht).
INFLUX_URL_FOR_GRAFANA=${INFLUX_URL_FOR_GRAFANA:-http://$(hostname -I | awk '{print $1}'):8086}

auth=(-u "$GRAFANA_USER:$GRAFANA_PASS" -H "Content-Type: application/json" -s)

ds=$($PY - <<EOF
import json
print(json.dumps({
  "name": "RobustEar InfluxDB", "uid": "robustear-influx", "type": "influxdb",
  "access": "proxy", "url": "$INFLUX_URL_FOR_GRAFANA",
  "jsonData": {"version": "Flux", "organization": "$INFLUX_ORG",
               "defaultBucket": "${INFLUX_BUCKET:-robustear}"},
  "secureJsonData": {"token": "$INFLUX_TOKEN"}}))
EOF
)

code=$(curl "${auth[@]}" -o /dev/null -w '%{http_code}' "$GRAFANA_URL/api/datasources/uid/robustear-influx")
if [ "$code" = "200" ]; then
    curl "${auth[@]}" -X PUT -d "$ds" "$GRAFANA_URL/api/datasources/uid/robustear-influx" -o /dev/null -w '   Datenquelle aktualisiert (%{http_code})\n'
elif [ "$code" = "404" ]; then
    curl "${auth[@]}" -X POST -d "$ds" "$GRAFANA_URL/api/datasources" -o /dev/null -w '   Datenquelle angelegt (%{http_code})\n'
else
    echo "!! Grafana-Anmeldung fehlgeschlagen (HTTP $code). GRAFANA_USER/GRAFANA_PASS in .env pruefen."
    exit 1
fi

# Verbindungstest der Datenquelle
curl "${auth[@]}" "$GRAFANA_URL/api/datasources/uid/robustear-influx/health" \
  | $PY -c 'import json,sys; d=json.load(sys.stdin); print("   Datenquellen-Test:", d.get("status"), "-", d.get("message"))'

$PY - "$GRAFANA_URL" <<'EOF' > /tmp/robustear_dash.json
import json, sys
d = json.load(open("grafana/robustear-dashboard.json", encoding="utf-8"))
d.pop("id", None)
print(json.dumps({"dashboard": d, "overwrite": True, "folderUid": ""}))
EOF
curl "${auth[@]}" -X POST -d @/tmp/robustear_dash.json "$GRAFANA_URL/api/dashboards/db" \
  | $PY -c 'import json,sys; d=json.load(sys.stdin); print("   Dashboard:", d.get("status"), d.get("url", d.get("message")))'
rm -f /tmp/robustear_dash.json
