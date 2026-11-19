# RobustEar – Robuste Geräuscherkennung mit Edge-ML

Zwei INMP441-Mikrofone an einer Seeed XIAO ESP32-S3 streamen Rohaudio per MQTT
an einen Raspberry Pi (Mosquitto, InfluxDB, Grafana in Docker). Daraus werden
Modelle mit Rohdaten, leicht und stark gefilterten Daten trainiert und verglichen.

```
ESP32-S3 ──MQTT──> Mosquitto ──> robustear-bridge ──> InfluxDB ──> Grafana
 2x INMP441          (Pi)            (Pi, Docker)        (Pi)
                       └──────────> PC: robustear_pc.py live (Mithören)
                                    PC: robustear_pc.py fetch (WAV aus InfluxDB)
```

## Ordner

| Ordner | Inhalt |
|---|---|
| `main/` | ESP32-S3-Firmware (ESP-IDF v6.0) |
| `pi/` | MQTT→InfluxDB-Bridge als Docker-Container für den Raspberry Pi |
| `pc/` | PC-Werkzeug: live mithören, aufnehmen, aus InfluxDB holen, prüfen |

## 1. Firmware (ESP32-S3)

Verdrahtung (beide Mikrofone parallel am selben Bus):

| INMP441 | XIAO ESP32-S3 |
|---|---|
| SCK  | GPIO3 (BCLK) |
| WS   | GPIO5 (LRCL) |
| SD   | GPIO4 (DIN)  |
| VDD / GND | 3V3 / GND |
| L/R Mikrofon 1 | GND → linker Kanal |
| L/R Mikrofon 2 | 3V3 → rechter Kanal |

```
idf.py menuconfig        # "RobustEar Konfiguration": WLAN, MQTT-URI (IP des Pi), ggf. MQTT-Login
idf.py -p COM9 flash monitor
```

Beim Start prüft ein Selbsttest beide Kanäle und meldet typische Verdrahtungsfehler.

MQTT-Topics (`<id>` = Geräte-ID, Standard `esp32s3-01`):

| Topic | Inhalt |
|---|---|
| `robustear/<id>/audio`  | binär: 28-Byte-Header `REA1` + int16 L,R interleaved, 1024 Frames (64 ms) |
| `robustear/<id>/level`  | JSON: `rms_l, rms_r, peak_l, peak_r` (dBFS), `seq`, `dropped`, ~4 Hz |
| `robustear/<id>/status` | JSON: `online` / `offline` (retained, Last Will) |

Header (little endian): `magic[4]="REA1", uint32 seq, uint64 timestamp_us, uint32 sample_rate, uint16 channels, uint16 bits, uint32 frames`.

## 2. Bridge auf dem Raspberry Pi

Ordner `pi/` auf den Pi kopieren (oder Repo klonen), dann:

```
cd pi
cp .env.example .env
nano .env                       # INFLUX_ORG, INFLUX_TOKEN, ggf. MQTT_USER/MQTT_PASS
docker compose up -d --build
docker compose logs -f bridge   # alle 10 s: Blöcke/s, verlorene Blöcke
```

Die Bridge legt die Buckets `robustear` (Pegel, Status) und `robustear_raw`
(Rohaudio, Standard 48 h Aufbewahrung, ca. 300 MB/h) an, falls das Token das darf.

InfluxDB-Daten:

| Bucket | Measurement | Felder |
|---|---|---|
| `robustear_raw` | `audio` | `pcm` (base64), `seq`, `gap`, `rms_l`, `rms_r`, `esp_ts_us`, … |
| `robustear` | `level` | `rms_l`, `rms_r`, `peak_l`, `peak_r`, `dropped` |
| `robustear` | `status` | `state`, `online` |
| `robustear` | `bridge` | `blocks_per_s`, `lost_blocks`, `bad_blocks` |

Grafana-Beispiel (Flux):
```
from(bucket: "robustear")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "level" and (r._field == "rms_l" or r._field == "rms_r"))
```

## 3. PC-Werkzeug

```
cd pc
pip install -r requirements.txt
copy .env.example .env          # IP des Pi, InfluxDB-Token eintragen

python robustear_pc.py live                         # live mithören + Pegel/Lücken/Korrelation
python robustear_pc.py live --record test.wav       # dabei als WAV speichern
python robustear_pc.py live --channel L --gain 30   # nur linkes Mikrofon, lauter
python robustear_pc.py fetch --start -10m --out x.wav   # aus InfluxDB holen
python robustear_pc.py analyze x.wav                # DC, Pegel, Clipping, L/R-Korrelation
```

WAV-Dateien enthalten immer die unveränderten Rohdaten. Nur die Wiedergabe über
Lautsprecher entfernt den DC-Offset und verstärkt (`--gain`, Standard +20 dB).

**Validierung:** Bei einem lauten Geräusch vor beiden Mikrofonen sollte die
L/R-Korrelation deutlich positiv sein (> 0.5), `verloren` sollte nahe 0 bleiben,
und `clip` sollte 0 sein.
