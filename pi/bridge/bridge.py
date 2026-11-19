"""
RobustEar MQTT -> InfluxDB Bridge (laeuft als Docker-Container auf dem Raspberry Pi)

Abonniert:
  robustear/<id>/audio   binaer, Header "REA1" (28 Byte) + int16 interleaved L,R
  robustear/<id>/level   JSON mit RMS/Peak in dBFS
  robustear/<id>/status  JSON online/offline

Schreibt nach InfluxDB 2.x:
  Bucket INFLUX_BUCKET_RAW  measurement "audio":  1 Punkt pro Audio-Block
        tags:   device
        fields: pcm (base64 der int16-Rohdaten), seq, frames, sample_rate,
                channels, esp_ts_us, gap (verlorene Bloecke davor),
                rms_l, rms_r (dBFS, ohne DC)
  Bucket INFLUX_BUCKET      measurement "level"  (fuer Grafana)
                            measurement "status"
                            measurement "bridge" (Statistik der Bridge)

Warum base64 statt einem Punkt pro Sample: 16 kHz Stereo waeren 32'000 Punkte/s,
das schafft InfluxDB auf dem Pi nicht. So sind es ~16 Punkte/s und die Rohdaten
lassen sich verlustfrei wieder als WAV rekonstruieren (siehe pc/robustear_pc.py).
"""

import base64
import json
import logging
import math
import os
import signal
import struct
import sys
import threading
import time
from array import array

import paho.mqtt.client as mqtt
from influxdb_client import BucketRetentionRules, InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions

# ------------------------------------------------------------------ Konfiguration
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER") or None
MQTT_PASS = os.getenv("MQTT_PASS") or None
MQTT_BASE = os.getenv("MQTT_BASE", "robustear")

INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")
INFLUX_ORG = os.getenv("INFLUX_ORG", "")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "robustear")
INFLUX_BUCKET_RAW = os.getenv("INFLUX_BUCKET_RAW", "robustear_raw")
# Aufbewahrung der Rohdaten in Stunden (0 = unbegrenzt). ~300 MB pro Stunde!
RAW_RETENTION_H = int(os.getenv("RAW_RETENTION_HOURS", "48"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

HDR = struct.Struct("<4sIQIHHI")  # magic, seq, ts_us, rate, channels, bits, frames
assert HDR.size == 28

logging.basicConfig(
    level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout
)
log = logging.getLogger("bridge")


# ------------------------------------------------------------------ Hilfsfunktionen
def dbfs(v: float) -> float:
    return -96.0 if v < 1.0 else 20.0 * math.log10(v / 32768.0)


def rms_dbfs(samples: array, ch: int, nch: int) -> float:
    """RMS eines Kanals ohne DC-Anteil, in dBFS."""
    s = samples[ch::nch]
    n = len(s)
    if n == 0:
        return -96.0
    mean = sum(s) / n
    acc = 0.0
    for x in s:
        d = x - mean
        acc += d * d
    return dbfs(math.sqrt(acc / n))


class DeviceClock:
    """Bildet die ESP-Zeit (us seit Boot) auf Wanduhrzeit ab.

    Beim ersten Block wird ein Anker gesetzt (Empfangszeit - Blockdauer).
    Danach werden die Blockzeiten exakt aus der ESP-Zeit abgeleitet, damit die
    Bloecke in InfluxDB lueckenlos und ohne Netzwerk-Jitter hintereinander liegen.
    Neu verankert wird bei ESP-Neustart oder wenn die Abweichung > 2 s ist.
    """

    def __init__(self):
        self.offset_us = None
        self.last_esp = None

    def to_wall_us(self, esp_ts_us: int, block_us: int) -> int:
        now_us = time.time_ns() // 1000
        if self.offset_us is not None and self.last_esp is not None:
            predicted = self.offset_us + esp_ts_us
            reboot = esp_ts_us < self.last_esp
            drift = abs(predicted - (now_us - block_us))
            if reboot or drift > 2_000_000:
                log.info("Zeitbasis neu gesetzt (reboot=%s, drift=%.2fs)", reboot, drift / 1e6)
                self.offset_us = None
        if self.offset_us is None:
            self.offset_us = now_us - block_us - esp_ts_us
        self.last_esp = esp_ts_us
        return self.offset_us + esp_ts_us


class Bridge:
    def __init__(self):
        self.influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=20_000)
        self.write = self.influx.write_api(
            write_options=WriteOptions(batch_size=200, flush_interval=1_000, retry_interval=2_000)
        )
        self.clocks: dict[str, DeviceClock] = {}
        self.last_seq: dict[str, int] = {}
        self.stats = {"blocks": 0, "gaps": 0, "bad": 0, "level": 0}
        self.lock = threading.Lock()

    # ---------------------------------------------------------- InfluxDB
    def ensure_buckets(self):
        api = self.influx.buckets_api()
        for name, hours in ((INFLUX_BUCKET, 0), (INFLUX_BUCKET_RAW, RAW_RETENTION_H)):
            try:
                if api.find_bucket_by_name(name):
                    log.info("Bucket '%s' vorhanden", name)
                    continue
                rules = [BucketRetentionRules(type="expire", every_seconds=hours * 3600)] if hours else []
                api.create_bucket(bucket_name=name, org=INFLUX_ORG, retention_rules=rules)
                log.info("Bucket '%s' angelegt (Aufbewahrung: %s)", name, f"{hours} h" if hours else "unbegrenzt")
            except Exception as e:  # Token ohne Rechte -> Bucket manuell anlegen
                log.warning("Bucket '%s' konnte nicht geprueft/angelegt werden: %s", name, e)

    # ---------------------------------------------------------- MQTT
    def on_connect(self, client, userdata, flags, rc, props=None):
        if rc.is_failure:
            log.error("MQTT Verbindung abgelehnt: %s", rc)
            return
        log.info("MQTT verbunden mit %s:%d", MQTT_HOST, MQTT_PORT)
        for t in ("audio", "level", "status"):
            client.subscribe(f"{MQTT_BASE}/+/{t}", qos=0)

    def on_disconnect(self, client, userdata, flags, rc, props=None):
        log.warning("MQTT getrennt (%s), verbinde neu ...", rc)

    def on_message(self, client, userdata, msg):
        parts = msg.topic.split("/")
        if len(parts) != 3:
            return
        _, device, kind = parts
        try:
            if kind == "audio":
                self.handle_audio(device, msg.payload)
            elif kind == "level":
                self.handle_level(device, msg.payload)
            elif kind == "status":
                self.handle_status(device, msg.payload)
        except Exception:
            log.exception("Fehler bei %s", msg.topic)

    def handle_audio(self, device: str, payload: bytes):
        if len(payload) < HDR.size:
            self.stats["bad"] += 1
            return
        magic, seq, ts_us, rate, nch, bits, frames = HDR.unpack_from(payload)
        pcm = payload[HDR.size:]
        if magic != b"REA1" or bits != 16 or len(pcm) != frames * nch * 2:
            self.stats["bad"] += 1
            log.warning("Ungueltiger Block von %s (magic=%r bits=%d len=%d)", device, magic, bits, len(pcm))
            return

        # Verlorene Bloecke zaehlen (seq-Luecke). Ruecksprung = ESP-Neustart.
        prev = self.last_seq.get(device)
        gap = 0
        if prev is not None and seq > prev + 1:
            gap = seq - prev - 1
            self.stats["gaps"] += gap
        self.last_seq[device] = seq

        block_us = frames * 1_000_000 // rate
        t_us = self.clocks.setdefault(device, DeviceClock()).to_wall_us(ts_us, block_us)

        samples = array("h")
        samples.frombytes(pcm)
        if sys.byteorder != "little":
            samples.byteswap()

        p = (
            Point("audio")
            .tag("device", device)
            .field("pcm", base64.b64encode(pcm).decode("ascii"))
            .field("seq", int(seq))
            .field("frames", int(frames))
            .field("sample_rate", int(rate))
            .field("channels", int(nch))
            .field("esp_ts_us", int(ts_us))
            .field("gap", int(gap))
            .field("rms_l", round(rms_dbfs(samples, 0, nch), 2))
            .field("rms_r", round(rms_dbfs(samples, 1, nch), 2) if nch > 1 else -96.0)
            .time(t_us, WritePrecision.US)
        )
        self.write.write(bucket=INFLUX_BUCKET_RAW, record=p)
        self.stats["blocks"] += 1

    def handle_level(self, device: str, payload: bytes):
        d = json.loads(payload)
        p = Point("level").tag("device", device)
        for k in ("rms_l", "rms_r", "peak_l", "peak_r"):
            if k in d:
                p = p.field(k, float(d[k]))
        for k in ("seq", "dropped"):
            if k in d:
                p = p.field(k, int(d[k]))
        self.write.write(bucket=INFLUX_BUCKET, record=p.time(time.time_ns(), WritePrecision.NS))
        self.stats["level"] += 1

    def handle_status(self, device: str, payload: bytes):
        d = json.loads(payload)
        state = str(d.get("state", "unknown"))
        log.info("Status %s: %s", device, state)
        p = (
            Point("status")
            .tag("device", device)
            .field("state", state)
            .field("online", 1 if state == "online" else 0)
            .time(time.time_ns(), WritePrecision.NS)
        )
        self.write.write(bucket=INFLUX_BUCKET, record=p)

    # ---------------------------------------------------------- Statistik
    def stats_loop(self, stop: threading.Event):
        last = dict(self.stats)
        while not stop.wait(10):
            s = dict(self.stats)
            d = {k: s[k] - last[k] for k in s}
            last = s
            log.info(
                "10s: %d Audio-Bloecke (%.1f/s), %d verloren, %d ungueltig, %d Pegel",
                d["blocks"], d["blocks"] / 10, d["gaps"], d["bad"], d["level"],
            )
            try:
                self.write.write(
                    bucket=INFLUX_BUCKET,
                    record=Point("bridge")
                    .field("blocks_per_s", d["blocks"] / 10)
                    .field("lost_blocks", int(d["gaps"]))
                    .field("bad_blocks", int(d["bad"]))
                    .time(time.time_ns(), WritePrecision.NS),
                )
            except Exception as e:
                log.warning("Statistik nicht geschrieben: %s", e)

    def close(self):
        self.write.close()  # restliche Punkte schreiben
        self.influx.close()


def main():
    if not INFLUX_TOKEN or not INFLUX_ORG:
        log.error("INFLUX_TOKEN und INFLUX_ORG muessen gesetzt sein (siehe .env)")
        sys.exit(1)

    bridge = Bridge()
    bridge.ensure_buckets()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="robustear-bridge")
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = bridge.on_connect
    client.on_disconnect = bridge.on_disconnect
    client.on_message = bridge.on_message
    client.reconnect_delay_set(1, 10)

    stop = threading.Event()
    threading.Thread(target=bridge.stats_loop, args=(stop,), daemon=True).start()

    def shutdown(*_):
        log.info("Beende ...")
        stop.set()
        client.disconnect()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
            break
        except OSError as e:
            log.warning("MQTT nicht erreichbar (%s), neuer Versuch in 3 s", e)
            time.sleep(3)

    client.loop_forever(retry_first_connection=True)
    bridge.close()


if __name__ == "__main__":
    main()
