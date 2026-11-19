"""
RobustEar PC-Werkzeug: Audio vom ESP32 mithoeren, aufnehmen, aus InfluxDB holen und pruefen.

  live     MQTT live abhoeren (Lautsprecher) + Pegel/Luecken/Korrelation anzeigen,
           optional gleichzeitig als WAV speichern (--record)
  fetch    Rohaudio eines Zeitraums aus InfluxDB holen und als WAV speichern
  analyze  WAV-Datei pruefen (DC-Offset, Pegel, Clipping, L/R-Korrelation, Spektrum)

Beispiele:
  python robustear_pc.py live --host 192.168.1.55
  python robustear_pc.py live --host 192.168.1.55 --record test.wav --no-play
  python robustear_pc.py fetch --start -5m --out letzte5min.wav
  python robustear_pc.py analyze letzte5min.wav

Verbindungsdaten koennen statt als Argument auch in pc/.env stehen
(siehe pc/.env.example).

Die WAV-Dateien enthalten die unveraenderten Rohdaten (inkl. DC-Offset).
Nur die Wiedergabe ueber Lautsprecher entfernt den DC-Anteil und verstaerkt.
"""

import argparse
import base64
import os
import queue
import struct
import sys
import threading
import time
import wave
from pathlib import Path

import numpy as np

HDR = struct.Struct("<4sIQIHHI")  # magic, seq, ts_us, rate, channels, bits, frames


# ------------------------------------------------------------------ Konfiguration
def load_env():
    """Liest pc/.env (KEY=VALUE) in os.environ, ohne bestehende Werte zu ueberschreiben."""
    p = Path(__file__).with_name(".env")
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def env(name, default=None):
    v = os.environ.get(name)
    return v if v not in (None, "") else default


# ------------------------------------------------------------------ Analyse
def dbfs(x):
    return -96.0 if x < 1.0 else 20 * np.log10(x / 32768.0)


def channel_stats(x: np.ndarray) -> dict:
    x = x.astype(np.float64)
    dc = x.mean()
    ac = x - dc
    return {
        "dc": dc,
        "rms": dbfs(np.sqrt(np.mean(ac**2))) if len(ac) else -96.0,
        "peak": dbfs(np.max(np.abs(ac))) if len(ac) else -96.0,
        "clip": int(np.sum((x >= 32767) | (x <= -32768))),
    }


def correlation(l: np.ndarray, r: np.ndarray) -> float:
    l = l.astype(np.float64) - l.mean()
    r = r.astype(np.float64) - r.mean()
    d = np.sqrt(np.sum(l * l) * np.sum(r * r))
    return float(np.sum(l * r) / d) if d > 0 else 0.0


# ------------------------------------------------------------------ Wiedergabe
class Player:
    """Spielt Bloecke mit kleinem Jitter-Puffer ab. Entfernt DC, verstaerkt."""

    def __init__(self, rate, channel, gain_db, buffer_ms=200):
        import sounddevice as sd

        self.channel = channel
        self.gain = 10 ** (gain_db / 20)
        self.buf = np.zeros((0, 2), dtype=np.float32)
        self.lock = threading.Lock()
        self.prefill = int(rate * buffer_ms / 1000)
        self.started = False
        self.underruns = 0
        self.dc = np.zeros(2)
        self.stream = sd.OutputStream(
            samplerate=rate, channels=2, dtype="float32", callback=self._cb, blocksize=256
        )
        self.stream.start()

    def push(self, stereo_i16: np.ndarray):
        x = stereo_i16.astype(np.float32)
        # langsamer DC-Abgleich (nur fuer die Wiedergabe)
        self.dc = 0.95 * self.dc + 0.05 * x.mean(axis=0)
        x = (x - self.dc) * self.gain / 32768.0
        if self.channel == "L":
            x[:, 1] = x[:, 0]
        elif self.channel == "R":
            x[:, 0] = x[:, 1]
        np.clip(x, -1, 1, out=x)
        with self.lock:
            self.buf = np.concatenate([self.buf, x])
            # bei zu grossem Rueckstau aufholen (max. 1 s)
            if len(self.buf) > 5 * self.prefill:
                self.buf = self.buf[-self.prefill:]

    def _cb(self, out, frames, t, status):
        with self.lock:
            if not self.started:
                if len(self.buf) < self.prefill:
                    out.fill(0)
                    return
                self.started = True
            n = min(frames, len(self.buf))
            out[:n] = self.buf[:n]
            out[n:] = 0
            self.buf = self.buf[n:]
            if n < frames:
                self.underruns += 1
                self.started = False  # neu vorpuffern

    def close(self):
        self.stream.stop()
        self.stream.close()


# ------------------------------------------------------------------ live
def cmd_live(a):
    import paho.mqtt.client as mqtt

    topic = f"{a.base}/{a.device}/audio"
    q: queue.Queue = queue.Queue(maxsize=200)

    def on_connect(c, u, flags, rc, props=None):
        if rc.is_failure:
            print(f"MQTT Verbindung abgelehnt: {rc}")
            return
        print(f"MQTT verbunden mit {a.host}:{a.port}, abonniere {topic}")
        c.subscribe(topic)
        c.subscribe(f"{a.base}/{a.device}/status")

    def on_message(c, u, msg):
        if msg.topic.endswith("/status"):
            print(f"\n[status] {msg.topic}: {msg.payload.decode(errors='replace')}")
            return
        try:
            q.put_nowait((msg.topic.split("/")[1], msg.payload))
        except queue.Full:
            pass

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"robustear-pc-{os.getpid()}")
    if a.user:
        client.username_pw_set(a.user, a.password)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(a.host, a.port, keepalive=30)
    client.loop_start()

    player = None
    wav = None
    last_seq = None
    lost = blocks = bad = 0
    window = []
    t_print = time.time()
    t_end = time.time() + a.seconds if a.seconds else None
    rate = None

    print("Warte auf Audio ... (Strg+C zum Beenden)")
    try:
        while t_end is None or time.time() < t_end:
            try:
                dev, payload = q.get(timeout=1.0)
            except queue.Empty:
                if time.time() - t_print > 3:
                    print("\r... keine Daten (laeuft der ESP32? stimmt --device?)        ", end="")
                    t_print = time.time()
                continue

            if len(payload) < HDR.size:
                bad += 1
                continue
            magic, seq, ts, r, nch, bits, frames = HDR.unpack_from(payload)
            pcm = np.frombuffer(payload, dtype="<i2", offset=HDR.size)
            if magic != b"REA1" or bits != 16 or nch != 2 or len(pcm) != frames * nch:
                bad += 1
                continue
            stereo = pcm.reshape(-1, 2)

            if rate is None:
                rate = r
                print(f"Erster Block von '{dev}': {r} Hz, {nch} Kanaele, {frames} Frames/Block")
                if not a.no_play:
                    player = Player(rate, a.channel, a.gain)
                if a.record:
                    wav = wave.open(a.record, "wb")
                    wav.setnchannels(2)
                    wav.setsampwidth(2)
                    wav.setframerate(rate)

            if last_seq is not None and seq > last_seq + 1:
                missing = seq - last_seq - 1
                lost += missing
                if wav:  # Luecken mit Stille fuellen, damit die Zeitachse stimmt
                    wav.writeframes(np.zeros(missing * frames * 2, dtype="<i2").tobytes())
            elif last_seq is not None and seq <= last_seq:
                print(f"\n[info] seq sprang von {last_seq} auf {seq} -> ESP32 neu gestartet?")
            last_seq = seq
            blocks += 1

            if wav:
                wav.writeframes(pcm.astype("<i2").tobytes())
            if player:
                player.push(stereo)

            window.append(stereo)
            if time.time() - t_print >= 1.0:
                w = np.concatenate(window)
                window.clear()
                L, R = channel_stats(w[:, 0]), channel_stats(w[:, 1])
                corr = correlation(w[:, 0], w[:, 1])
                ur = f" underruns={player.underruns}" if player else ""
                print(
                    f"\rseq={seq:7d} bloecke={blocks:6d} verloren={lost:4d} ungueltig={bad}"
                    f" | L {L['rms']:6.1f} dBFS DC={L['dc']:7.0f}"
                    f" | R {R['rms']:6.1f} dBFS DC={R['dc']:7.0f}"
                    f" | corr={corr:+.2f} clip={L['clip'] + R['clip']}{ur}   ",
                    end="",
                )
                t_print = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        print()
        client.loop_stop()
        client.disconnect()
        if player:
            player.close()
        if wav:
            wav.close()
            print(f"Gespeichert: {a.record}")
        total = blocks + lost
        if total:
            print(f"Empfangen {blocks} Bloecke, verloren {lost} ({100 * lost / total:.1f} %), ungueltig {bad}")


# ------------------------------------------------------------------ fetch
def cmd_fetch(a):
    from influxdb_client import InfluxDBClient

    if not (a.influx_token and a.influx_org):
        sys.exit("INFLUX_TOKEN und INFLUX_ORG fehlen (Argumente oder pc/.env)")

    dev_filter = f' and r.device == "{a.device}"' if a.device != "+" else ""
    stop = f", stop: {a.stop}" if a.stop else ""
    flux = f"""
from(bucket: "{a.bucket}")
  |> range(start: {a.start}{stop})
  |> filter(fn: (r) => r._measurement == "audio"{dev_filter})
  |> filter(fn: (r) => r._field == "pcm" or r._field == "seq" or r._field == "frames" or r._field == "sample_rate")
  |> pivot(rowKey: ["_time", "device"], columnKey: ["_field"], valueColumn: "_value")
  |> group()
  |> sort(columns: ["_time"])
"""
    print(f"Hole Audio aus InfluxDB ({a.influx_url}, Bucket {a.bucket}, Start {a.start}) ...")
    with InfluxDBClient(url=a.influx_url, token=a.influx_token, org=a.influx_org, timeout=120_000) as c:
        records = c.query_api().query_stream(flux)
        wav = None
        last_seq = None
        blocks = lost = resets = 0
        devices = set()
        t_first = t_last = None
        for rec in records:
            v = rec.values
            devices.add(v.get("device"))
            if len(devices) > 1:
                sys.exit(f"Mehrere Geraete im Zeitraum: {devices} -> --device angeben")
            frames, rate, seq = int(v["frames"]), int(v["sample_rate"]), int(v["seq"])
            pcm = base64.b64decode(v["pcm"])
            if wav is None:
                wav = wave.open(a.out, "wb")
                wav.setnchannels(2)
                wav.setsampwidth(2)
                wav.setframerate(rate)
            if last_seq is not None:
                if seq > last_seq + 1:
                    missing = seq - last_seq - 1
                    lost += missing
                    if missing * frames <= rate * a.max_gap:
                        wav.writeframes(bytes(missing * frames * 4))
                elif seq <= last_seq:
                    resets += 1
            last_seq = seq
            wav.writeframes(pcm)
            blocks += 1
            t_first = t_first or v["_time"]
            t_last = v["_time"]

    if wav is None:
        sys.exit("Keine Audiodaten im Zeitraum gefunden.")
    wav.close()
    print(f"Gespeichert: {a.out}")
    print(f"  Zeitraum   {t_first}  bis  {t_last}")
    print(f"  Bloecke    {blocks}, verloren {lost} (mit Stille gefuellt), ESP-Neustarts {resets}")
    analyze_file(a.out)


# ------------------------------------------------------------------ analyze
def analyze_file(path):
    with wave.open(path, "rb") as w:
        rate, nch, sw, n = w.getframerate(), w.getnchannels(), w.getsampwidth(), w.getnframes()
        if sw != 2:
            sys.exit("Nur 16-Bit-WAV wird unterstuetzt")
        x = np.frombuffer(w.readframes(n), dtype="<i2").reshape(-1, nch)

    print(f"\nAnalyse {path}: {n / rate:.1f} s, {rate} Hz, {nch} Kanaele")
    names = ["L (Mic L/R=GND)", "R (Mic L/R=3V3)"]
    for c in range(nch):
        s = channel_stats(x[:, c])
        # dominante Frequenz (ohne DC)
        seg = x[: min(len(x), rate * 10), c].astype(np.float64)
        seg -= seg.mean()
        spec = np.abs(np.fft.rfft(seg * np.hanning(len(seg)))) if len(seg) > 16 else np.zeros(1)
        freqs = np.fft.rfftfreq(len(seg), 1 / rate) if len(seg) > 16 else np.zeros(1)
        f_dom = freqs[np.argmax(spec[1:]) + 1] if len(spec) > 1 else 0
        silent = np.all(x[:, c] == x[0, c])
        print(
            f"  {names[c] if c < 2 else c}: DC={s['dc']:8.1f}  RMS={s['rms']:6.1f} dBFS  "
            f"Peak={s['peak']:6.1f} dBFS  Clipping={s['clip']}  dominant ~{f_dom:.0f} Hz"
            + ("  !! KONSTANT -> Mikrofon/Verdrahtung pruefen" if silent else "")
        )
    if nch == 2:
        corr = correlation(x[:, 0], x[:, 1])
        print(f"  L/R-Korrelation: {corr:+.3f}", end="  ")
        if np.array_equal(x[:, 0], x[:, 1]):
            print("!! Kanaele identisch -> beide L/R-Pins gleich verdrahtet?")
        elif corr > 0.5:
            print("(gut: beide Mikrofone hoeren dasselbe Signal)")
        elif corr < -0.5:
            print("(stark negativ: ein Mikrofon phasengedreht?)")
        else:
            print("(niedrig: bei Stille normal; bei lautem Geraeusch Mikrofone pruefen)")


def cmd_analyze(a):
    analyze_file(a.file)


# ------------------------------------------------------------------ main
def main():
    load_env()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--device", default=env("RE_DEVICE", "esp32s3-01"), help="Geraete-ID ('+' = alle)")
    p.add_argument("--base", default=env("RE_MQTT_BASE", "robustear"))
    sub = p.add_subparsers(dest="cmd", required=True)

    pl = sub.add_parser("live", help="MQTT live abhoeren/aufnehmen")
    pl.add_argument("--host", default=env("RE_MQTT_HOST", "localhost"), help="IP des Raspberry Pi")
    pl.add_argument("--port", type=int, default=int(env("RE_MQTT_PORT", "1883")))
    pl.add_argument("--user", default=env("RE_MQTT_USER"))
    pl.add_argument("--password", default=env("RE_MQTT_PASS"))
    pl.add_argument("--record", metavar="DATEI.wav", help="Rohdaten zusaetzlich als WAV speichern")
    pl.add_argument("--no-play", action="store_true", help="nicht abspielen (nur anzeigen/aufnehmen)")
    pl.add_argument("--channel", choices=["L", "R", "both"], default="both", help="Wiedergabe-Kanal")
    pl.add_argument("--gain", type=float, default=20.0, help="Wiedergabe-Verstaerkung in dB (Standard 20)")
    pl.add_argument("--seconds", type=float, default=0, help="nach N Sekunden beenden")
    pl.set_defaults(func=cmd_live)

    pf = sub.add_parser("fetch", help="Zeitraum aus InfluxDB als WAV holen")
    pf.add_argument("--influx-url", default=env("RE_INFLUX_URL", "http://localhost:8086"))
    pf.add_argument("--influx-token", default=env("RE_INFLUX_TOKEN"))
    pf.add_argument("--influx-org", default=env("RE_INFLUX_ORG"))
    pf.add_argument("--bucket", default=env("RE_INFLUX_BUCKET_RAW", "robustear_raw"))
    pf.add_argument("--start", default="-5m", help="Flux-Zeit: -5m, -1h oder 2026-11-19T14:00:00Z")
    pf.add_argument("--stop", default=None, help="Flux-Zeit, Standard: jetzt")
    pf.add_argument("--max-gap", type=float, default=5.0, help="Luecken bis N s mit Stille fuellen")
    pf.add_argument("--out", default="robustear_fetch.wav")
    pf.set_defaults(func=cmd_fetch)

    pa = sub.add_parser("analyze", help="WAV-Datei pruefen")
    pa.add_argument("file")
    pa.set_defaults(func=cmd_analyze)

    a = p.parse_args()
    a.func(a)


if __name__ == "__main__":
    main()
