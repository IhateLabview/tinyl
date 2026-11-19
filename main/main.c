/*
 * RobustEar – ESP32-S3 (Seeed XIAO) Firmware
 *
 * Liest zwei INMP441 I2S-MEMS-Mikrofone gleichzeitig (Stereo auf einem I2S-Bus)
 * und sendet die Rohdaten per MQTT an den Raspberry Pi.
 *
 * Verdrahtung (beide Mikrofone parallel am selben Bus):
 *   INMP441 SCK  -> GPIO3  (BCLK)
 *   INMP441 WS   -> GPIO5  (LRCL)
 *   INMP441 SD   -> GPIO4  (DOUT)
 *   INMP441 VDD  -> 3V3, GND -> GND
 *   Mikrofon A: L/R -> GND  => linker Kanal
 *   Mikrofon B: L/R -> 3V3  => rechter Kanal
 *
 * MQTT-Topics (<id> = CONFIG_RE_DEVICE_ID):
 *   robustear/<id>/audio   binaer: Header (28 Byte) + int16 interleaved L,R
 *   robustear/<id>/level   JSON:   RMS/Peak pro Kanal in dBFS (~4 Hz, fuer Grafana)
 *   robustear/<id>/status  JSON:   online/offline (retained, Last Will)
 *
 * Bewusst KEIN Filter auf dem ESP32: die Daten sind "roh" (inkl. DC-Offset),
 * damit auf dem Pi alle Varianten (roh / leicht / stark gefiltert) aus
 * denselben Daten erzeugt werden koennen.
 */

#include <math.h>
#include <stdio.h>
#include <string.h>

#include "freertos/FreeRTOS.h"
#include "freertos/event_groups.h"
#include "freertos/queue.h"
#include "freertos/task.h"

#include "driver/i2s_std.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_netif.h"
#include "esp_timer.h"
#include "esp_wifi.h"
#include "mqtt_client.h"
#include "nvs_flash.h"

static const char *TAG = "robustear";

/* ---------- Konfiguration ---------- */
#define SAMPLE_RATE     CONFIG_RE_SAMPLE_RATE
#define FRAMES_PER_MSG  CONFIG_RE_FRAMES_PER_MSG
#define NUM_CHANNELS    2
#define POOL_BLOCKS     8                 /* Puffer zwischen I2S und MQTT */
#define LEVEL_EVERY_N   4                 /* Pegel-JSON jede N-te Nachricht */

#define RE_STR2(x) #x
#define RE_STR(x)  RE_STR2(x)

#define TOPIC_BASE      "robustear/" CONFIG_RE_DEVICE_ID
#define TOPIC_AUDIO     TOPIC_BASE "/audio"
#define TOPIC_LEVEL     TOPIC_BASE "/level"
#define TOPIC_STATUS    TOPIC_BASE "/status"

/* ---------- Binaerformat einer Audio-Nachricht (little endian) ---------- */
typedef struct __attribute__((packed)) {
    char     magic[4];      /* "REA1" */
    uint32_t seq;           /* fortlaufende Nummer, Luecken = verlorene Bloecke */
    uint64_t timestamp_us;  /* esp_timer beim ersten Sample des Blocks */
    uint32_t sample_rate;
    uint16_t channels;      /* 2 */
    uint16_t bits;          /* 16 */
    uint32_t frames;        /* Anzahl Stereo-Frames */
} audio_hdr_t;              /* 28 Byte */

typedef struct {
    audio_hdr_t hdr;
    int16_t     pcm[FRAMES_PER_MSG * NUM_CHANNELS];
} audio_block_t;

static audio_block_t s_pool[POOL_BLOCKS];
static QueueHandle_t s_free_q;    /* leere Bloecke */
static QueueHandle_t s_full_q;    /* gefuellte Bloecke */

static i2s_chan_handle_t        s_rx;
static esp_mqtt_client_handle_t s_mqtt;
static EventGroupHandle_t       s_events;
#define WIFI_UP_BIT  BIT0
#define MQTT_UP_BIT  BIT1

static volatile uint32_t s_dropped;    /* verworfen, weil Netzwerk zu langsam */
static uint32_t s_sent;                /* erfolgreich an MQTT uebergeben */
static uint32_t s_pub_fail;            /* publish fehlgeschlagen / nicht verbunden */

/* ======================= WLAN ======================= */
static void wifi_event_handler(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    if (base == WIFI_EVENT && id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED) {
        xEventGroupClearBits(s_events, WIFI_UP_BIT);
        ESP_LOGW(TAG, "WLAN getrennt, verbinde neu ...");
        vTaskDelay(pdMS_TO_TICKS(1000));
        esp_wifi_connect();
    } else if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *e = (ip_event_got_ip_t *)data;
        ESP_LOGI(TAG, "IP: " IPSTR, IP2STR(&e->ip_info.ip));
        xEventGroupSetBits(s_events, WIFI_UP_BIT);
    }
}

static void wifi_init(void)
{
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));
    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, wifi_event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, wifi_event_handler, NULL));

    wifi_config_t wc = { 0 };
    strncpy((char *)wc.sta.ssid, CONFIG_RE_WIFI_SSID, sizeof(wc.sta.ssid));
    strncpy((char *)wc.sta.password, CONFIG_RE_WIFI_PASSWORD, sizeof(wc.sta.password));
    wc.sta.threshold.authmode = WIFI_AUTH_WPA2_PSK;

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wc));
    ESP_ERROR_CHECK(esp_wifi_start());
    esp_wifi_set_ps(WIFI_PS_NONE);   /* Stromsparen aus -> stabiler Durchsatz */
}

/* ======================= MQTT ======================= */
static void mqtt_event_handler(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    switch ((esp_mqtt_event_id_t)id) {
    case MQTT_EVENT_CONNECTED:
        ESP_LOGI(TAG, "MQTT verbunden");
        esp_mqtt_client_publish(s_mqtt, TOPIC_STATUS,
            "{\"state\":\"online\",\"sample_rate\":" RE_STR(SAMPLE_RATE) ",\"channels\":2}",
            0, 1, 1);
        xEventGroupSetBits(s_events, MQTT_UP_BIT);
        break;
    case MQTT_EVENT_DISCONNECTED:
        ESP_LOGW(TAG, "MQTT getrennt");
        xEventGroupClearBits(s_events, MQTT_UP_BIT);
        break;
    case MQTT_EVENT_ERROR:
        ESP_LOGE(TAG, "MQTT Fehler");
        break;
    default:
        break;
    }
}

static void mqtt_init(void)
{
    esp_mqtt_client_config_t cfg = {
        .broker.address.uri = CONFIG_RE_MQTT_URI,
        .credentials.client_id = "robustear-" CONFIG_RE_DEVICE_ID,
        .session.last_will = {
            .topic  = TOPIC_STATUS,
            .msg    = "{\"state\":\"offline\"}",
            .qos    = 1,
            .retain = 1,
        },
        .session.keepalive = 10,
        .buffer.size = sizeof(audio_block_t) + 256,
        .network.reconnect_timeout_ms = 2000,
    };
    s_mqtt = esp_mqtt_client_init(&cfg);
    esp_mqtt_client_register_event(s_mqtt, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(s_mqtt);
}

/* ======================= I2S ======================= */
static void i2s_init(void)
{
    i2s_chan_config_t chan_cfg = I2S_CHANNEL_DEFAULT_CONFIG(I2S_NUM_0, I2S_ROLE_MASTER);
    chan_cfg.dma_desc_num  = 8;
    chan_cfg.dma_frame_num = 256;     /* 8 x 256 Frames = 128 ms DMA-Puffer */
    ESP_ERROR_CHECK(i2s_new_channel(&chan_cfg, NULL, &s_rx));

    /* INMP441: Philips-I2S, 24 Bit Daten in 32-Bit-Slot, MSB zuerst */
    i2s_std_config_t std_cfg = {
        .clk_cfg  = I2S_STD_CLK_DEFAULT_CONFIG(SAMPLE_RATE),
        .slot_cfg = I2S_STD_PHILIPS_SLOT_DEFAULT_CONFIG(I2S_DATA_BIT_WIDTH_32BIT, I2S_SLOT_MODE_STEREO),
        .gpio_cfg = {
            .mclk = I2S_GPIO_UNUSED,
            .bclk = CONFIG_RE_GPIO_BCLK,
            .ws   = CONFIG_RE_GPIO_WS,
            .dout = I2S_GPIO_UNUSED,
            .din  = CONFIG_RE_GPIO_DIN,
            .invert_flags = { 0 },
        },
    };
    std_cfg.slot_cfg.slot_mask = I2S_STD_SLOT_BOTH;

    ESP_ERROR_CHECK(i2s_channel_init_std_mode(s_rx, &std_cfg));
    ESP_ERROR_CHECK(i2s_channel_enable(s_rx));
    ESP_LOGI(TAG, "I2S gestartet: %d Hz, BCLK=%d WS=%d DIN=%d",
             SAMPLE_RATE, CONFIG_RE_GPIO_BCLK, CONFIG_RE_GPIO_WS, CONFIG_RE_GPIO_DIN);
}

/* Selbsttest: prueft die rohen 32-Bit-I2S-Worte eines Blocks pro Kanal.
 * Typische Fehlerbilder:
 *   alles 0          -> SD-Leitung nicht verbunden / Mikrofon ohne Strom / L/R falsch
 *   alles -1 (0xFF..)-> SD-Leitung haengt auf HIGH
 *   konstanter Wert  -> Takt (BCLK/WS) fehlt
 *   untere 8 Bit != 0-> Datenformat stimmt nicht (INMP441 liefert 24 Bit)
 *   beide Kanaele identisch -> beide L/R-Pins gleich verdrahtet */
static void i2s_selftest(const int32_t *raw, int frames)
{
    const char *name[2] = { "LINKS (L/R=GND)", "RECHTS (L/R=3V3)" };
    bool identical = true;
    for (int c = 0; c < 2; c++) {
        int32_t mn = INT32_MAX, mx = INT32_MIN;
        int zeros = 0, ones = 0, lowbits = 0;
        int64_t sum = 0;
        for (int i = 0; i < frames; i++) {
            int32_t v = raw[2 * i + c];
            if (v < mn) mn = v;
            if (v > mx) mx = v;
            if (v == 0) zeros++;
            if (v == -1) ones++;
            if (v & 0xFF) lowbits++;
            sum += v >> 8;
            if (c == 0 && raw[2 * i] != raw[2 * i + 1]) identical = false;
        }
        const char *verdict = "OK";
        if (zeros == frames)               verdict = "FEHLER: nur 0 -> SD/Strom/L-R-Pin pruefen";
        else if (ones == frames)           verdict = "FEHLER: nur -1 -> SD-Leitung haengt auf HIGH";
        else if (mn == mx)                 verdict = "FEHLER: konstant -> BCLK/WS pruefen";
        else if (lowbits > frames / 2)     verdict = "WARNUNG: untere 8 Bit belegt -> Format pruefen";
        ESP_LOGI(TAG, "Selbsttest %-17s min=%9ld max=%9ld mittel(24bit)=%7ld  %s",
                 name[c], (long)(mn >> 8), (long)(mx >> 8), (long)(sum / frames), verdict);
    }
    if (identical) {
        ESP_LOGW(TAG, "Selbsttest: beide Kanaele identisch -> sind beide L/R-Pins gleich verdrahtet?");
    }
}

/* Liest I2S (32 Bit) und wandelt in int16 um. Laeuft immer, auch ohne MQTT. */
static void audio_task(void *arg)
{
    static int32_t raw[FRAMES_PER_MSG * NUM_CHANNELS];
    uint32_t seq = 0;

    /* Die ersten ~100 ms verwerfen (Einschwingen des INMP441) */
    size_t n;
    for (int i = 0; i < 2; i++) {
        i2s_channel_read(s_rx, raw, sizeof(raw), &n, portMAX_DELAY);
    }
    i2s_selftest(raw, n / (NUM_CHANNELS * sizeof(int32_t)));

    while (1) {
        size_t got = 0;
        uint8_t *dst = (uint8_t *)raw;
        while (got < sizeof(raw)) {
            if (i2s_channel_read(s_rx, dst + got, sizeof(raw) - got, &n, portMAX_DELAY) == ESP_OK) {
                got += n;
            }
        }
        /* Zeitstempel ~ erstes Sample des Blocks (Ende minus Blocklaenge) */
        int64_t ts = esp_timer_get_time() - (int64_t)FRAMES_PER_MSG * 1000000 / SAMPLE_RATE;

        audio_block_t *blk;
        if (xQueueReceive(s_free_q, &blk, 0) != pdTRUE) {
            s_dropped++;          /* Netzwerk zu langsam -> Block verwerfen */
            seq++;
            continue;
        }

        /* 24-Bit-Wert steht MSB-aligned im 32-Bit-Wort -> obere 16 Bit nehmen */
        for (int i = 0; i < FRAMES_PER_MSG * NUM_CHANNELS; i++) {
            blk->pcm[i] = (int16_t)(raw[i] >> 16);
        }

        memcpy(blk->hdr.magic, "REA1", 4);
        blk->hdr.seq          = seq++;
        blk->hdr.timestamp_us = (uint64_t)ts;
        blk->hdr.sample_rate  = SAMPLE_RATE;
        blk->hdr.channels     = NUM_CHANNELS;
        blk->hdr.bits         = 16;
        blk->hdr.frames       = FRAMES_PER_MSG;

        xQueueSend(s_full_q, &blk, portMAX_DELAY);
    }
}

static float to_dbfs(double v)
{
    return (v < 1.0) ? -96.0f : (float)(20.0 * log10(v / 32768.0));
}

/* Sendet gefuellte Bloecke per MQTT, berechnet nebenbei den Pegel. */
static void mqtt_task(void *arg)
{
    audio_block_t *blk;
    double sum_l = 0, sum_r = 0;
    int peak_l = 0, peak_r = 0;
    uint32_t cnt = 0, frames_acc = 0;
    char json[200];

    while (1) {
        xQueueReceive(s_full_q, &blk, portMAX_DELAY);

        /* Pegel (fuer Grafana) – DC wird hier fuer die Anzeige entfernt */
        double mean_l = 0, mean_r = 0;
        for (int i = 0; i < FRAMES_PER_MSG; i++) {
            mean_l += blk->pcm[2 * i];
            mean_r += blk->pcm[2 * i + 1];
        }
        mean_l /= FRAMES_PER_MSG;
        mean_r /= FRAMES_PER_MSG;
        for (int i = 0; i < FRAMES_PER_MSG; i++) {
            double l = blk->pcm[2 * i] - mean_l;
            double r = blk->pcm[2 * i + 1] - mean_r;
            sum_l += l * l;
            sum_r += r * r;
            if (fabs(l) > peak_l) peak_l = (int)fabs(l);
            if (fabs(r) > peak_r) peak_r = (int)fabs(r);
        }
        frames_acc += FRAMES_PER_MSG;

        EventBits_t bits = xEventGroupGetBits(s_events);
        if ((bits & MQTT_UP_BIT) &&
            esp_mqtt_client_publish(s_mqtt, TOPIC_AUDIO, (const char *)blk,
                                    sizeof(audio_block_t), 0, 0) >= 0) {
            s_sent++;
        } else {
            s_pub_fail++;
        }

        if (++cnt >= LEVEL_EVERY_N) {
            if (bits & MQTT_UP_BIT) {
                snprintf(json, sizeof(json),
                    "{\"rms_l\":%.1f,\"rms_r\":%.1f,\"peak_l\":%.1f,\"peak_r\":%.1f,"
                    "\"seq\":%lu,\"dropped\":%lu}",
                    to_dbfs(sqrt(sum_l / frames_acc)), to_dbfs(sqrt(sum_r / frames_acc)),
                    to_dbfs(peak_l), to_dbfs(peak_r),
                    (unsigned long)blk->hdr.seq, (unsigned long)s_dropped);
                esp_mqtt_client_publish(s_mqtt, TOPIC_LEVEL, json, 0, 0, 0);
            }
            /* Statistik ca. alle 2 s im Serial-Monitor */
            static int log_div;
            if (++log_div >= 8) {
                log_div = 0;
                ESP_LOGI(TAG, "seq=%lu gesendet=%lu verworfen=%lu nicht_gesendet=%lu  "
                         "RMS L=%.1f R=%.1f dBFS  WLAN=%s MQTT=%s",
                         (unsigned long)blk->hdr.seq, (unsigned long)s_sent,
                         (unsigned long)s_dropped, (unsigned long)s_pub_fail,
                         to_dbfs(sqrt(sum_l / frames_acc)), to_dbfs(sqrt(sum_r / frames_acc)),
                         (bits & WIFI_UP_BIT) ? "ok" : "--", (bits & MQTT_UP_BIT) ? "ok" : "--");
            }
            sum_l = sum_r = 0;
            peak_l = peak_r = 0;
            cnt = frames_acc = 0;
        }

        xQueueSend(s_free_q, &blk, portMAX_DELAY);
    }
}

/* ======================= main ======================= */
void app_main(void)
{
    esp_err_t err = nvs_flash_init();
    if (err == ESP_ERR_NVS_NO_FREE_PAGES || err == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        err = nvs_flash_init();
    }
    ESP_ERROR_CHECK(err);

    s_events = xEventGroupCreate();
    s_free_q = xQueueCreate(POOL_BLOCKS, sizeof(audio_block_t *));
    s_full_q = xQueueCreate(POOL_BLOCKS, sizeof(audio_block_t *));
    for (int i = 0; i < POOL_BLOCKS; i++) {
        audio_block_t *p = &s_pool[i];
        xQueueSend(s_free_q, &p, 0);
    }

    i2s_init();
    wifi_init();
    mqtt_init();

    /* Audio auf Core 1 (hohe Prio), Netzwerk auf Core 0 */
    xTaskCreatePinnedToCore(audio_task, "audio", 4096, NULL, 10, NULL, 1);
    xTaskCreatePinnedToCore(mqtt_task,  "mqtt_tx", 6144, NULL, 5, NULL, 0);
}
