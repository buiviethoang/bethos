# Monitoring and testing the merger flow

This doc describes how to produce **varied** messages into Kafka and how to **monitor** the merger and compacted topic so you can verify the "latest wins" logic.

---

## 1. Producing varied messages (ETL)

Use the **varied ETL** config so the same VIN gets multiple messages with different values and `received_at`:

```bash
BENTO_CONFIG=./config/pipeline_etl_varied.yaml go run .
```

What it does:

- **Generate** ticks 6 times, every 2s.
- Each tick: **csv_generator** writes 800 rows to `./data/telemetry.csv` with **truncate_before_write: true** (file is overwritten each tick, so each batch is independent).
- **num_vincodes: 10** — only 10 distinct VINs (`VF37ARFZE00000001` … `VF37ARFZE00000010`), so the same VIN appears in every batch with new random sensor values.
- **csv_reader** reads up to 2000 lines (the current tick’s 800).
- **telemetry_aggregator** + **kafka_message_builder** send one message per VIN per batch to `sensor-service.dispatch.telemetry-aggregated`.

So you get **6 batches × up to 10 VINs = up to 60 messages** on the aggregated topic, with **varied** payloads (different values and `received_at` per tick). The merger can then merge by VIN and keep the latest `received_at` per sensor.

**Optional:** Delete the CSV before a run for a clean start: `rm ./data/telemetry.csv` (or leave it; with truncate, each tick overwrites).

---

## 2. Running the merger (log-compacted)

In another terminal (or after ETL finishes), run the merger so it consumes from the aggregated topic and writes to the compacted topic:

```bash
BENTO_CONFIG=./config/pipeline_log_compacted.yaml go run .
```

- Reads from `sensor-service.dispatch.telemetry-aggregated` (consumer group `bento_latest_merger_log_compacted`).
- Every 30s (or 2m if you set `interval: "2m"`) it flushes: one message per VIN to `sensor-service.dispatch.telemetry-latest-compacted` with key = VIN.

If the consumer group has already committed past the ETL messages, **reset the group offset** so this run re-reads them (see below).

---

## 3. What to check (showcase)

### Kafka UI

1. **Topic `sensor-service.dispatch.telemetry-aggregated`**
   - **Messages**: You should see many messages (e.g. dozens). Open a few: same VIN can appear multiple times with different `data.*` and `received_at`.
   - **Key**: VIN (vincode), e.g. `VF37ARFZE00000001` — set by the ETL output `key: ${! meta("vincode") }`. Value is JSON with `num_of_data`, `data` (id + sensors), `produced_at`.

2. **Topic `sensor-service.dispatch.telemetry-latest-compacted`**
   - **Messages**: After at least one flush, you should see **one record per VIN** (e.g. 10 if you used 10 VINs in varied ETL).
   - **Key**: VIN (e.g. `VF37ARFZE00000001`).
   - **Value**: One JSON payload per device with **all** sensors merged; for each sensor the value should be the one with the **latest `received_at`** among the consumed messages.

3. **Consumers**
   - Group `bento_latest_merger_log_compacted`: check **lag** and **current offset** so you know whether the merger is up to date.

### Logs (merger)

Watch for:

- `[latest_merger] event=flush flush_duration_ms=... vin_count=... message_count=...`
  - After varied ETL + one flush: e.g. `vin_count=10` and `message_count=10`.
- `event=skip reason=empty_vin` if some messages have no `data.id`.
- `event=error` for unmarshal or other errors.

### Verifying "latest wins"

1. In the **aggregated** topic, pick one VIN and one sensor (e.g. `odometer`). Note two messages: A (older `received_at`) and B (newer `received_at`).
2. In the **compacted** topic, open the message for that VIN.
3. Check that sensor: its `value` and `received_at` should match the **newer** message (B), not A.

---

## 4. Tips

| Goal | What to do |
|------|------------|
| Re-read all aggregated messages with the merger | Reset consumer group offset to **earliest**: Kafka UI → Consumers → `bento_latest_merger_log_compacted` → Reset offset → earliest. Then restart the merger. |
| Fresh ETL data only | Delete `./data/telemetry.csv` before running `pipeline_etl_varied.yaml`. With `truncate_before_write: true`, each tick still overwrites the file. |
| Reproducible ETL data | In `pipeline_etl_varied.yaml` set `seed: 42` (or any fixed number) in `csv_generator`. |
| More/fewer VINs | Change `num_vincodes` in `pipeline_etl_varied.yaml` (e.g. 5 or 20). |
| More batches | Increase `generate.count` (e.g. 10) in `pipeline_etl_varied.yaml`. |
| Flush more/less often | In `pipeline_log_compacted.yaml` change `generate.interval` (e.g. `"15s"` or `"2m"`). |

---

## 5. One-shot test flow

1. Start Kafka (e.g. `docker compose up -d`).
2. Create the compacted topic (see [config/kafka_config](../config/kafka_config)).
3. Run varied ETL: `BENTO_CONFIG=./config/pipeline_etl_varied.yaml go run .` (wait for 6 ticks to finish).
4. Reset consumer group offset for `bento_latest_merger_log_compacted` to **earliest** (Kafka UI or CLI).
5. Run merger: `BENTO_CONFIG=./config/pipeline_log_compacted.yaml go run .`
6. Wait for at least one flush (e.g. 30s); check logs for `vin_count=10 message_count=10`.
7. In Kafka UI, open `sensor-service.dispatch.telemetry-latest-compacted` and inspect one message per VIN and compare a sensor’s `received_at` with the aggregated topic.
