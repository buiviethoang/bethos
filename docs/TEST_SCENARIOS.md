# Test scenarios: Latest Merger & log-compacted flow

## Why merged / compacted messages can look "unchanged"

Input messages for the same VIN often look very similar: same structure, same sensors, small differences in values or `received_at`. After merging:

- **Per-sensor we keep the value with the latest `received_at`.** So the merged state is the union of all sensors ever seen for that VIN (within TTL), each with the newest value.
- **Flush output** is a snapshot of that state at flush time. If no new messages were consumed between two flushes, the snapshot is identical, so the compacted topic message is the same.
- **Order of keys** in JSON is undefined (Go map iteration), so two logically equal payloads can look different when stringified; they are still the same data.

So "nothing changes" between two compacted records is expected when there is no new input for that VIN between flushes.

---

## Scenario 1: Flush trigger detection

| Case | Input | Expected |
|------|--------|----------|
| 1.1 | `{"_flush":true}` | Treated as flush trigger; processor runs flush and returns batch. |
| 1.2 | `{"_flush": true}` (space) | Treated as flush trigger (JSON parse accepts it). |
| 1.3 | `{"_flush":false}` | Not flush; body parsed as payload (will fail or skip if not valid telemetry). |
| 1.4 | `{"other": 1}` | Not flush; parsed as payload. |
| 1.5 | `not json` | Not flush; unmarshal error returned. |

---

## Scenario 2: Merge by VIN and latest received_at

| Case | Input messages (same VIN) | Expected state after merge |
|------|---------------------------|----------------------------|
| 2.1 | Msg1: sensor A = {value: "1", received_at: 100}; Msg2: sensor A = {value: "2", received_at: 200} | A = {value: "2", received_at: 200}. |
| 2.2 | Msg1: A at 200; Msg2: A at 100 | A = {value from Msg1, received_at: 200} (newer wins). |
| 2.3 | Msg1: A at 100; Msg2: B at 200 (no A) | A and B both present; A from Msg1, B from Msg2. |
| 2.4 | Empty VIN (`data.id` = "") | Message skipped; no state change; log event=skip. |

---

## Scenario 3: TTL eviction on flush

| Case | State | Expected |
|------|--------|----------|
| 3.1 | VIN1 LastSeen = now - 5 min | VIN1 included in snapshot and in flush batch. |
| 3.2 | VIN1 LastSeen = now - 11 min | VIN1 evicted (not in snapshot); not in batch. |

---

## Scenario 4: Log-compacted strategy output

| Case | Snapshot (FlushState) | Expected batch |
|------|------------------------|----------------|
| 4.1 | {} | Empty batch (nil or len 0). |
| 4.2 | {VIN1: {sensor_a: {value: "1", received_at: 100}}} | One message; payload has data.id = VIN1, data.sensor_a = ...; meta vincode = VIN1. |
| 4.3 | Two VINs | Two messages; each has correct vincode meta and payload.data.id. |

---

## Scenario 5: Process error handling

| Case | Input | Expected |
|------|--------|----------|
| 5.1 | Message that fails AsBytes() | Error returned; log event=error. |
| 5.2 | Valid JSON but not a flush and not a valid Payload (e.g. missing data) | Unmarshal or merge may skip; error or nil batch as appropriate. |
| 5.3 | Valid payload with data.id = "" | Nil batch, no error; log event=skip. |

---

## Scenario 6: End-to-end (integration)

| Case | Steps | Expected |
|------|--------|----------|
| 6.1 | Produce N telemetry messages for VIN1; then flush trigger | One compacted message for VIN1 with latest value per sensor. |
| 6.2 | Produce messages for VIN1 and VIN2; flush | Two compacted messages, one per VIN. |
| 6.3 | No messages, only flush triggers | Flush returns empty batch (vin_count=0, message_count=0). |

---

## Scenario 7: Payload model (Data unmarshal)

| Case | JSON under "data" | Expected |
|------|-------------------|----------|
| 7.1 | `{"id": "VIN1", "sensor_a": {"value": "x", "received_at": 123}}` | Data.ID = "VIN1"; Metrics["sensor_a"] = {Value: "x", ReceivedAt: 123}. |
| 7.2 | `{"sensor_a": {...}, "id": "VIN1"}` (id after other keys) | Same as 7.1 (order independent). |
| 7.3 | `{"id": "VIN1"}` (no sensors) | Data.ID = "VIN1"; Metrics empty. |

These scenarios are covered by unit tests in `processors/latest_merger_test.go`, `internal/merger/compact_flush_strategy_test.go`, and `internal/model/sensor_data_test.go`.
