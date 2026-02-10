# bethos

Pipeline lấy dữ liệu telemetry từ InfluxDB, aggregate theo device (VIN), và ghi ra Kafka với bản ghi “latest” đầy đủ mỗi 2 phút. Sử dụng [Bento](https://github.com/warpstreamlabs/bento) (Redpanda Connect) làm runtime.

## Luồng tổng quát

- **Đầu vào:** InfluxDB (query định kỳ 10s theo pod + chunk), hoặc Kafka topic `sensor-service.dispatch.telemetry-aggregated` cho bước merger.
- **Đầu ra:** Kafka topic `sensor-service.dispatch.telemetry-latest-compacted` — message chứa thông tin đầy đủ và mới nhất của từng sensor theo device (VIN), emit mỗi 2 phút.

InfluxDB không có sẵn input trong Bento; project dùng custom batch input `influxdb` (chunk theo pod, lookback 10s). Với scale lớn (100k+ device, ~3M record/10s), pipeline được thiết kế để có thể scale theo consumer group và partition.

## Cấu hình Kafka

- **Brokers mặc định (dev):** `localhost:19091`, `localhost:19092`, `localhost:19093` (đổi qua biến môi trường hoặc sửa trực tiếp trong file config nếu cần).
- **Topic ETL (đầu vào merger):** `sensor-service.dispatch.telemetry-aggregated` — nên tạo với `cleanup.policy=compact` (xem [config/kafka_config](config/kafka_config)). Luồng ETL ghi vào topic compact này; pipeline consume (latest_merger) dùng `start_from_oldest: true` để khi service restart vẫn có thể consume lại từ compact topic (latest per VIN).
- **Topic đích compacted:** `sensor-service.dispatch.telemetry-latest-compacted` — cần tạo với `cleanup.policy=compact` (xem [config/kafka_config](config/kafka_config)).
- **Consumer group (log_compacted):** `bento_latest_merger_log_compacted`. Trong warpstreamlabs/bento v1.14.1, input `kafka_franz` không hỗ trợ `session_timeout` / `heartbeat_interval` / `rebalance_timeout`; nếu gặp `UNKNOWN_MEMBER_ID`, thử giảm `checkpoint_limit` hoặc dùng input `kafka` (Sarama) có các timeout nhóm.

## Bốn strategy của Latest Merger

Pipeline “latest merger” gom telemetry theo VIN và định kỳ flush ra output. Có 4 strategy, chọn qua config `latest_merger.strategy`:

| Strategy        | Mô tả | Khi nào dùng |
|----------------|-------|-------------------------------|
| **inline**     | Merge trong memory, flush trực tiếp ra output (một process). | Single process, đơn giản. |
| **log_compacted** | Merge trong memory, flush mỗi 2 phút ra Kafka topic compacted (key = VIN). | **Khuyến nghị** cho “latest đầy đủ mỗi 2 phút” — topic compacted giữ 1 bản ghi mới nhất per VIN. |
| **state_store** | Merge trong memory, khi flush chỉ ghi vào cache (Redis/memory); cần process/publisher riêng đọc cache và emit. | Tách merge và publish, dùng khi có publisher độc lập. |
| **window_stream** | Merge theo time window + allowed lateness, emit khi window đóng. | Cần semantics theo window (2 phút + late data). |

Luồng khuyến nghị cho yêu cầu “message đầy đủ và latest mỗi 2 phút” là **log_compacted** (config: [config/pipeline_log_compacted.yaml](config/pipeline_log_compacted.yaml)).

## Luồng InfluxDB → Kafka

- Input **influxdb** (batch): query InfluxDB theo danh sách **pods**, mỗi pod chia **chunk** theo thời gian (ví dụ 2s), **lookback** 10s so với thời điểm chạy.
- Dữ liệu đọc được đưa qua pipeline (aggregate theo device, build message) rồi ghi vào Kafka (ví dụ topic aggregated). Phía sau có thể dùng pipeline merger (ví dụ log_compacted) đọc topic aggregated và ghi topic latest-compacted.

Chi tiết cấu hình: `chunk_duration`, `lookback`, `batch_size`, `resource_map_path` trong config InfluxDB; với 3M rec/10s nên tune số pods và batch_size cho phù hợp.

### Fault tolerance và Idempotency (luồng InfluxDB)

- **Fault tolerance:** Input InfluxDB dùng retry với exponential backoff khi query lỗi (cấu hình `retry_max_attempts`, `retry_initial_interval`, `retry_max_interval`). Kafka output: Bento nack khi gửi thất bại và retry batch. Không dùng Redis.
- **Idempotency (Option B — mặc định):** Luồng chấp nhận at-least-once: có thể gửi nhiều lần cùng vincode (retry, restart). Topic đích `sensor-service.dispatch.telemetry-aggregated` dùng **compacted** với **key = vincode** — gửi trùng cùng vincode chỉ cập nhật bản ghi mới nhất, downstream luôn thấy một state/vincode. Đảm bảo tạo topic với `cleanup.policy=compact` và output config có `key: ${! meta("vincode") }`.
- **Idempotency (Option A — tùy chọn):** Nếu bật `checkpoint_path`, input ghi thời điểm kết thúc cycle đã xử lý thành công; restart đọc checkpoint và chỉ query từ thời điểm đó, tránh xử lý trùng time window. Phù hợp 1 replica hoặc shared volume.

## Chạy pipeline log_compacted

1. Tạo các topic (chạy trong Kafka container hoặc nơi có `kafka-topics.sh`). Xem lệnh đầy đủ trong [config/kafka_config](config/kafka_config): topic ETL `sensor-service.dispatch.telemetry-aggregated` và topic đích `sensor-service.dispatch.telemetry-latest-compacted` đều dùng `cleanup.policy=compact` (bắt buộc). Consumer dùng `start_from_oldest: true` để có thể replay an toàn khi restart.
2. Khởi chạy:
   ```bash
   BENTO_CONFIG=./config/pipeline_log_compacted.yaml go run .
   ```

Pipeline đọc từ `sensor-service.dispatch.telemetry-aggregated`, merge theo VIN, mỗi 2 phút flush ra `sensor-service.dispatch.telemetry-latest-compacted` với key = VIN.

## Varied ETL và giám sát (test merger)

Để kiểm tra logic merger với message đa dạng (cùng VIN, nhiều batch với giá trị/`received_at` khác nhau): dùng [config/pipeline_etl_varied.yaml](config/pipeline_etl_varied.yaml) (generate 6 lần, 10 VIN, mỗi tick ghi đè CSV). Chạy ETL xong rồi chạy pipeline log_compacted; xem [docs/MONITORING.md](docs/MONITORING.md) để theo dõi Kafka UI, log merger và cách verify "latest wins".

## Observability (prod)

Processor `latest_merger` ghi log có prefix `[latest_merger]` với format key=value, dễ parse (Loki, grep):

- **event=flush** — mỗi lần flush theo timer: `flush_duration_ms`, `vin_count`, `message_count`.
- **event=flush_error** — lỗi khi strategy OnFlush trả về error.
- **event=shutdown_flush** — khi process thoát (Close): flush chạy để log; `note=data_not_emitted_on_shutdown` (batch không gửi được từ Close trong Bento).
- **event=error** — lỗi khi đọc message (`as_bytes`) hoặc unmarshal payload; kèm `err=...`.
- **event=skip** — message có `data.id` rỗng (bị bỏ qua, không merge).

**Nếu `vin_count=0` liên tục:** topic có message nhưng consumer group có thể đã commit offset vượt qua các message đó (chạy cũ hoặc instance khác). Reset offset của group `bento_latest_merger_log_compacted` về earliest (Kafka UI: Consumers → group → Reset offset) rồi restart pipeline; hoặc produce message mới trong khi pipeline đang chạy.

Theo dõi thêm: **consumer lag** qua metrics Kafka/broker (consumer group `bento_latest_merger_log_compacted`). Nếu flush chậm hoặc UNKNOWN_MEMBER_ID, cân nhắc giảm `checkpoint_limit` trong config input `kafka_franz`.

## So sánh luồng merge: bethos vs bento-demo

Thư mục `bento-demo/` là một phiên bản tham khảo khác của luồng telemetry (sync.Map, batch, model merge). So sánh nhanh:

| Tiêu chí | bethos (luồng hiện tại) | bento-demo |
|----------|-------------------------|------------|
| **State** | `sync.Map` + lock per device (LatestMerger) | `sync.Map` trong input (VehicleTelemetryReceiver) |
| **Pipeline** | Tích hợp Bento đầy đủ: Kafka in → latest_merger → Kafka out | Processor pass-through; output qua channel, chưa Kafka |
| **Flush / emit** | Nhiều strategy: log_compacted, window_stream, inline, state_store | Batch theo ticker (tối đa 100 device/message), không strategy |
| **Model** | Payload + Data, ProducedAt; merge trong processor | VehicleData.Merge / Telemetry; merge ở model |
| **Benchmark** | Có: `processors/latest_merger_benchmark_test.go`, `internal/merger/compact_flush_strategy_benchmark_test.go` | Có: `pkg/models/telemetry_benchmark_test.go` |
| **Batch output** | Hỗ trợ batch (PayloadBatch, batch_size) để giảm network I/O | Cấu trúc sẵn Telemetry với `Data []VehicleData`, batch 100 |

**Điểm mạnh bethos:** Pipeline Bento rõ ràng, dễ vận hành; tách strategy dễ mở rộng; model có ProducedAt; test + benchmark đầy đủ; ETL → compact topic → consume replay.

**Điểm mạnh bento-demo:** Merge ở model (VehicleData.Merge); format batch sẵn (nhiều device/message).

**Kết luận:** Luồng hiện tại đã áp dụng ý tưởng từ bento-demo (sync.Map, benchmark, batch) và giữ kiến trúc Bento + strategy.

## Production: broker qua env

Trong [config/pipeline_log_compacted.yaml](config/pipeline_log_compacted.yaml), `seed_brokers` đang hardcode. Để dùng env trên prod: set `KAFKA_BROKER_1`, `KAFKA_BROKER_2`, `KAFKA_BROKER_3` (hoặc một biến `KAFKA_BROKERS` dạng `host1:port,host2:port`) rồi dùng config template / script (vd. `envsubst`) thay thế vào file trước khi chạy Bento; hoặc sửa trực tiếp list broker trong YAML theo môi trường.
