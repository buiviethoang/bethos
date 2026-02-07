# Latest Merger Strategy (Kafka -> Merge -> Kafka)

Hai cách triển khai "merge theo device, mỗi 2 phút emit latest".

## Strategy 1: Inline (một pipeline)

- **Một pipeline**: Kafka (aggregated) + generate 2m -> `latest_merger` (strategy: inline) -> Kafka (destination).
- Merge và emit trong cùng luồng: mỗi 2 phút merger flush trực tiếp ra output.

**Chạy:** `BENTO_CONFIG=./config/pipeline_latest_merger.yaml go run .`

## Strategy 2: State store + luồng định kỳ (hai luồng)

1. **Luồng 1 (merger):** Cứ merge thông tin theo device (tổng hợp đầy đủ nhất); mỗi 2 phút ghi state vào **cache** (không gửi Kafka).
2. **Luồng 2 (publisher):** Định kỳ 2 phút chạy, đọc toàn bộ state mới nhất từ cache và gửi sang Kafka destination.

**Cấu hình:**

- `pipeline_latest_merger_state_store_merger.yaml`: input Kafka + generate 2m, processor `latest_merger` (strategy: state_store, cache: latest_state), output drop.
- `pipeline_latest_merger_state_store_publisher.yaml`: input generate 2m, processor `latest_state_publisher` (cache: latest_state), output Kafka.

**Cách chạy:**

- **Cùng một process (streams mode):** Dùng Bento streams để chạy cả hai stream, dùng chung cache (memory). Ví dụ thư mục streams với hai file stream và một bento.yaml có `resources.caches.latest_state: memory`.
- **Hai process:** Chạy merger và publisher trong hai terminal; dùng **Redis** làm cache trong cả hai file config (cùng connection) để chia sẻ state.
