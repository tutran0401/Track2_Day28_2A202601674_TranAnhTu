# ANSWERS — Day 28 Track 2 (làm cá nhân — Trần Anh Tú, 2A202601674)

## 0. Phạm vi đã chạy thật

Chạy trên máy cá nhân (Windows, Docker Desktop, 16 GB RAM / 16 CPU, ~37 GB đĩa
trống, mạng không ổn định — nhiều lượt tải >100 MB bị đứt giữa chừng).

- **Đã chạy `docker compose up` (KHÔNG `--profile full`)**: `kafka`, `api`,
  `gateway`, `feast`, `qdrant`, `mlflow`, `prometheus`, `grafana`, `jaeger`,
  `otel-collector`, `kafka-exporter`, `pushgateway`, `runtime-init` — 12/12
  container `Up`/`healthy`.
- **Chưa chạy được `--profile full`**: image `airflow` build lỗi 2 lần liên
  tiếp khi tải `pyspark==4.2.0` (450 MB) — `IncompleteRead` do kết nối mạng bị
  ngắt giữa chừng (không phải lỗi code hay lỗi cấu hình). Do đó **không có**
  Airflow, Spark Connect, Delta Lake thật trong lần chạy này.
- **Không có vLLM thật** (không có GPU/Kaggle endpoint trong phiên làm việc
  này) — đúng theo quy tắc của bài: báo `UNVERIFIED`/`not_ready`, không giả
  lập server.

## 1. 4 hàm bắt buộc (`src/lab28_platform/integration_tasks.py`)

Đã hoàn thiện cả 4 hàm, không sửa test, không che `NotImplementedError`:

- `event_headers` — luôn có `idempotency-key`; chỉ thêm `traceparent` khi có,
  không gửi chuỗi rỗng.
- `dedupe_latest` — gom theo `idempotency_key`, giữ bản ghi có
  `(occurred_at, event_id)` lớn nhất, sắp xếp kết quả theo khóa để kết quả
  tái lập.
- `feast_online_request` — dùng `FEATURE_REFS` từ `contracts.py`, không viết
  lại danh sách feature ở nơi khác.
- `readiness_status` — ưu tiên: có lỗi `mandatory=true` → `not_ready`; còn lại
  có lỗi không bắt buộc → `degraded`; không có lỗi → `ready`.

**Kiểm chứng:** 87/87 test (`starter-tests` + `tests`) pass; `ruff check .`
sạch; `scripts/verify_matrix.py`, `scripts/check_portability.py`,
`scripts/validate_manifests.py` đều pass mã 0.

## 2. Evidence thu được (live, từ hệ thống thật đang chạy)

| IP | Trạng thái | Ghi chú |
|---|---|---|
| IP01 | ✅ `evidence/ip01-kafka-consume.json` | Consume trực tiếp topic `data.raw`; message có header `traceparent` + `idempotency-key` — xác nhận `event_headers` hoạt động đúng trên Kafka thật. |
| IP02 | ❌ Không có | Cần Airflow — build lỗi do mạng (xem mục 0). |
| IP03 | ❌ Không có | Cần Spark Connect + Airflow để ghi Delta. `lab28 evidence` tự báo lỗi `LakehouseUnavailable: no Delta table`. |
| IP04 | ⚠️ Đã kiểm tra, `NOT_FOUND` | Feast `/get-online-features` trả `PRESENT` cho `asker_id` nhưng `NOT_FOUND` cho mọi feature — đúng dự kiến vì chưa có bước materialize (cần Delta/Airflow). Không có "online row" thật để làm evidence. |
| IP05 | ✅ `evidence/ip05-qdrant-search.json` | `lab28 index --source file`: 13/13 tài liệu upsert vào Qdrant, model `paraphrase-multilingual-MiniLM-L12-v2`. |
| IP06 | ✅ `evidence/ip06-mlflow-release.json` | `lab28-rag-release` version 2, alias `champion`, có `run_id`. |
| IP07 | ✅ (negative evidence) `evidence/ip07-vllm-identity.json` | `reachable: false`, `is_real_vllm: false` — đúng vì không có GPU/vLLM thật trong phiên này. Gate GPU: `UNVERIFIED`, không giả lập theo đúng quy tắc bài. |
| IP08 | ✅ `evidence/ip08-gateway.json` | `/health` → 200 kèm `x-request-id`; burst 80 request đồng thời qua gateway → 429 `local_rate_limited` kèm `x-request-id` — rate limit Envoy hoạt động thật. |
| IP09 | ✅ `evidence/ip09-prometheus-targets.json` + `ip09-grafana-dashboards.json` | 9/10 job Prometheus `up` (`lab28-vllm-optional` down — đúng dự kiến); 1 dashboard Grafana đã provision (`Lab 28 Platform Overview`). |
| IP10 | ⚠️ Một phần `evidence/ip10-trace.json` | Trace `/api/v1/ask` chứa 6/11 span bắt buộc trong **một** trace (`lab28.gateway.request`, `lab28.api.ask`, `lab28.feast.get_online_features`, `lab28.qdrant.query`, `lab28.mlflow.resolve_release`, `lab28.vllm.chat_completion`); một trace `/feedback` khác chứa thêm `lab28.api.ingest`, `lab28.kafka.produce`. Tổng cộng 8/11 span xuất hiện trên hệ thống, nhưng gate yêu cầu **một trace duy nhất** đi qua toàn bộ 11 span (bao gồm `lab28.kafka.consume`, `lab28.airflow.dag`, `lab28.spark.delta_merge`) — không đạt được vì thiếu Airflow/Spark. |

**Tổng kết:** 6/10 IP đạt đầy đủ evidence sống (IP01, IP05, IP06, IP08, IP09, và
IP07 dưới dạng negative-evidence đúng quy tắc); IP10 đạt một phần (8/11 span,
không nằm trong 1 trace); IP02, IP03, IP04 chưa có evidence do thiếu
Airflow/Spark trong lần chạy này.

## 3. Trade-off và lý do kỹ thuật

- **Không giả lập vLLM/Airflow để "xanh hoá" evidence.** Bài quy định rõ làm
  giả vLLM/trace/evidence bị 0 điểm phần tương ứng — báo `UNVERIFIED`/
  `not_ready` trung thực tốt hơn một demo giả.
- **`readiness_status` coi vLLM là mandatory** (`settings.vllm.require_real`)
  nên `/ready` trả `not_ready` chứ không phải `degraded` khi thiếu vLLM thật.
  Đây là lựa chọn đúng về mặt sản phẩm: không có LLM thì không thể trả lời
  câu hỏi, khác với Feast (không mandatory) — thiếu Feast chỉ làm câu trả lời
  kém đi, không làm mất khả năng phục vụ.
- **Editable install và đường dẫn Unicode.** Thư mục dự án có ký tự có dấu
  ("Máy tính") khiến `uv run` với editable-install mặc định lỗi
  `ModuleNotFoundError`. Khắc phục bằng `uv sync --no-editable` +
  `uv run --no-sync`. Đây là rủi ro portability thật (không phải lỗi code lab)
  cần lưu ý khi làm trên Windows/OneDrive có đường dẫn tiếng Việt.
- **MLflow crash do console encoding (cp1252) khi in emoji.** Khắc phục bằng
  `PYTHONIOENCODING=utf-8`. Cũng là vấn đề môi trường Windows, không phải lỗi
  logic của `lab28-platform`.

## 4. Production gaps (những gì chưa đủ để lên production)

1. **Không có bằng chứng end-to-end thật cho IP02–IP04**: pipeline Kafka →
   Airflow → Delta → Feast chưa từng chạy trong môi trường này; rủi ro lỗi
   tiềm ẩn ở phần dedupe/MERGE thực tế (dù unit test `dedupe_latest` đã pass)
   chưa được xác nhận trên dữ liệu Kafka replay thật.
2. **Không có SLO alert nào được kiểm chứng kích hoạt thật** (chỉ xác nhận
   Prometheus scrape "up", chưa trigger một alert rule thật và quan sát nó
   bắn).
3. **Không có load test / P50-P95-P99** vì dừng ở hệ thống cơ bản, thời gian
   không đủ tin cậy do vLLM/Airflow chưa có để đo latency toàn luồng RAG.
4. **Không có rollback MLflow thật** (mới `register` + `promote`, chưa demo
   rollback alias `champion` về version cũ).
5. **K8s/GitOps chỉ được validate tĩnh** (`scripts/validate_manifests.py`
   pass) — chưa `kubectl apply`/Argo CD sync thật, chưa chứng minh
   drift/self-heal/rollback trên cluster thật.
6. **Mạng trong môi trường build không ổn định** — với ~450 MB/gói lớn, một
   pipeline CI/CD thật cần build cache bền hơn (registry cache, mirror nội bộ)
   để không phụ thuộc một lần tải liên tục thành công.

## 5. Đóng góp thành viên

Làm cá nhân — Trần Anh Tú (2A202601674) thực hiện toàn bộ: 4 hàm bắt buộc,
vận hành Docker, thu evidence, viết tài liệu này.
