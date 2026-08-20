# Food Delivery Data Warehouse — Đồ án 18

Hệ thống data warehouse end-to-end cho nền tảng **giao đồ ăn**, mô phỏng luồng: thu thập dữ liệu (batch + streaming) → xử lý Spark → data lake (Parquet) → data warehouse (star schema) → điều phối Airflow → biến đổi dbt → dashboard BI + mô hình AI/ML dự đoán thời gian giao hàng (ETA).

Đồ án được ấn định chạy trên **GCP**, nhưng toàn bộ hệ thống dưới đây chạy **100% local bằng Docker Compose** (ánh xạ dịch vụ GCP → open-source), kiến trúc và code giữ nguyên logic, chỉ đổi endpoint kết nối.

| Khái niệm | Dịch vụ GCP tương ứng | Công cụ dùng thật (local) |
|---|---|---|
| Data lake | Cloud Storage | **MinIO** |
| Streaming | Managed Kafka / Pub/Sub | **Apache Kafka** |
| Xử lý dữ liệu | Dataproc | **Apache Spark** |
| Data Warehouse | BigQuery | **PostgreSQL** |
| Orchestration | Cloud Composer | **Apache Airflow** |
| Transformation | Dataform | **dbt** |
| BI Dashboard | Looker Studio | **Metabase** |
| ML | BigQuery ML / Vertex AI | **scikit-learn** |

---

## 1. Cấu trúc thư mục

```
food-delivery-datawarehouse/
├── ingestion_A/                # Thu thập dữ liệu
│   ├── data/food_delivery_raw.csv
│   ├── common.py                # hàm dùng chung: haversine, sinh dữ liệu mô phỏng
│   ├── simulate_batch.py        # batch: CSV Kaggle -> MinIO raw/
│   ├── producer.py               # streaming: bơm sự kiện đơn hàng vào Kafka
│   └── consumer_test.py
├── spark_B/                    # Xử lý PySpark
│   ├── batch_processing.py      # raw/ -> cleansed/ -> curated/ (MinIO)
│   └── streaming_processing.py  # đọc Kafka, tính toán realtime
├── warehouse_C/                # Data Warehouse
│   ├── create_tables.sql        # DDL star schema (Postgres)
│   ├── load_to_postgres.py      # curated/ (MinIO) -> Postgres
│   └── dbt_project/
│       ├── models/staging/stg_orders.sql
│       └── models/marts/mart_delivery_summary.sql, mart_distance_vs_time.sql
├── orchestration_D/            # Điều phối & Analytics
│   ├── dags/food_delivery_pipeline_dag.py
│   ├── metabase/dashboard_queries.sql
│   └── ml/train_eta_model.py, model_evaluation.md
├── docker-compose.yml
├── .env
└── spark-defaults.conf
```

---

## 2. Nguồn dữ liệu

Dataset gốc: **Food Delivery Time** (Kaggle), 10 cột. `simulate_batch.py` đổi tên cột về snake_case (`order_id`, `driver_id`, `driver_age`, `driver_rating`, `restaurant_lat/lon`, `delivery_lat/lon`, `order_type`, `vehicle_type`), sau đó **sinh thêm** các cột không có sẵn trong Kaggle để phục vụ mô phỏng nghiệp vụ:

- `restaurant_id`, `city` (được gán mô phỏng từ toạ độ)
- `order_date`, `time_ordered`, `time_picked`, `time_delivered`
- `order_amount`

## 3. Luồng xử lý end-to-end (DAG chính)

DAG `food_delivery_pipeline_dag` (Airflow, lịch chạy `@daily`) thực hiện tuần tự:

```
task_ingest → task_spark_batch → task_create_tables → task_load_warehouse → task_dbt_run → task_refresh_dashboard
```

1. **`task_ingest`** — chạy `simulate_batch.py`: đọc `food_delivery_raw.csv`, đổi tên cột, sinh cột mô phỏng, ghi Parquet vào MinIO `raw/`.
2. **`task_spark_batch`** — `spark-submit batch_processing.py`: đọc `raw/`, loại dòng thiếu cột bắt buộc hoặc toạ độ vô lý → ghi `cleansed/`; sau đó tính `distance_km` (Haversine), `prep_time_min`, `delivery_time_min`, `is_late` (SLA 45 phút) → ghi `curated/`, partition theo `order_date` và `city`.
3. **`task_create_tables`** — chạy `create_tables.sql`: **DROP và tạo lại** toàn bộ star schema trong Postgres (`food_delivery_dw`). ⚠️ Lưu ý: mỗi lần DAG chạy, dữ liệu warehouse bị xoá sạch và nạp lại từ đầu — không phải incremental load.
4. **`task_load_warehouse`** — `load_to_postgres.py`: đọc `curated/` từ MinIO, upsert 4 bảng dimension (`Dim_Restaurant`, `Dim_Driver`, `Dim_Zone`, `Dim_Date`), sau đó upsert `Fact_Order` (dùng `ON CONFLICT` nên chạy lại nhiều lần vẫn an toàn).
5. **`task_dbt_run`** — `dbt run && dbt test` trong `warehouse_C/dbt_project`: build staging model và 2 mart, chạy test dữ liệu.
6. **`task_refresh_dashboard`** — nhắc mở Metabase để xem dashboard (không tự động refresh qua API).

### Luồng streaming (Kafka) — chạy tách biệt, không nằm trong DAG

`producer.py` (ingestion) bơm sự kiện vòng đời đơn hàng (`placed → cooking → picked → delivered`) vào topic Kafka `order-events`. `streaming_processing.py` (Spark Structured Streaming, stateful) ghép 4 sự kiện của cùng một đơn, khi đủ `delivered` thì tính `distance_km`, `prep_time_min`, `delivery_time_min`, `is_late` và in ra console (tuỳ chọn ghi thêm vào MinIO `curated/streaming/`). Đây là thành phần **minh hoạ khả năng xử lý streaming**, chạy thủ công (`spark-submit`) song song, chưa được đưa vào Airflow DAG.

## 4. Data Warehouse — Star Schema

**Fact_Order**: `order_id`, `restaurant_key`, `driver_key`, `zone_key`, `date_key`, `distance_km`, `prep_time_min`, `delivery_time_min`, `order_amount`, `is_late`

**Dimensions**:
| Bảng | Cột |
|---|---|
| `Dim_Restaurant` | `restaurant_key`, `restaurant_id`, `restaurant_lat`, `restaurant_lon`, `city` |
| `Dim_Driver` | `driver_key`, `driver_id`, `driver_age`, `driver_rating`, `vehicle_type` |
| `Dim_Zone` | `zone_key`, `city` |
| `Dim_Date` | `date_key`, `order_date`, `hour`, `day_of_week` |

**dbt** (`warehouse_C/dbt_project`):
- `stg_orders` (staging): select trực tiếp từ `source('raw', 'fact_order')`, có test `unique`/`not_null` trên `order_id` và `relationships` cho từng khoá ngoại tới 4 bảng dimension.
- `mart_delivery_summary` (mart): tổng hợp số đơn, thời gian giao trung bình, khoảng cách trung bình, số đơn trễ — group theo `city` + `hour`.
- `mart_distance_vs_time` (mart): bảng chi tiết `distance_km`, `delivery_time_min`, `is_late` theo từng đơn — phục vụ biểu đồ tương quan khoảng cách/thời gian.

## 5. Dashboard BI (Metabase)

3 câu query dựng sẵn trong `orchestration_D/metabase/dashboard_queries.sql`:

1. Số đơn theo giờ / khu vực (bar chart)
2. Thời gian giao trung bình theo ngày / khu vực (line chart)
3. Top 10 nhà hàng có tỷ lệ giao trễ cao nhất (bar chart ngang)

Cách dùng: Metabase → **New Question → Native query (SQL editor)** → chọn database `postgres-warehouse` → dán 1 trong 3 câu → chọn loại biểu đồ → **Save** → pin vào Dashboard.

## 6. AI/ML — Dự đoán thời gian giao hàng (ETA)

Script: `orchestration_D/ml/train_eta_model.py`. Mô hình RandomForest dự đoán `delivery_time_min` dùng 4 feature: `distance_km`, `driver_age`, `driver_rating`, `vehicle_type` (one-hot). Dữ liệu lấy trực tiếp từ JOIN `Fact_Order` + `Dim_Driver` trong Postgres.

**Xử lý outlier:** phát hiện 404/45.403 dòng (~0,9%) có `distance_km` bất thường (có dòng lên tới 19.688km) do một số toạ độ nhà hàng trong dataset Kaggle gốc bị lật dấu (âm thay vì dương). Đã lọc `distance_km <= 20km` trước khi train (loại 2.512/45.403 dòng, ~5,5%). **Lưu ý:** việc lọc này mới chỉ áp dụng ở tầng ML (`train_eta_model.py`), **chưa** sửa tại nguồn (`batch_processing.py`) — nên `curated/`, warehouse, dashboard Metabase và mart `mart_distance_vs_time` hiện vẫn còn các dòng outlier này.

**Kết quả (sau khi lọc, 42.891 dòng):**

| Metric | Model | Baseline (mean) |
|---|---|---|
| MAE | 4.66 phút | 14.83 phút |
| RMSE | 6.10 phút | 17.95 phút |
| R² | 0.8844 | — |

Tốt hơn baseline 68.6%. `distance_km` chiếm ~98.7% mức độ quan trọng của mô hình; `driver_age`, `driver_rating`, `vehicle_type` gần như không đóng góp. Chi tiết đầy đủ xem `orchestration_D/ml/model_evaluation.md`.

---

## 7. Hướng dẫn chạy hệ thống

### Yêu cầu
- Docker + Docker Compose
- ~16GB RAM khuyến nghị nếu chạy đầy đủ (`full` profile)

### Bước 1 — Khởi động toàn bộ hệ thống

File `.env` đã có sẵn `COMPOSE_PROFILES=full` nên chỉ cần:

```bash
docker compose up -d
```

Lệnh trên dựng đủ 11 container: `zookeeper`, `kafka`, `kafka-ui`, `minio`, `spark-master`, `spark-worker`, `postgres-warehouse`, `postgres-airflow`, `airflow-webserver`, `airflow-scheduler`, `metabase`.

Nếu máy yếu RAM, có thể chạy chọn lọc từng nhóm dịch vụ, ví dụ chỉ test phần Spark:
```bash
COMPOSE_PROFILES=batch docker compose up -d
```
(các profile khác: `streaming`, `warehouse`, `orchestration`, `bi` — có thể kết hợp nhiều profile cách nhau bằng dấu phẩy)

### Bước 2 — Truy cập các dịch vụ

| Dịch vụ | URL | Tài khoản |
|---|---|---|
| Airflow | http://localhost:8081 | `admin` / `admin` |
| Spark Master UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| Kafka UI | http://localhost:8090 | — |
| Metabase | http://localhost:3000 | tự tạo tài khoản lần đầu |
| Postgres warehouse | `localhost:5432` | db `food_delivery_dw`, user `warehouse` / `warehouse123` |

### Bước 3 — Chạy pipeline chính

1. Mở Airflow (http://localhost:8081), tìm DAG `food_delivery_pipeline_dag`.
2. Bật (unpause) DAG rồi trigger chạy thủ công (nút ▶), hoặc chờ lịch `@daily`.
3. Theo dõi 6 task chạy tuần tự: `task_ingest → task_spark_batch → task_create_tables → task_load_warehouse → task_dbt_run → task_refresh_dashboard`.
4. Khi tất cả task xanh (success), dữ liệu đã có đầy đủ trong Postgres warehouse.

### Bước 4 — Dựng dashboard Metabase

1. Mở Metabase (http://localhost:3000), làm theo wizard cài đặt lần đầu.
2. Kết nối database: **PostgreSQL**, host `postgres-warehouse`, port `5432`, database `food_delivery_dw`, user `warehouse`, password `warehouse123`.
3. Tạo 3 câu hỏi (Question) bằng SQL trong `orchestration_D/metabase/dashboard_queries.sql`, ghim vào 1 Dashboard.

### Bước 5 (tuỳ chọn) — Chạy mô hình dự đoán ETA

```bash
docker exec -it airflow-scheduler bash
python -m pip install -r /opt/airflow/project/orchestration_D/ml/requirements.txt --break-system-packages
python /opt/airflow/project/orchestration_D/ml/train_eta_model.py
```

### Bước 6 (tuỳ chọn) — Chạy thử luồng streaming Kafka

Cần đủ profile `streaming` + `batch`. Ở 2 terminal khác nhau:

```bash
# Terminal 1 — bơm sự kiện đơn hàng vào Kafka
python producer.py

# Terminal 2 — Spark đọc Kafka realtime
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-project/spark_B/streaming_processing.py
```

Kết quả các đơn hàng đã hoàn tất (đủ 4 trạng thái) sẽ in ra console theo từng micro-batch.

### Dừng hệ thống

```bash
docker compose down        # dừng, giữ lại dữ liệu (volume)
docker compose down -v     # dừng và xoá luôn toàn bộ dữ liệu
```

---

## 8. Giới hạn đã biết

- `task_create_tables` xoá và tạo lại schema mỗi lần DAG chạy → warehouse **không giữ lịch sử**, mỗi lần chạy là nạp lại toàn bộ từ `curated/`.
- Outlier toạ độ (dòng có `distance_km` cực lớn do lỗi dấu âm trong Kaggle gốc) mới được lọc ở tầng ML, **chưa** được xử lý tại `batch_processing.py` — ảnh hưởng đến số liệu trên dashboard Metabase và mart `mart_distance_vs_time`.
- Feature `order_type` (loại đơn: Meal/Snack/Drinks/Buffet) có trong Kaggle gốc nhưng không được đưa vào star schema, nên mô hình ETA hiện chỉ dùng được 4/5 feature dự kiến.
- Luồng streaming Kafka là thành phần minh hoạ độc lập, chưa tích hợp vào Airflow DAG chính.
