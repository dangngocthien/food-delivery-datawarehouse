# HƯỚNG DẪN CHẠY DEMO — Đồ án 18: Giao đồ ăn (Data Warehouse local, Docker Compose)

> Đồ án chạy 100% local bằng Docker Compose, thay thế các dịch vụ GCP gốc (xem bảng ánh xạ đầu file `docker-compose.yml`).
> Máy giảng viên cần: Docker Desktop, ~8GB RAM trống, port 3000/5432/7077/8080/8081/8090/9000/9001/9092 chưa bị chiếm.

---

## 0. Chuẩn bị (1 lần)

```powershell
git clone <repo-url>
cd food-delivery-datawarehouse
```

Kiểm tra có file `.env` ở thư mục gốc và `ingestion_A/.env` (đã có sẵn, không cần sửa gì để demo đầy đủ).

---

## 1. Khởi động toàn bộ hệ thống

```powershell
docker compose up -d --build
```

- `--build` cần cho lần đầu (build image Spark từ `spark_B/Dockerfile.spark`); lần sau chỉ cần `docker compose up -d`.
- Lệnh này bật đủ 10 container: `zookeeper, kafka, kafka-ui, minio, spark-master, spark-worker, postgres-warehouse, postgres-airflow, airflow-init/webserver/scheduler, metabase`.
- Đợi 1–2 phút để tất cả container healthy (Postgres cho Airflow cần `healthcheck` pass trước khi Airflow init chạy).

Kiểm tra trạng thái:
```powershell
docker compose ps
```
Tất cả nên ở trạng thái `Up` (riêng `airflow-init` sẽ `Exited (0)` — bình thường, vì nó chỉ chạy 1 lần để tạo user admin rồi thoát).

---

## 2. Các giao diện web để giảng viên xem trực tiếp

| Dịch vụ | URL | Đăng nhập |
|---|---|---|
| **Airflow** (D — Orchestration) | http://localhost:8081 | `admin` / `admin` |
| **Spark Master UI** (B) | http://localhost:8080 | không cần |
| **Kafka UI** (A) | http://localhost:8090 | không cần |
| **MinIO Console** (data lake) | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| **Metabase** (D — Dashboard) | http://localhost:3000 | tự tạo tài khoản lần đầu (setup wizard) |
| **Postgres warehouse** (C) | `localhost:5432`, db `food_delivery_dw` | `warehouse` / `warehouse123` |

---

## 3. Chạy pipeline end-to-end theo đúng thứ tự A → B → C → D

### Bước 1 — Ingestion batch + tạo bucket MinIO (A)

Chạy trong container `spark-master` (dùng `spark-submit`, giống cách chạy streaming của B):
```powershell
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/spark-project/ingestion_A/simulate_batch.py
```
Xác nhận thành công: vào MinIO Console (bước 2) → bucket `food-delivery-lake/raw/` có file.

### Bước 2 — Spark batch processing (B) — đã test thật, lệnh chính xác
```powershell
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/spark-project/spark_B/batch_processing.py
```
Xác nhận thành công: MinIO Console → `food-delivery-lake/curated/` có Parquet, partition theo `order_date=.../city=...`.

### Bước 3 — Streaming: producer (A) + Spark Structured Streaming (B)

Terminal 1 — producer chạy **trực tiếp trên máy host** (không phải trong container), giả lập việc khách đặt đơn:
```powershell
cd ingestion_A
python -m venv venv        # nếu chưa có venv riêng cho A
venv\Scripts\activate
pip install -r requirements.txt
python producer.py
```
Producer đọc `ingestion_A/.env` (dùng `localhost:9092` cho Kafka, `localhost:9000` cho MinIO — đúng vì chạy từ host, khác với `.env` gốc dùng tên service nội bộ Docker).

Terminal 2 — chạy Spark Structured Streaming (lệnh đã test thật, chạy ổn định):
```powershell
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/spark-project/spark_B/streaming_processing.py
```
Xác nhận thành công: log hiện các batch kết quả liên tục (đơn hàng đã ghép đủ `placed→picked→delivered` với `distance_km`, `prep_time_min`, `delivery_time_min`, `is_late`). Có thể xem Kafka UI (`localhost:8090`) để thấy message chảy qua topic `order-events`.

### Bước 4 — Nạp warehouse + dbt (C)

Nạp Postgres — đã test thật, chạy ổn định:
```powershell
docker exec -it spark-master python /opt/spark-project/warehouse_C/load_to_postgres.py
```
Xác nhận thành công: kết nối Postgres (DBeaver/psql tới `localhost:5432`, db `food_delivery_dw`) → thấy `Fact_Order`, `Dim_Restaurant`, `Dim_Driver`, `Dim_Zone`, `Dim_Date` có dữ liệu.

dbt — **⚠️ CẦN C ĐIỀN**: khung project đã có sẵn ở `warehouse_C/dbt_project/` (staging + 2 mart), nhưng chưa xác định chạy `dbt run`/`dbt test` từ đâu (venv trên máy hay trong container nào) và `profiles.yml` trỏ Postgres host nào (`localhost` nếu chạy từ máy, hay `postgres-warehouse` nếu chạy trong container). C tự điền 2 dòng lệnh dưới đây sau khi quyết định:
```powershell
⚠️ C XÁC NHẬN: dbt run --project-dir warehouse_C/dbt_project
⚠️ C XÁC NHẬN: dbt test --project-dir warehouse_C/dbt_project
```
Xác nhận thành công: `dbt test` pass toàn bộ (not_null, unique, relationships trên các khoá ở Mục 2.4).

### Bước 5 — Airflow DAG (D) — chạy toàn bộ pipeline tự động
1. Vào http://localhost:8081, đăng nhập `admin`/`admin`.
2. Bật (unpause) DAG, bấm **Trigger DAG**.
3. Theo dõi Graph View: 5 task nối tiếp `task_ingest → task_spark_batch → task_load_warehouse → task_dbt_run → task_refresh_dashboard` chuyển xanh lần lượt.
4. Nếu 1 task đỏ, bấm vào task → xem Log để debug (đã cấu hình `retries`/`retry_delay` nên Airflow tự thử lại trước khi báo fail hẳn).

### Bước 6 — Dashboard Metabase (D)
1. Vào http://localhost:3000, lần đầu làm theo setup wizard (tạo admin account).
2. Add database: **PostgreSQL**, host `postgres-warehouse` (tên service, không phải `localhost` vì Metabase gọi qua Docker network nội bộ), port `5432`, db `food_delivery_dw`, user `warehouse`, password `warehouse123`.
3. Mở dashboard đã dựng sẵn: số đơn theo giờ/khu vực, thời gian giao trung bình, top nhà hàng có `is_late=true` nhiều nhất.

### Bước 7 — Model dự đoán `delivery_time_min` (D) — ⚠️ CHƯA LÀM

`orchestration_D/` hiện chỉ có `dags/`, `README.md`, `requirements.txt` — **chưa có script/notebook train model**. D cần bổ sung file (VD: `orchestration_D/train_model.py`) trước khi điền lệnh chạy vào đây:
```powershell
⚠️ D XÁC NHẬN: đường dẫn script/notebook train model, ví dụ:
docker exec spark-master python /opt/spark-project/orchestration_D/train_model.py
```
Xác nhận thành công: in ra chỉ số đánh giá (MAE/RMSE) của model dự đoán `delivery_time_min` từ `distance_km, driver_age, driver_rating, order_type, vehicle_type`.

---

## 4. Chạy chọn lọc từng phần (nếu máy yếu, không cần bật cả 10 container)

```powershell
$env:COMPOSE_PROFILES="batch"; docker compose up -d          # chỉ MinIO + Spark (test B)
$env:COMPOSE_PROFILES="streaming"; docker compose up -d      # chỉ Kafka (test A)
$env:COMPOSE_PROFILES="warehouse"; docker compose up -d      # chỉ Postgres warehouse (test C)
$env:COMPOSE_PROFILES="orchestration"; docker compose up -d  # chỉ Airflow (test D)
$env:COMPOSE_PROFILES="batch,warehouse"; docker compose up -d  # kết hợp nhiều nhóm
Remove-Item Env:COMPOSE_PROFILES   # xoá sau khi test xong, tránh ảnh hưởng .env (COMPOSE_PROFILES=full)
```

---

## 5. Dừng hệ thống

```powershell
docker compose down        # dừng, giữ lại dữ liệu (volume)
docker compose down -v     # dừng + xoá sạch dữ liệu (dùng khi muốn demo lại từ đầu)
```

---

## 6. Lỗi thường gặp khi demo (đã gặp thật trong quá trình làm)

| Lỗi | Nguyên nhân | Cách sửa |
|---|---|---|
| `ClassCastException: SerializedLambda ... Function3` khi chạy streaming | Base image Spark tag `:3.5` không ghim version, trong khi JAR Kafka connector bake cứng `3.5.0` → lệch version core vs JAR | Đã sửa: ghim `bitnamilegacy/spark:3.5.6` + đổi 2 JAR Kafka connector (`spark-sql-kafka-0-10`, `spark-token-provider-kafka-0-10`) sang đúng `3.5.6` trong `Dockerfile.spark`, rebuild `--no-cache` |
| `PySparkTypeError: [NOT_INT] Argument durationMs should be an int, got str` | `ORPHAN_TIMEOUT` khai báo dạng string (`"2 hours"`) nhưng `setTimeoutDuration()` yêu cầu `int` (ms) | Đã sửa: `ORPHAN_TIMEOUT = 2*60*60*1000` (int) trong `streaming_processing.py` dòng 53 |
| `ModuleNotFoundError: No module named 'psycopg2'` khi chạy `load_to_postgres.py` trong `spark-master` | Image Spark gốc không có `psycopg2-binary`/`s3fs` cài sẵn | Đã sửa: thêm `RUN pip install psycopg2-binary s3fs` vào `spark_B/Dockerfile.spark`, rebuild lại (`docker compose up -d --build`) |
| Airflow báo lỗi kết nối Postgres lúc mới `up` | `postgres-airflow` chưa healthy khi `airflow-init` chạy | Đợi thêm 30–60s rồi `docker compose restart airflow-init airflow-webserver airflow-scheduler` |
| Metabase không connect được Postgres | Dùng nhầm `localhost` thay vì tên service `postgres-warehouse` | Metabase chạy trong Docker network riêng — luôn dùng tên service, không dùng `localhost` |
| Spark job OOM-kill | RAM container Spark bị giới hạn sát (`spark-master` 1.5g, `spark-worker` 2g) trong khi job xử lý dữ liệu lớn | Tăng `mem_limit` trong `docker-compose.yml` nếu máy giảng viên có nhiều RAM hơn 8GB, hoặc giảm `spark.sql.shuffle.partitions` |
| PowerShell báo `Missing argument in parameter list` khi set `COMPOSE_PROFILES=... docker compose up` | Cú pháp `VAR=value command` là của Bash, PowerShell không hỗ trợ | Dùng `$env:COMPOSE_PROFILES="..."; docker compose up -d` (xem Mục 4) |

---

## 7. Checklist nhanh trước khi demo cho giảng viên

- [ ] `docker compose up -d --build` chạy sạch, `docker compose ps` toàn bộ `Up`
- [ ] Batch: MinIO có đủ `raw/ → cleansed/ → curated/`
- [ ] Streaming: producer chạy liên tục, Spark streaming log ra kết quả liên tục, không lỗi
- [x] Warehouse: Postgres có đủ 1 fact + 4 dimension — **đã test, nạp 45.403 dòng thành công**
- [ ] Warehouse: `dbt test` pass toàn bộ — **C cần hoàn thiện**
- [ ] Airflow: DAG trigger chạy hết 5 task màu xanh
- [ ] Metabase: dashboard hiển thị số liệu thật (không rỗng)
- [ ] Model AI: chạy ra kết quả đánh giá (MAE/RMSE) cụ thể — **D cần viết `train_model.py`**
- [ ] Đã điền đủ các chỗ `⚠️` ở trên bằng lệnh thật của A/C/D trước khi đưa cho giảng viên
