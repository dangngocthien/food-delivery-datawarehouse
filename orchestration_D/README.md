# /orchestration — Thành viên D: Orchestration & Analytics/AI

## Mục tiêu
Điều phối toàn bộ pipeline bằng Airflow, dựng dashboard, xây mô hình dự đoán, và phụ trách sơ đồ kiến trúc + ước tính chi phí.

## Việc cần làm

### 1. Airflow DAG (5 bước nối tiếp)
Cài Airflow (Docker Compose chính thức). Viết DAG đủ 5 task:
```
task_ingest (gọi script của A)
  → task_spark_batch (gọi batch_processing.py của B)
  → task_load_warehouse (nạp Parquet vào Postgres của C)
  → task_dbt_run (chạy dbt run + dbt test của C)
  → task_refresh_dashboard (trigger/refresh Metabase, hoặc log thông báo hoàn tất)
```
Đặt lịch chạy (schedule) + cấu hình retry khi lỗi (`retries`, `retry_delay`) cho từng task.

### 2. Dashboard
Cài Metabase (khuyên dùng, dễ hơn Superset), kết nối vào Postgres của C, dựng dashboard:
- Số đơn theo giờ/khu vực
- Thời gian giao trung bình
- Top nhà hàng chậm (cột `is_late`)

### 3. Mô hình dự đoán
Xây mô hình dự đoán `delivery_time_min` bằng scikit-learn/XGBoost, dùng đặc trưng:
`distance_km`, `driver_age`, `driver_rating`, `order_type`, `vehicle_type`.

### 4. Sơ đồ kiến trúc + ước tính chi phí
- Vẽ sơ đồ kiến trúc end-to-end bằng draw.io: nguồn dữ liệu → ingestion (batch/Kafka) → Spark → data lake 3 tầng → warehouse (star schema) → Airflow → dbt → dashboard/AI.
- Thể hiện **2 lớp**: kiến trúc GCP gốc (Dataproc, GCS, BigQuery, Cloud Composer...) và kiến trúc open-source đang chạy thật (Spark, MinIO, Postgres, Airflow local...).
- Ước tính chi phí giả định nếu triển khai thật trên GCP (theo bảng giá công khai Google Cloud), dù nhóm chạy local không tốn phí thật.

## Cài đặt cần thiết
```
pip install scikit-learn xgboost pandas joblib
```
+ Airflow, Metabase/Superset chạy qua Docker; Jupyter Notebook; draw.io (web, không cần cài).

## File cần có trong thư mục này
- `dags/food_delivery_pipeline_dag.py`
- `ml/train_eta_model.py`
- `ml/model_evaluation.md` — kết quả đánh giá độ chính xác
- `architecture_diagram.drawio` (hoặc file xuất PNG/PDF)
- `gcp_cost_estimate.md` hoặc `.xlsx`

## Nộp
- [ ] DAG chạy được + ảnh lịch sử chạy thành công (đủ 5 task, có bước dbt)
- [ ] Dashboard hoàn chỉnh
- [ ] Script AI + kết quả đánh giá độ chính xác
- [ ] Sơ đồ kiến trúc draw.io (2 lớp GCP + open-source)
- [ ] Bảng ước tính chi phí GCP giả định

## Lưu ý
Đây cũng là người khởi tạo khung `docker-compose.yml` dùng chung ở gốc repo — các thành viên khác thêm service riêng vào sau, nhắn nhau trong group chat trước khi sửa để tránh conflict.
