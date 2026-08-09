# /spark — Thành viên B: Spark & Data Lake

## Mục tiêu
Xử lý dữ liệu bằng PySpark: làm sạch, tính toán, ghi Parquet có partition.

## Việc cần làm

1. **Batch processing:**
   - Đọc dữ liệu từ MinIO `raw/` (đúng schema Mục 2.2 — dùng `sample_data.csv` của A nếu chưa có dữ liệu thật).
   - Làm sạch (loại null/lỗi), ghi ra `cleansed/`.

2. **Tính toán các cột mới** (công thức bắt buộc theo Mục 2.2):
   - `distance_km` — Haversine từ `(restaurant_lat, restaurant_lon)` và `(delivery_lat, delivery_lon)`.
   - `prep_time_min` = `time_picked − time_ordered` (phút).
   - `delivery_time_min` = `time_delivered − time_picked` (phút).
   - `is_late` = true nếu `delivery_time_min` > ngưỡng SLA đã thống nhất (VD: 45 phút).
   - Ghi Parquet ra `curated/`, **partition theo `order_date` và `city`**:
     ```
     curated/order_date=YYYY-MM-DD/city=<ten_thanh_pho>/*.parquet
     ```

3. **Streaming processing:**
   - Viết Spark Structured Streaming đọc từ Kafka topic `order-events` (do A tạo).
   - Tính thời gian giao theo thời gian thực.

## Cài đặt cần thiết
```
pip install pyspark pandas pyarrow
```
+ Spark chạy qua Docker, Java JDK 11/17.

## File cần có trong thư mục này
- `batch_processing.py`
- `streaming_processing.py`
- `sample_data.csv` — dữ liệu giả riêng (nếu cần test độc lập trước khi có dữ liệu của A)
- `requirements.txt`

## Nộp
- [ ] `batch_processing.py`, `streaming_processing.py`
- [ ] Parquet partition đúng trong `curated/`
- [ ] Ảnh chụp cấu trúc thư mục MinIO

## Lưu ý
- `delivery_time_min` là dữ liệu **mô phỏng** (Kaggle không có sẵn) — ghi rõ điều này trong báo cáo, ở mục "Giới hạn dữ liệu".
- Không tự đổi tên cột/schema. Nếu thấy cần đổi, báo cả nhóm trong group chat trước.
