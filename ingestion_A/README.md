# Ingestion & Streaming — Phần A

Phần này chịu trách nhiệm đưa dữ liệu vào hệ thống bằng 2 luồng: **batch** (đọc CSV Kaggle → ghi Parquet vào MinIO) và **streaming** (giả lập sự kiện đơn hàng realtime → gửi vào Kafka). Thư mục này là input đầu vào cho **Thành viên B (Spark & Data Lake)**.

> Toàn bộ script trong thư mục này chạy trên **hạ tầng Docker chung của cả nhóm** (`docker-compose.yml` ở thư mục gốc repo). Không có hạ tầng Docker riêng.

---

## 1. Cấu trúc thư mục

```
ingestion_A/
├── common.py             # Hàm & hằng số dùng chung (haversine, sinh restaurant_id, luật random...)
├── simulate_batch.py     # Đọc CSV Kaggle, sinh dữ liệu batch, ghi vào MinIO raw/
├── producer.py           # Sinh đơn hàng liên tục, bắn sự kiện vào Kafka topic order-events
├── consumer_test.py      # Script tự kiểm tra producer hoạt động đúng
├── requirements.txt      # Python packages cần cài
├── .env.example          # Mẫu file cấu hình (copy thành .env, không push .env thật)
├── TOM_TAT_TIEN_DO.md    # Tóm tắt tiến độ chi tiết
└── README.md             # File này
```

## 2. Cách chạy lại từ đầu

```bash
# 1. Cài package
pip install -r requirements.txt

# 2. Tạo file .env từ mẫu (điền đúng giá trị hạ tầng chung — xem bảng Mục 3)
cp .env.example .env

# 3. Bật hạ tầng chung (chạy ở thư mục gốc repo, không phải trong ingestion_A/)
cd ..
docker compose up -d

# 4. Chạy batch ingestion (cần có file CSV Kaggle "Food Delivery Time" trước)
cd ingestion_A
python simulate_batch.py --input data/food_delivery.csv

# 5. Chạy streaming ingestion
python producer.py

# 6. (Tuỳ chọn) Kiểm tra producer hoạt động đúng
python consumer_test.py
```

## 3. Endpoint để B kết nối (hạ tầng chung — xem README gốc repo)

| Dịch vụ | Endpoint | Ghi chú |
|---|---|---|
| MinIO API | `http://localhost:9000` | user/pass: `minioadmin` / `minioadmin123` |
| MinIO console | `http://localhost:9001` | để xem trực quan bucket/file |
| Kafka bootstrap server | `localhost:9092` | dùng để Spark Structured Streaming đọc trực tiếp |
| Kafka UI | `http://localhost:8090` | để xem message trực quan |
| Bucket MinIO | `food-delivery-lake` | cần tạo tay lần đầu qua console |
| Kafka topic | `order-events` | |

## 4. Dữ liệu batch — nơi B đọc vào

**Vị trí:** `s3a://food-delivery-lake/raw/` (file Parquet)

**Schema (đúng hợp đồng dữ liệu đã thống nhất cả nhóm):**

Cột lấy thật từ Kaggle (đã đổi tên snake_case):
| Cột | Kiểu |
|---|---|
| `order_id` | string |
| `driver_id` | string |
| `driver_age` | int |
| `driver_rating` | float |
| `restaurant_lat` | float |
| `restaurant_lon` | float |
| `delivery_lat` | float |
| `delivery_lon` | float |
| `order_type` | string |
| `vehicle_type` | string |

Cột A tự mô phỏng thêm:
| Cột | Kiểu | Ghi chú |
|---|---|---|
| `restaurant_id` | string | sinh từ cặp toạ độ nhà hàng duy nhất |
| `city` | string | random trong danh sách cố định |
| `order_date` | date | random trong 30 ngày gần đây |
| `time_ordered` | timestamp | random giờ trong ngày |
| `time_picked` | timestamp | `time_ordered` + thời gian chuẩn bị giả lập |
| `time_delivered` | timestamp | `time_picked` + thời gian giao giả lập (tính theo khoảng cách) |
| `order_amount` | float | random 30.000–300.000 VNĐ |

> Lưu ý: `time_*` và `order_amount` là dữ liệu **mô phỏng**, không phải số liệu thật từ Kaggle — cần ghi rõ trong báo cáo cuối kỳ ở mục "Giới hạn dữ liệu".

## 5. Dữ liệu streaming — nơi B đọc vào

**Kafka topic:** `order-events`

**Format message (JSON):**
```json
{
  "order_id": "ORD00123",
  "status": "placed",
  "timestamp": "2026-08-08T10:15:00",
  "restaurant_id": "REST_10p77_106p70",
  "driver_id": "BANGRES18DEL02",
  "city": "Ho Chi Minh",
  "restaurant_lat": 10.77,
  "restaurant_lon": 106.70,
  "delivery_lat": 10.80,
  "delivery_lon": 106.65
}
```

`status` chỉ nhận 1 trong 4 giá trị theo đúng thứ tự: `placed` → `cooking` → `picked` → `delivered`.

## 6. Việc B cần làm tiếp (theo phân công)

1. Đọc `raw/` từ MinIO, làm sạch dữ liệu (loại null/lỗi), ghi ra `cleansed/`.
2. Tính `distance_km` (Haversine), `prep_time_min`, `delivery_time_min`, `is_late`; ghi Parquet ra `curated/`, partition theo `order_date` và `city`.
3. Viết Spark Structured Streaming đọc trực tiếp từ Kafka topic `order-events` để xử lý theo thời gian thực.

## 7. Trạng thái hiện tại của Phần A

Xem chi tiết trong [`TOM_TAT_TIEN_DO.md`](./TOM_TAT_TIEN_DO.md) — bao gồm việc đã làm, sự cố đã xử lý, và các việc còn lại.