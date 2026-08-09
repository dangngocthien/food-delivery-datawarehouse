# /ingestion — Thành viên A: Ingestion & Streaming

## Mục tiêu
Đưa dữ liệu vào hệ thống theo 2 dạng: **batch** (dataset Kaggle) và **streaming** (Kafka).

## Việc cần làm

1. **Tải dataset Kaggle** (Food Delivery Time — 10 cột thật), đổi tên cột về snake_case theo đúng bảng ở Mục 2.2 file phân công:
   - `ID` → `order_id`, `Delivery_person_ID` → `driver_id`, `Delivery_person_Age` → `driver_age`, `Delivery_person_Ratings` → `driver_rating`, `Restaurant_latitude/longitude` → `restaurant_lat/lon`, `Delivery_location_latitude/longitude` → `delivery_lat/lon`, `Type_of_order` → `order_type`, `Type_of_vehicle` → `vehicle_type`.

2. **Viết script mô phỏng thêm cột** không có trong Kaggle (theo đúng luật sinh ở Mục 2.2):
   - `restaurant_id` — sinh từ cặp toạ độ nhà hàng duy nhất.
   - `city` — random trong `["Ho Chi Minh", "Ha Noi", "Da Nang"]`.
   - `order_date` — random trong 30 ngày gần đây (batch) / ngày hiện tại (streaming).
   - `time_ordered` — random giờ trong ngày (batch) / thời điểm thật lúc producer chạy (streaming).
   - `time_picked` = `time_ordered` + random 10–30 phút.
   - `time_delivered` = `time_picked` + thời gian giao giả lập (dựa trên khoảng cách).
   - `order_amount` — random 30.000–300.000 VNĐ.
   - Ghi kết quả vào MinIO `raw/`.

3. **Viết `producer.py`**: sinh đơn hàng giả lập, gửi 4 sự kiện trạng thái/đơn (`placed`, `cooking`, `picked`, `delivered` — đúng thứ tự) vào Kafka topic **`order-events`**, đúng format JSON:
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

4. **Viết consumer test** đơn giản để tự kiểm tra message chảy đúng.

5. **Tạo khối lượng đủ lớn cho streaming**: cho `producer.py` chạy lặp/loop sinh liên tục nhiều đơn hàng giả (không chỉ demo vài chục dòng), vì dataset Kaggle gốc chỉ ~45k đơn (nhỏ).

## Cài đặt cần thiết
```
pip install kafka-python boto3 minio
```
+ Kafka, Zookeeper, Kafka UI chạy qua Docker (xem `docker-compose.yml` ở gốc repo).

## File cần có trong thư mục này
- `download_kaggle_data.py` hoặc ghi chú cách tải dataset
- `simulate_and_load_raw.py` — script sinh cột mô phỏng, ghi vào MinIO `raw/`
- `producer.py`
- `consumer_test.py`
- `sample_data.csv` — 20–30 dòng dữ liệu giả đúng schema Mục 2.2 (để B/C/D code song song, không cần chờ dữ liệu thật)
- `requirements.txt`

## Nộp
- [ ] `producer.py` chạy được
- [ ] Dữ liệu trong MinIO `raw/` đúng schema (đủ cột thật lẫn cột mô phỏng)
- [ ] Ảnh chụp Kafka UI

## Lưu ý
Không tự đổi tên cột/schema. Nếu thấy cần đổi, báo cả nhóm trong group chat trước.
