# Tóm tắt tiến độ — Phần A: Ingestion & Streaming

**Thành viên phụ trách:** A
**Trạng thái:** Hoàn thành phần code chính, đã kiểm thử thành công với dữ liệu Kaggle thật

---

## 1. Đã hoàn thành

- **Hạ tầng:** Docker Compose bật thành công 4 service — Kafka, Zookeeper, MinIO, Kafka UI.
- **Batch ingestion (`simulate_batch.py`):** đã chạy với **dataset Kaggle thật** (Food Delivery Time), đổi tên cột về chuẩn snake_case theo hợp đồng dữ liệu, sinh thêm các cột mô phỏng (`restaurant_id`, `city`, `order_date`, `time_ordered`, `time_picked`, `time_delivered`, `order_amount`), ghi kết quả dạng Parquet vào MinIO `raw/`. Đã xác nhận file `.parquet` xuất hiện đúng trong bucket.
- **Streaming ingestion (`producer.py`):** đã chạy thành công, sinh đơn hàng và bắn 4 sự kiện trạng thái/đơn (`placed → cooking → picked → delivered`) vào Kafka topic `order-events`, đúng format JSON 10 field theo hợp đồng dữ liệu.
- **Kiểm thử (`consumer_test.py`):** đã chạy, dùng để tự xác nhận message chảy đúng thứ tự 4 trạng thái.

## 2. Sự cố đã gặp & cách xử lý

| Sự cố | Nguyên nhân | Cách xử lý |
|---|---|---|
| Không vào được `localhost:8080` (Kafka UI) | Cổng 8080 trên máy Windows bị chương trình khác chiếm | Đổi port mapping trong `docker-compose.a-local-test.yml` từ `8080:8080` sang `8081:8080` |
| Container `kafka-ui` kẹt ở trạng thái "Created" | Chưa từng khởi động được / xung đột cổng | `docker rm ingestion-kafka-ui-1` rồi `docker compose up -d` lại |

## 3. Việc còn lại (trước khi coi Phần A hoàn tất 100%)

- [ ] Xác nhận lại đủ 10 field JSON trong tab Messages của Kafka UI khớp hợp đồng dữ liệu
- [ ] Cho producer chạy đủ lâu để có khối lượng dữ liệu lớn hơn (không chỉ vài chục đơn demo)
- [ ] Chụp ảnh Kafka UI + MinIO console làm bằng chứng nộp bài

## 4. Bàn giao cho thành viên B

Xem chi tiết trong `README.md` cùng thư mục — có đầy đủ endpoint, schema, và cách chạy lại.
