# Tóm tắt tiến độ — Phần A: Ingestion & Streaming

**Thành viên phụ trách:** A
**Trạng thái:** Hoàn thành phần code chính, đã kiểm thử thành công với dữ liệu Kaggle thật. Đã chuyển sang chạy trên hạ tầng Docker chung của cả nhóm.

---

## 1. Đã hoàn thành

- **Hạ tầng:** Ban đầu A dùng hạ tầng Docker riêng (`docker-compose.a-local-test.yml`) để tự test lúc code. File này **đã bị xoá** — toàn bộ pipeline hiện chạy trên **hạ tầng Docker chung** ở `docker-compose.yml` gốc repo (Kafka, Zookeeper, MinIO, Kafka UI, Spark, Postgres, Airflow, Metabase).
- **Batch ingestion (`simulate_batch.py`):** đã chạy với **dataset Kaggle thật** (Food Delivery Time), đổi tên cột về chuẩn snake_case theo hợp đồng dữ liệu, sinh thêm các cột mô phỏng (`restaurant_id`, `city`, `order_date`, `time_ordered`, `time_picked`, `time_delivered`, `order_amount`), ghi kết quả dạng Parquet vào MinIO `raw/`. Đã xác nhận file `.parquet` xuất hiện đúng trong bucket.
- **Streaming ingestion (`producer.py`):** đã chạy thành công, sinh đơn hàng và bắn 4 sự kiện trạng thái/đơn (`placed → cooking → picked → delivered`) vào Kafka topic `order-events`, đúng format JSON 10 field theo hợp đồng dữ liệu.
- **Kiểm thử (`consumer_test.py`):** đã chạy, dùng để tự xác nhận message chảy đúng thứ tự 4 trạng thái.

## 2. Sự cố đã gặp & cách xử lý

| Sự cố | Nguyên nhân | Cách xử lý |
|---|---|---|
| Không vào được `localhost:8080` (Kafka UI) trên hạ tầng riêng cũ | Cổng 8080 trên máy Windows bị chương trình khác chiếm | Đổi port mapping trong `docker-compose.a-local-test.yml` từ `8080:8080` sang `8081:8080` |
| Container `kafka-ui` kẹt ở trạng thái "Created" | Chưa từng khởi động được / xung đột cổng | `docker rm ingestion-kafka-ui-1` rồi `docker compose up -d` lại |
| Hạ tầng riêng của A xung đột cổng với hạ tầng chung | Kafka UI riêng của A dùng `8081`, trùng port Airflow webserver (`8081`) trong `docker-compose.yml` chung | D phát hiện khi so sánh 2 file compose. Đã xoá hẳn hạ tầng riêng của A, chuyển toàn bộ sang chạy trên hạ tầng chung (Kafka UI hạ tầng chung dùng port `8090`, không trùng ai) |
| `bitnami/spark:3.5` không pull được (`not found`) khi bật hạ tầng chung | Từ 9/2025, Bitnami gỡ toàn bộ tag phiên bản miễn phí khỏi Docker Hub | Đổi `image: bitnami/spark:3.5` → `image: bitnamilegacy/spark:3.5` trong `docker-compose.yml` gốc (D đã sửa và push) |

## 3. Việc còn lại (trước khi coi Phần A hoàn tất 100%)

- [ ] Chạy lại `simulate_batch.py` và `producer.py` trên **hạ tầng chung** (bucket `food-delivery-lake` cần tạo lại vì là container/volume mới), xác nhận dữ liệu xuất hiện đúng trong MinIO `raw/` và Kafka topic `order-events`
- [ ] Xác nhận lại đủ 10 field JSON trong tab Messages của Kafka UI (`localhost:8090`) khớp hợp đồng dữ liệu
- [ ] Cho producer chạy đủ lâu để có khối lượng dữ liệu lớn hơn (không chỉ vài chục đơn demo)
- [ ] Chụp ảnh Kafka UI + MinIO console (trên hạ tầng chung) làm bằng chứng nộp bài
- [ ] Push file `.env.example` lên Git (hiện chưa có trên repo, chỉ có trên máy A) để B/C/D biết chính xác tên biến môi trường cần khai báo

## 4. Bàn giao cho thành viên B

Xem chi tiết trong `README.md` cùng thư mục — có đầy đủ endpoint (hạ tầng chung), schema, và cách chạy lại.