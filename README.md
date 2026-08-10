# [TẠM THỜI] Food Delivery Data Warehouse — Đồ án 18

> ⚠️ **File này là bản làm việc tạm cho nhóm trong lúc code.**
> Khi hoàn thành đồ án, xoá file này và viết `README.md` hoàn chỉnh cho giảng viên
> (kèm sơ đồ kiến trúc, kết quả demo, hướng dẫn chấm điểm...).

---

## 1. Tổng quan đồ án

Xây dựng data warehouse cho nền tảng giao đồ ăn: phân tích đơn hàng theo khu vực/giờ,
thời gian giao, hiệu suất nhà hàng. Cloud gốc: **GCP**, chạy thay thế 100% bằng
**Docker Compose local**.

Luồng end-to-end: `Ingestion (batch + Kafka) → Spark → Data Lake (MinIO) →
Data Warehouse (Postgres, star schema) → Airflow → dbt → Dashboard (Metabase) + AI`

## 2. Phân công & cấu trúc thư mục

| Thư mục | Phụ trách | Nội dung |
|---|---|---|
| `ingestion_A/` | Thành viên A | Tải Kaggle, mô phỏng dữ liệu, `producer.py` Kafka |
| `spark_B/` | Thành viên B | PySpark batch + streaming, tính `distance_km`, `is_late`... |
| `warehouse_C/` | Thành viên C | Postgres star schema, dbt staging/marts/test |
| `orchestration_D/` | Thành viên D | Airflow DAG, Metabase dashboard, model ML dự đoán ETA, sơ đồ kiến trúc |

Chi tiết công việc từng phần: xem `README.md` bên trong mỗi thư mục tương ứng.

**Hợp đồng dữ liệu (tên cột, schema, format Kafka message)** — cố định, không ai
được tự đổi mà không báo nhóm: xem file phân công gốc (`PhanCong_DoAn18...md`) Mục 2.

**Nguyên tắc dữ liệu:** không nộp/push file dữ liệu thật (Parquet, MinIO, Kafka
messages...) lên Git. Mỗi người tự chạy lại script trên máy mình để tái tạo dữ liệu
vào hạ tầng chung — giảng viên tái tạo lại toàn bộ bằng cách chạy các script theo
đúng thứ tự A → B → C → D.

## 3. Cách chạy toàn bộ hệ thống

```bash
# Lần đầu / mỗi khi cần khởi động lại toàn bộ
docker compose up -d

# Xem log 1 service cụ thể (VD: airflow-scheduler)
docker compose logs -f airflow-scheduler

# Dừng (giữ lại dữ liệu)
docker compose down

# Dừng và xoá luôn toàn bộ dữ liệu (dùng khi muốn làm lại từ đầu)
docker compose down -v
```

Lần đầu chạy `docker compose up -d`, Airflow sẽ tải image khá lâu (~vài phút tuỳ mạng).
Đợi tất cả container ở trạng thái `Up` trước khi thao tác (`docker compose ps` để kiểm tra).

> **Ghi chú hạ tầng (quan trọng — đọc trước khi chạy):** Kể từ 9/2025, Bitnami
> (Broadcom) đã gỡ toàn bộ tag phiên bản miễn phí của image Spark khỏi Docker Hub
> (`bitnami/spark:3.5` không còn pull được). File `docker-compose.yml` này đã được
> cập nhật dùng `bitnamilegacy/spark:3.5` (bản đóng băng, không cập nhật thêm nhưng
> vẫn tải được, phù hợp cho đồ án học tập) cho cả `spark-master` và `spark-worker`.
> Nếu bạn từng clone repo trước khi bản sửa này được push, hãy `git pull` để lấy
> đúng image mới trước khi chạy `docker compose up -d`.

## 4. Danh sách dịch vụ & cổng truy cập

| Dịch vụ | URL / Cổng | Tài khoản mặc định | Phụ trách |
|---|---|---|---|
| Kafka UI | http://localhost:8090 | — | A |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` | A, B |
| Spark UI | http://localhost:8080 | — | B |
| Postgres (warehouse) | `localhost:5432` | `warehouse` / `warehouse123`, db `food_delivery_dw` | C |
| Airflow | http://localhost:8081 | `admin` / `admin` | D |
| Metabase | http://localhost:3000 | Tự tạo tài khoản lần đầu truy cập | D |

> Mỗi thành viên tự tạo file `.env` riêng trong thư mục của mình (không push lên
> Git) để trỏ đúng vào các endpoint ở bảng trên. Xem `.env.example` (nếu có) trong
> từng thư mục thành viên để biết tên biến cần khai báo.

## 5. Lưu ý tài nguyên máy

Cấu hình đủ chạy: khuyến nghị ~16GB RAM. Nếu máy yếu hơn, có thể tạm comment
(thêm `#` phía trước) 2 service ít cần dùng liên tục trong lúc code phần khác:
- `spark-worker` (nếu chưa cần chạy Spark job)
- `metabase` (nếu chưa tới bước dựng dashboard)

Sau đó `docker compose up -d` lại để áp dụng.

## 6. Quy trình Git cho cả nhóm

- Mỗi người **chỉ sửa file trong thư mục của mình** (`ingestion_A/`, `spark_B/`...)
  để tránh conflict.
- Trước khi code: `git pull`
- Sau khi code xong 1 phần: `git add .` → `git commit -m "mô tả ngắn"` → `git push`
- Làm thường xuyên, không dồn cuối kỳ (lịch sử commit là căn cứ chấm điểm cá nhân).
- File `docker-compose.yml` ở gốc là điểm chung — báo nhóm trong group chat trước
  khi sửa để tránh ghi đè lẫn nhau.

## 7. Checklist tiến độ (tự tick khi xong)

- [ ] A: `producer.py` chạy được, dữ liệu vào MinIO `raw/` đúng schema
- [ ] B: `batch_processing.py` + `streaming_processing.py`, Parquet partition đúng trong `curated/`
- [ ] C: Star schema + dbt project, `dbt test` pass toàn bộ
- [ ] D: DAG Airflow chạy đủ 5 task, dashboard Metabase, model ML, sơ đồ kiến trúc + chi phí GCP

---

*File tạm — sẽ được thay bằng README chính thức nộp giảng viên sau khi hoàn thành đồ án.*