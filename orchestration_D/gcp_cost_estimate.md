# Ước tính chi phí GCP (giả định) — Đồ án 18: Giao đồ ăn

> Nhóm chạy pipeline local bằng Docker Compose nên **không phát sinh chi phí thật**. Bảng dưới đây ước tính chi phí **nếu triển khai thật trên GCP** ở quy mô nhỏ (demo môn học: batch chạy hằng ngày + streaming lưu lượng thấp), để đáp ứng yêu cầu Mục 8 của đề bài. Giá là tham khảo công khai tại thời điểm viết tài liệu — nên kiểm tra lại giá mới nhất khi báo cáo (cloud.google.com/pricing).

## Mapping dịch vụ open-source (đang chạy) → GCP tương đương

| Local (Docker Compose) | GCP tương đương |
|---|---|
| MinIO | Cloud Storage (GCS) |
| Apache Kafka (`kafka`, `zookeeper`) | Managed Service for Apache Kafka / Pub/Sub |
| Apache Spark (`spark-master`, `spark-worker`) | Dataproc (khuyến nghị Dataproc Serverless) |
| PostgreSQL (`postgres-warehouse`) | BigQuery |
| Apache Airflow (`airflow-webserver`, `airflow-scheduler`) | Cloud Composer |
| Metabase | Looker Studio (miễn phí) hoặc Metabase tự host trên Compute Engine |

## Ước tính chi phí hằng tháng (quy mô nhỏ, giả định)

| Dịch vụ | Cấu hình giả định | Ước tính/tháng |
|---|---|---|
| Cloud Composer (Airflow) | môi trường nhỏ (small), chạy 24/7 | ~$200–300 |
| Dataproc Serverless (Spark batch) | vài giờ chạy/ngày, batch nhỏ | ~$20–50 |
| BigQuery | on-demand, dưới 1TB quét/tháng | ~$10–30 |
| Cloud Storage (GCS) | vài chục GB, storage class Standard | ~$1–3 |
| Managed Service for Apache Kafka / Pub/Sub | throughput thấp | ~$30–100 |
| Compute Engine (nếu tự host Metabase) | e2-small | ~$15–25 |
| **Tổng ước tính** | | **~$320–565 / tháng** |

## Ghi chú khi báo cáo

- **Cloud Composer là phần đắt nhất** vì môi trường Airflow chạy 24/7 kể cả khi không có job nào — đây là lý do nhiều nhóm cân nhắc dùng **Dataproc Serverless** (trả theo giờ chạy thật) thay vì cluster Dataproc cố định, để giảm chi phí.
- Nếu chỉ chạy batch 1 lần/ngày, có thể cân nhắc trigger Cloud Composer theo lịch thay vì giữ cluster luôn bật, hoặc dùng Cloud Scheduler + Cloud Functions cho các workload nhỏ hơn.
- Nên trích dẫn nguồn giá chính thức (cloud.google.com/pricing) trực tiếp trong báo cáo thay vì chỉ dùng bảng ước tính này, vì giá GCP thay đổi theo thời gian và khu vực (region).
