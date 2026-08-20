## Tóm tắt phiên làm việc — Đồ án 18: Giao đồ ăn, phần D

### Thông tin project (đã biết từ trước)
- Đồ án 18 trong ngân hàng 20 đồ án Big Data (môn Dữ liệu lớn, ĐH Phan Thiết), chủ đề **Giao đồ ăn**, cloud ấn định **GCP**, chạy thay thế 100% bằng Docker Compose local.
- Repo: `D:\food-delivery-datawarehouse`, 4 thư mục: `ingestion_A`, `spark_B`, `warehouse_C`, `orchestration_D`.
- Bạn là **D** (Orchestration & Analytics/AI).
- Container: `airflow-scheduler`, `airflow-webserver`, `spark-master`, `spark-worker`, `minio`, `kafka`, `postgres-warehouse`, `postgres-airflow`, `metabase`, `kafka-ui`, `zookeeper`. DAG id: `food_delivery_pipeline_dag`, 6 task đã pass từ trước (kể cả `task_create_tables` do D tự thêm).
- Postgres warehouse: host=`postgres-warehouse`, db=`food_delivery_dw`, user=`warehouse`, password=`warehouse123`.

### 1. Sơ đồ kiến trúc — đã xong
Tạo `kien_truc_do_an_18.mermaid` — 2 lớp GCP gốc + Open-source thực chạy, khớp đúng 6 task trong DAG. **Lưu ý:** đề bài yêu cầu nộp draw.io, Mermaid chỉ là bản nháp, cần convert lại khi nộp (chưa làm).

### 2. Quyết định giữ phần AI/ML
Ban đầu định bỏ theo lời giảng viên "không recommend làm cuối buổi". Xác nhận lại: Mục 2 + Mục 8 đề bài yêu cầu **bắt buộc** dashboard BI + AI/ML. Kết luận: lời giảng viên là về thời điểm, không phải bỏ hẳn → giữ lại, làm mức tối thiểu.

### 3. Setup & chạy `train_eta_model.py` (`orchestration_D/ml/train_eta_model.py`)
- Sửa lỗi: container dừng (`docker compose up -d`), lỗi quyền `pip` (dùng `python -m pip install` thay vì gọi `pip` thẳng).
- Cài `requirements.txt` (pandas, numpy, scikit-learn, sqlalchemy, psycopg2-binary, joblib) vào container `airflow-scheduler`.

### 4. Phát hiện & xử lý outlier dữ liệu
- Chạy lần đầu: MAE 37.35 phút, RMSE 493.65 phút (gấp 13 lần MAE) → nghi outlier.
- Điều tra: `distance_km` max = 19,688km, 404/45,403 dòng (~0.9%) có `distance_km > 500km`, rải trên 238 nhà hàng.
- Nguyên nhân xác nhận: toạ độ `dim_restaurant.restaurant_lat/lon` của các dòng lỗi mang **dấu âm** (rơi vào Nam Mỹ) trong khi dataset gốc là Ấn Độ (toạ độ đúng phải dương) — nghi là lỗi/nhiễu có sẵn trong Kaggle gốc.
- Đã thêm đoạn lọc `distance_km <= 20` vào `train_eta_model.py` (chỉ ở tầng ML, **chưa** sửa ở `batch_processing.py` của B).

### 5. Kết quả model sau khi lọc (42,891/45,403 dòng)
| Metric | Model | Baseline |
|---|---|---|
| MAE | 4.66 phút | 14.83 phút |
| RMSE | 6.10 phút | 17.95 phút |
| R² | 0.8844 | — |

Tốt hơn baseline 68.6%. Feature importance: `distance_km` chiếm 98.7%, các feature còn lại (`driver_age`, `driver_rating`, `vehicle_type`) không đáng kể.

### 6. Đã hoàn thiện `model_evaluation.md`
File đầy đủ (feature dùng/không dùng, lý do lọc outlier, kết quả, feature importance, nhận xét) — đã gửi bạn, cần copy đè vào `orchestration_D/ml/model_evaluation.md`. Vai trò của file: **nguồn để viết vào báo cáo Word**, không phải sản phẩm giảng viên chấm trực tiếp.


