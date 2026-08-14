# Đánh giá model dự đoán ETA (delivery_time_min)

## Feature sử dụng (4)
- `distance_km`
- `driver_age`
- `driver_rating`
- `vehicle_type` (one-hot encode)

## Feature KHÔNG dùng — giới hạn dữ liệu (data limitation)
- **`order_type`**: không tồn tại trong warehouse. Theo data contract Mục 2.4
  (file phân công), `Fact_Order` và `Dim_Driver` đều không có cột này —
  Kaggle gốc có `Type_of_order` nhưng khi thiết kế schema Postgres, cột này
  không được đưa vào bất kỳ bảng fact/dimension nào. Đây là gap giữa contract
  gốc và yêu cầu ML (đề bài yêu cầu dùng 5 feature, warehouse chỉ hỗ trợ 4).
  Ghi rõ mục này trong báo cáo phần "Giới hạn dữ liệu".

## Nguồn dữ liệu
JOIN trực tiếp `Fact_Order` + `Dim_Driver` (2 bảng gốc C đã tạo theo
`warehouse_C/create_tables.sql`) trong `train_eta_model.py` — không cần
thêm dbt model, không đụng vào phần việc của C. `mart_delivery_summary` và
`mart_distance_vs_time` (2 mart C đã làm) là bảng tổng hợp (group by),
đúng mục đích dashboard, không dùng được cho train row-level.

## Kết quả (điền sau khi chạy `train_eta_model.py` với data thật)

| Metric | Model (RandomForest, 4 feature) | Baseline (mean) |
|---|---|---|
| MAE (phút) | | |
| RMSE (phút) | | — |
| R² | | — |

## Nhận xét
(điền sau khi có kết quả thật — model có tốt hơn baseline bao nhiêu %, feature
nào quan trọng nhất theo `model.named_steps['regressor'].feature_importances_`)