# /warehouse — Thành viên C: Data Warehouse & dbt

## Mục tiêu
Xây kho dữ liệu dạng star schema trên Postgres, biến đổi bằng dbt, đảm bảo test dữ liệu pass.

## Việc cần làm

1. **Tạo Postgres** (qua Docker), viết script tạo bảng đúng schema Mục 2.4:
   ```sql
   Fact_Order(order_id, restaurant_key, driver_key, zone_key, date_key,
              distance_km, prep_time_min, delivery_time_min, order_amount, is_late)
   Dim_Restaurant(restaurant_key, restaurant_id, restaurant_lat, restaurant_lon, city)
   Dim_Driver(driver_key, driver_id, driver_age, driver_rating, vehicle_type)
   Dim_Zone(zone_key, city)
   Dim_Date(date_key, order_date, hour, day_of_week)
   ```

2. **Viết script nạp dữ liệu** từ `curated/` (Parquet của B) vào các bảng trên.

3. **Cài dbt** (`dbt-postgres`), tạo:
   - `staging/` — đổi tên/lọc nhẹ
   - `marts/` — VD: thời gian giao trung bình theo giờ/khu vực, `distance_km` vs `delivery_time_min`

4. **Viết dbt test:** `not_null`, `unique`, `relationships` — áp trên các khóa ở Mục 2.4.

## Cài đặt cần thiết
```
pip install dbt-core dbt-postgres psycopg2
```
+ PostgreSQL chạy qua Docker, DBeaver/pgAdmin để xem dữ liệu.

## File cần có trong thư mục này
- `create_tables.sql`
- `load_to_postgres.py`
- `dbt_project/` (chứa `staging/`, `marts/`, file test)
- `requirements.txt`

## Nộp
- [x] Sơ đồ star schema
- [x] Script tạo bảng SQL
- [x] dbt project (`staging/`, `marts/`)
- [x] Kết quả `dbt test` pass toàn bộ

## Lưu ý
Không tự đổi tên cột/schema ở Mục 2.4. Nếu thấy cần đổi, báo cả nhóm trong group chat trước.
