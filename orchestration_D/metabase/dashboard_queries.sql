-- ============================================================
-- Metabase Dashboard Queries — Đồ án 18 (Giao đồ ăn), Phần D
-- Schema tham chiếu: Mục 2.4 file phân công
--   fact_order(order_id, restaurant_key, driver_key, zone_key, date_key,
--              distance_km, prep_time_min, delivery_time_min, order_amount, is_late)
--   dim_restaurant(restaurant_key, restaurant_id, restaurant_lat, restaurant_lon, city)
--   dim_driver(driver_key, driver_id, driver_age, driver_rating, vehicle_type)
--   dim_zone(zone_key, city)
--   dim_date(date_key, order_date, hour, day_of_week)
--
-- Cách dùng: Metabase → New Question → Native query (SQL editor)
-- → chọn database postgres-warehouse → dán 1 trong 3 câu bên dưới
-- → chọn loại biểu đồ (ghi chú kèm mỗi câu) → Save → pin vào Dashboard.
-- ============================================================


-- 1) SỐ ĐƠN THEO GIỜ / KHU VỰC
-- Chart gợi ý: Bar chart, X = hour, Series (group by) = city, Y = order_count
SELECT
    dd.hour            AS hour,
    dz.city            AS city,
    COUNT(*)           AS order_count
FROM fact_order fo
JOIN dim_date dd ON fo.date_key = dd.date_key
JOIN dim_zone dz ON fo.zone_key = dz.zone_key
GROUP BY dd.hour, dz.city
ORDER BY dd.hour, dz.city;


-- 2) THỜI GIAN GIAO TRUNG BÌNH THEO NGÀY / KHU VỰC
-- Chart gợi ý: Line chart, X = order_date, Series = city, Y = avg_delivery_time_min
SELECT
    dd.order_date                          AS order_date,
    dz.city                                AS city,
    ROUND(AVG(fo.delivery_time_min)::numeric, 1)   AS avg_delivery_time_min,
    COUNT(*)                               AS order_count
FROM fact_order fo
JOIN dim_date dd ON fo.date_key = dd.date_key
JOIN dim_zone dz ON fo.zone_key = dz.zone_key
GROUP BY dd.order_date, dz.city
ORDER BY dd.order_date, dz.city;


-- 3) TOP 10 NHÀ HÀNG CÓ TỶ LỆ GIAO TRỄ CAO NHẤT
-- Chart gợi ý: Row chart (bar ngang), X = restaurant_id, Y = late_orders
-- Lọc HAVING > 0 để loại nhà hàng chưa từng bị trễ; sắp theo số đơn trễ giảm dần.
SELECT
    dr.restaurant_id                                                       AS restaurant_id,
    dr.city                                                                AS city,
    COUNT(*) FILTER (WHERE fo.is_late)                                     AS late_orders,
    COUNT(*)                                                               AS total_orders,
    ROUND((100.0 * COUNT(*) FILTER (WHERE fo.is_late) / COUNT(*))::numeric, 1)        AS late_rate_pct
FROM fact_order fo
JOIN dim_restaurant dr ON fo.restaurant_key = dr.restaurant_key
GROUP BY dr.restaurant_id, dr.city
HAVING COUNT(*) FILTER (WHERE fo.is_late) > 0
ORDER BY late_orders DESC
LIMIT 10;
