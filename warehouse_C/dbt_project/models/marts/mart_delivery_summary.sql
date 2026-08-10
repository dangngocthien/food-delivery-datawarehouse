select
    dz.city,
    dd.hour,
    count(f.order_id) as total_orders,
    avg(f.delivery_time_min) as avg_delivery_time,
    avg(f.distance_km) as avg_distance_km,
    sum(case when f.is_late then 1 else 0 end) as late_orders
from {{ ref('stg_orders') }} f
join {{ source('raw', 'dim_zone') }} dz on f.zone_key = dz.zone_key
join {{ source('raw', 'dim_date') }} dd on f.date_key = dd.date_key
group by dz.city, dd.hour