select
    order_id,
    restaurant_key,
    driver_key,
    zone_key,
    date_key,
    distance_km,
    prep_time_min,
    delivery_time_min,
    order_amount,
    is_late
from {{ source('raw', 'fact_order') }}