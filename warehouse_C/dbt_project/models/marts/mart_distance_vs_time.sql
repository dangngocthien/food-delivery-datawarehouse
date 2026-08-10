select
    distance_km,
    delivery_time_min,
    is_late
from {{ ref('stg_orders') }}