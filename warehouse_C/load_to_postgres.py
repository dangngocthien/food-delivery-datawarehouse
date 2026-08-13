"""

Cài đặt cần thiết:
    pip install pandas pyarrow s3fs psycopg2-binary

Chạy độc lập để test:
    python load_to_postgres.py
"""

import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ---------- CONFIG (đọc từ biến môi trường, có giá trị mặc định khớp docker-compose) ----------

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
CURATED_PATH = os.getenv("CURATED_PATH", "s3://food-delivery-lake/curated/")

PG_HOST = os.getenv("PG_HOST", "postgres-warehouse")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "food_delivery_dw")
PG_USER = os.getenv("PG_USER", "warehouse")
PG_PASSWORD = os.getenv("PG_PASSWORD", "warehouse123")


# ---------- Đọc dữ liệu curated ----------

def read_curated() -> pd.DataFrame:
    storage_options = {
        "key": MINIO_ACCESS_KEY,
        "secret": MINIO_SECRET_KEY,
        "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
    }
    df = pd.read_parquet(CURATED_PATH, storage_options=storage_options)

    # order_date/city đến từ partition path -> ép kiểu cho chắc
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date
    df["time_ordered"] = pd.to_datetime(df["time_ordered"])
    df["hour"] = df["time_ordered"].dt.hour
    df["day_of_week"] = df["time_ordered"].dt.day_name()

    # NaN -> None để psycopg2 insert NULL đúng
    df = df.astype(object).where(pd.notnull(df), None)
    return df


# ---------- Upsert dimension (không phụ thuộc UNIQUE constraint có sẵn hay không) ----------

def upsert_dim(conn, table: str, key_col: str, match_cols: list[str],
                insert_cols: list[str], df_dim: pd.DataFrame) -> dict:
    """
    Trả về map: tuple(giá trị match_cols) -> key_col (surrogate key).
    Chỉ insert những dòng chưa tồn tại (so theo match_cols), an toàn khi chạy lại nhiều lần.
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT {key_col}, {', '.join(match_cols)} FROM {table}")
        existing_map = {tuple(row[1:]): row[0] for row in cur.fetchall()}

    def already_exists(row):
        return tuple(row[c] for c in match_cols) in existing_map

    new_rows = df_dim[~df_dim.apply(already_exists, axis=1)]

    if not new_rows.empty:
        records = [tuple(r) for r in new_rows[insert_cols].itertuples(index=False, name=None)]
        with conn.cursor() as cur:
            execute_values(cur, f"INSERT INTO {table} ({', '.join(insert_cols)}) VALUES %s", records)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(f"SELECT {key_col}, {', '.join(match_cols)} FROM {table}")
            existing_map = {tuple(row[1:]): row[0] for row in cur.fetchall()}

    return existing_map


# ---------- Upsert fact (có PK order_id -> dùng ON CONFLICT thẳng) ----------

def upsert_fact(conn, df_fact: pd.DataFrame) -> None:
    records = [tuple(r) for r in df_fact.itertuples(index=False, name=None)]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO Fact_Order (order_id, restaurant_key, driver_key, zone_key, date_key,
                                     distance_km, prep_time_min, delivery_time_min, order_amount, is_late)
            VALUES %s
            ON CONFLICT (order_id) DO UPDATE SET
                restaurant_key = EXCLUDED.restaurant_key,
                driver_key = EXCLUDED.driver_key,
                zone_key = EXCLUDED.zone_key,
                date_key = EXCLUDED.date_key,
                distance_km = EXCLUDED.distance_km,
                prep_time_min = EXCLUDED.prep_time_min,
                delivery_time_min = EXCLUDED.delivery_time_min,
                order_amount = EXCLUDED.order_amount,
                is_late = EXCLUDED.is_late
        """, records)
    conn.commit()


# ---------- Main ----------

def main():
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                             user=PG_USER, password=PG_PASSWORD)
    try:
        df = read_curated()
        print(f"Đọc {len(df)} dòng từ {CURATED_PATH}")

        # --- Dim_Restaurant ---
        dim_restaurant_df = df.drop_duplicates("restaurant_id")[
            ["restaurant_id", "restaurant_lat", "restaurant_lon", "city"]
        ]
        restaurant_map = upsert_dim(
            conn, "Dim_Restaurant", "restaurant_key", ["restaurant_id"],
            ["restaurant_id", "restaurant_lat", "restaurant_lon", "city"], dim_restaurant_df
        )

        # --- Dim_Driver ---
        dim_driver_df = df.drop_duplicates("driver_id")[
            ["driver_id", "driver_age", "driver_rating", "vehicle_type"]
        ]
        driver_map = upsert_dim(
            conn, "Dim_Driver", "driver_key", ["driver_id"],
            ["driver_id", "driver_age", "driver_rating", "vehicle_type"], dim_driver_df
        )

        # --- Dim_Zone ---
        dim_zone_df = df.drop_duplicates("city")[["city"]]
        zone_map = upsert_dim(conn, "Dim_Zone", "zone_key", ["city"], ["city"], dim_zone_df)

        # --- Dim_Date ---
        dim_date_df = df.drop_duplicates(subset=["order_date", "hour"])[
            ["order_date", "hour", "day_of_week"]
        ]
        date_map = upsert_dim(
            conn, "Dim_Date", "date_key", ["order_date", "hour"],
            ["order_date", "hour", "day_of_week"], dim_date_df
        )

        print(f"Dim_Restaurant: {len(restaurant_map)} | Dim_Driver: {len(driver_map)} | "
              f"Dim_Zone: {len(zone_map)} | Dim_Date: {len(date_map)}")

        # --- Fact_Order ---
        fact_df = pd.DataFrame({
            "order_id": df["order_id"],
            "restaurant_key": df["restaurant_id"].map(lambda x: restaurant_map[(x,)]),
            "driver_key": df["driver_id"].map(lambda x: driver_map[(x,)]),
            "zone_key": df["city"].map(lambda x: zone_map[(x,)]),
            "date_key": df.apply(lambda r: date_map[(r["order_date"], r["hour"])], axis=1),
            "distance_km": df["distance_km"],
            "prep_time_min": df["prep_time_min"],
            "delivery_time_min": df["delivery_time_min"],
            "order_amount": df["order_amount"],
            "is_late": df["is_late"],
        })

        upsert_fact(conn, fact_df)
        print(f"Fact_Order: nạp/cập nhật {len(fact_df)} dòng thành công.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
