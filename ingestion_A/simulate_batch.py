"""
simulate_batch.py — Việc #1 và #2 của Thành viên A.

Làm 2 việc:
1. Đọc file CSV gốc từ Kaggle (10 cột thật), đổi tên cột về snake_case chuẩn.
2. Sinh thêm các cột mô phỏng (Mục 2.2) rồi ghi Parquet ra MinIO raw/.

Cách chạy:
    python simulate_batch.py --input data/food_delivery_raw.csv

Nếu chưa có file Kaggle thật, dùng luôn dữ liệu giả để code trước:
    python simulate_batch.py --fake 200
"""

import argparse
from datetime import datetime, timedelta

import pandas as pd

from common import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET,
    haversine_km, make_restaurant_id, random_city, random_prep_minutes,
    random_order_amount, simulate_delivery_minutes, random_order_date_last_30_days,
    random_time_of_day,
)

# Mục 2.2 — đổi tên cột gốc Kaggle -> tên chuẩn của cả nhóm
COLUMN_RENAME_MAP = {
    "ID": "order_id",
    "Delivery_person_ID": "driver_id",
    "Delivery_person_Age": "driver_age",
    "Delivery_person_Ratings": "driver_rating",
    "Restaurant_latitude": "restaurant_lat",
    "Restaurant_longitude": "restaurant_lon",
    "Delivery_location_latitude": "delivery_lat",
    "Delivery_location_longitude": "delivery_lon",
    "Type_of_order": "order_type",
    "Type_of_vehicle": "vehicle_type",
}

REQUIRED_RAW_COLUMNS = list(COLUMN_RENAME_MAP.keys())


def load_and_rename(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    missing = [c for c in REQUIRED_RAW_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"File CSV thiếu cột: {missing}. "
            f"Kiểm tra lại đúng file Kaggle 'Food Delivery Time' 10 cột (Mục 2.0)."
        )

    df = df[REQUIRED_RAW_COLUMNS].rename(columns=COLUMN_RENAME_MAP)

    # ép kiểu đúng theo Mục 2.2
    df["order_id"] = df["order_id"].astype(str)
    df["driver_id"] = df["driver_id"].astype(str)
    df["driver_age"] = pd.to_numeric(df["driver_age"], errors="coerce").astype("Int64")
    df["driver_rating"] = pd.to_numeric(df["driver_rating"], errors="coerce")
    for col in ["restaurant_lat", "restaurant_lon", "delivery_lat", "delivery_lon"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def make_fake_rows(n: int) -> pd.DataFrame:
    """Sinh dữ liệu giả 10 cột 'như Kaggle' — chỉ dùng khi chưa có file thật,
    để code song song từ ngày 1 (Mục 2.5)."""
    import random
    rows = []
    for i in range(n):
        rlat, rlon = round(random.uniform(10.7, 10.9), 5), round(random.uniform(106.6, 106.8), 5)
        dlat, dlon = round(random.uniform(10.7, 10.9), 5), round(random.uniform(106.6, 106.8), 5)
        rows.append({
            "order_id": f"ORD{i:05d}",
            "driver_id": f"DEL{random.randint(1, 50):03d}",
            "driver_age": random.randint(18, 50),
            "driver_rating": round(random.uniform(3.0, 5.0), 1),
            "restaurant_lat": rlat,
            "restaurant_lon": rlon,
            "delivery_lat": dlat,
            "delivery_lon": dlon,
            "order_type": random.choice(["Meal", "Snack", "Drinks", "Buffet"]),
            "vehicle_type": random.choice(["motorcycle", "scooter", "bicycle"]),
        })
    return pd.DataFrame(rows)


def add_simulated_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Sinh thêm các cột không có trong Kaggle — Mục 2.2, luật cho BATCH."""
    df = df.copy()

    df["restaurant_id"] = df.apply(
        lambda r: make_restaurant_id(r["restaurant_lat"], r["restaurant_lon"]), axis=1
    )
    df["city"] = [random_city() for _ in range(len(df))]
    df["order_date"] = [random_order_date_last_30_days() for _ in range(len(df))]

    time_ordered_list = []
    time_picked_list = []
    time_delivered_list = []

    for _, r in df.iterrows():
        h, m, s = random_time_of_day()
        t_ordered = datetime.combine(r["order_date"], datetime.min.time()) + timedelta(
            hours=h, minutes=m, seconds=s
        )
        prep_min = random_prep_minutes()
        t_picked = t_ordered + timedelta(minutes=prep_min)

        dist = haversine_km(r["restaurant_lat"], r["restaurant_lon"], r["delivery_lat"], r["delivery_lon"])
        delivery_min = simulate_delivery_minutes(dist)
        t_delivered = t_picked + timedelta(minutes=delivery_min)

        time_ordered_list.append(t_ordered)
        time_picked_list.append(t_picked)
        time_delivered_list.append(t_delivered)

    df["time_ordered"] = time_ordered_list
    df["time_picked"] = time_picked_list
    df["time_delivered"] = time_delivered_list
    df["order_amount"] = [random_order_amount() for _ in range(len(df))]

    return df


def write_to_minio_raw(df: pd.DataFrame, filename: str = "orders.parquet"):
    storage_options = {
        "key": MINIO_ACCESS_KEY,
        "secret": MINIO_SECRET_KEY,
        "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
    }
    path = f"s3://{MINIO_BUCKET}/raw/{filename}"
    df.to_parquet(
        path,
        storage_options=storage_options,
        index=False,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
    print(f"[OK] Đã ghi {len(df)} dòng vào {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Đường dẫn file CSV Kaggle thật (10 cột)")
    parser.add_argument("--fake", type=int, help="Số dòng dữ liệu giả để sinh thay vì đọc file thật")
    parser.add_argument("--out", default="orders.parquet", help="Tên file parquet ghi vào raw/")
    parser.add_argument("--local-only", action="store_true",
                         help="Chỉ lưu file .parquet ra local (không ghi MinIO) — để test nhanh")
    args = parser.parse_args()

    if args.fake:
        df = make_fake_rows(args.fake)
    elif args.input:
        df = load_and_rename(args.input)
    else:
        raise SystemExit("Phải truyền --input <file.csv> hoặc --fake <n>")

    df = add_simulated_columns(df)
    print(df.head())
    print(f"Tổng số dòng: {len(df)}, tổng số cột: {len(df.columns)}")

    if args.local_only:
        df.to_parquet(args.out, index=False)
        print(f"[OK] Đã ghi local: {args.out}")
    else:
        write_to_minio_raw(df, args.out)


if __name__ == "__main__":
    main()
