"""
batch_processing.py — Việc #1 và #2 của Thành viên B (Spark & Data Lake).

Luồng xử lý (đúng Mục 2 file phân công — KHÔNG tự đổi tên cột/schema):
1. Đọc dữ liệu từ MinIO raw/ (do A ghi, đúng schema Mục 2.2).
2. Làm sạch (loại bỏ dòng thiếu cột bắt buộc / toạ độ vô lý), ghi ra cleansed/.
3. Tính distance_km (Haversine), prep_time_min, delivery_time_min, is_late.
4. Ghi Parquet ra curated/, partition theo order_date và city
   (đúng cấu trúc Mục 2.1: curated/order_date=YYYY-MM-DD/city=<ten_thanh_pho>/*.parquet).

Cách chạy — bên trong container spark-master (sau khi `docker compose up -d`):
    docker exec -it spark-master spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        /opt/airflow/project/spark_B/batch_processing.py

Nếu chưa có dữ liệu thật của A, dùng sample_data.csv trong thư mục này để test độc lập
(đúng tinh thần Mục 2.5 — code song song, không chờ A):
    docker exec -it spark-master spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        /opt/airflow/project/spark_B/batch_processing.py \
        --local-input /opt/airflow/project/spark_B/sample_data.csv --local-output
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ---------- Cấu hình lấy từ biến môi trường (.env) — đồng bộ với common.py của A ----------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "food-delivery-lake")

RAW_PATH = f"s3a://{MINIO_BUCKET}/raw/"
CLEANSED_PATH = f"s3a://{MINIO_BUCKET}/cleansed/"
CURATED_PATH = f"s3a://{MINIO_BUCKET}/curated/"

SLA_LATE_MINUTES = 45  # đúng ngưỡng thống nhất ở common.py của A (Mục 2.2)

# Cột bắt buộc phải có giá trị hợp lệ mới coi là dòng "sạch" (Mục 2.2)
REQUIRED_COLUMNS = [
    "order_id", "driver_id", "restaurant_lat", "restaurant_lon",
    "delivery_lat", "delivery_lon", "order_date",
    "time_ordered", "time_picked", "time_delivered",
]


def build_spark(app_name: str = "batch_processing") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # dynamic: chỉ ghi đè đúng partition (order_date, city) liên quan, không xoá partition khác
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_raw(spark: SparkSession, local_input: str | None):
    """Đọc dữ liệu raw. Nếu --local-input được truyền (CSV mẫu Mục 2.5),
    đọc từ đó thay vì MinIO — dùng khi chưa có dữ liệu thật của A."""
    if local_input:
        return spark.read.option("header", "true").option("inferSchema", "true").csv(local_input)
    return spark.read.parquet(RAW_PATH)


def cast_types(df):
    """Ép kiểu tường minh — tránh lệch kiểu khi đọc từ CSV mẫu (Mục 2.5) so với Parquet của A."""
    df = df.withColumn("order_date", F.to_date("order_date"))
    for c in ["time_ordered", "time_picked", "time_delivered"]:
        df = df.withColumn(c, F.to_timestamp(c))
    for c in ["restaurant_lat", "restaurant_lon", "delivery_lat", "delivery_lon",
              "driver_rating", "order_amount"]:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(DoubleType()))
    if "driver_age" in df.columns:
        df = df.withColumn("driver_age", F.col("driver_age").cast("int"))
    return df


def clean(df):
    """Loại bỏ dòng thiếu cột bắt buộc hoặc toạ độ/rating vô lý."""
    df = df.dropna(subset=REQUIRED_COLUMNS)
    df = df.filter(
        F.col("restaurant_lat").between(-90, 90)
        & F.col("delivery_lat").between(-90, 90)
        & F.col("restaurant_lon").between(-180, 180)
        & F.col("delivery_lon").between(-180, 180)
    )
    if "driver_rating" in df.columns:
        df = df.filter(F.col("driver_rating").between(0, 5) | F.col("driver_rating").isNull())
    return df.dropDuplicates(["order_id"])


def haversine_km(lat1, lon1, lat2, lon2):
    """Haversine bằng hàm Spark SQL gốc (không dùng UDF Python để giữ hiệu năng phân tán).
    Công thức đúng Mục 2.2 — cột distance_km, nhóm C sẽ dùng lại cột này."""
    r = F.lit(6371.0)  # bán kính Trái Đất (km)
    phi1, phi2 = F.radians(lat1), F.radians(lat2)
    dphi = F.radians(lat2 - lat1)
    dlambda = F.radians(lon2 - lon1)
    a = F.sin(dphi / 2) ** 2 + F.cos(phi1) * F.cos(phi2) * F.sin(dlambda / 2) ** 2
    c = F.lit(2) * F.atan2(F.sqrt(a), F.sqrt(F.lit(1) - a))
    return (r * c).cast(DoubleType())


def enrich(df):
    """Tính distance_km, prep_time_min, delivery_time_min, is_late — đúng công thức Mục 2.2."""
    df = df.withColumn(
        "distance_km",
        haversine_km(F.col("restaurant_lat"), F.col("restaurant_lon"),
                     F.col("delivery_lat"), F.col("delivery_lon")),
    )
    df = df.withColumn(
        "prep_time_min",
        (F.unix_timestamp("time_picked") - F.unix_timestamp("time_ordered")) / 60.0,
    )
    df = df.withColumn(
        "delivery_time_min",
        (F.unix_timestamp("time_delivered") - F.unix_timestamp("time_picked")) / 60.0,
    )
    df = df.withColumn("is_late", F.col("delivery_time_min") > F.lit(SLA_LATE_MINUTES))
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--local-input", help="Đọc CSV mẫu local thay vì MinIO raw/ (Mục 2.5)")
    parser.add_argument("--local-output", action="store_true",
                         help="Ghi cleansed/curated ra thư mục local thay vì MinIO — để test nhanh")
    args = parser.parse_args()

    spark = build_spark()

    df_raw = cast_types(read_raw(spark, args.local_input))
    print(f"[RAW] {df_raw.count()} dòng, {len(df_raw.columns)} cột")

    df_cleansed = clean(df_raw)
    print(f"[CLEANSED] {df_cleansed.count()} dòng còn lại sau khi làm sạch")

    cleansed_out = "cleansed_local" if args.local_output else CLEANSED_PATH
    df_cleansed.write.mode("overwrite").parquet(cleansed_out)
    print(f"[OK] Đã ghi cleansed/ vào {cleansed_out}")

    df_curated = enrich(df_cleansed)

    curated_out = "curated_local" if args.local_output else CURATED_PATH
    (
        df_curated.write.mode("overwrite")
        .partitionBy("order_date", "city")
        .parquet(curated_out)
    )
    print(f"[OK] Đã ghi curated/ (partition order_date, city) vào {curated_out}")

    spark.stop()


if __name__ == "__main__":
    main()
