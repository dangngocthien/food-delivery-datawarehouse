"""
streaming_processing.py — Việc #3 của Thành viên B (Spark & Data Lake).

Đọc Kafka topic 'order-events' (do A ghi, đúng format JSON Mục 2.3 — 1 message = 1 sự
kiện trạng thái đơn: placed / cooking / picked / delivered). Mỗi đơn thật sự sinh ra 4
message rải rác theo thời gian, nên cần GHÉP các message cùng order_id lại bằng stateful
streaming (applyInPandasWithState) — khi đơn nhận đủ "delivered", tính:
    - distance_km        (Haversine, từ toạ độ nhà hàng/điểm giao có sẵn trong mỗi message)
    - prep_time_min       = time_picked  - time_ordered  (mốc 'placed')
    - delivery_time_min   = time_delivered - time_picked
    - is_late              = delivery_time_min > SLA_LATE_MINUTES

Lưu ý (đúng Mục 2.3): message Kafka KHÔNG có order_amount, driver_age, driver_rating,
order_type, vehicle_type — nên output streaming chỉ có các cột tính được từ Kafka.
Muốn đầy đủ mọi cột (kể cả order_amount...) thì dùng dữ liệu batch ở curated/ (B đã ghi
qua batch_processing.py), không phải streaming.

Cách chạy — bên trong container spark-master (sau khi `docker compose up -d` và
A đang chạy `producer.py` bơm dữ liệu vào Kafka):
    docker exec -it spark-master spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        /opt/airflow/project/spark_B/streaming_processing.py

Mặc định in kết quả ra console mỗi micro-batch. Thêm --write-minio để đồng thời ghi các
đơn đã hoàn tất vào MinIO (thư mục curated/streaming/, tách khỏi curated/ của batch để
không lẫn schema).
"""

import argparse
import os

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, BooleanType,
)

# ---------- Cấu hình lấy từ biến môi trường (.env) — đồng bộ với common.py của A ----------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "food-delivery-lake")
CURATED_STREAMING_PATH = f"s3a://{MINIO_BUCKET}/curated/streaming/"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "order-events")

SLA_LATE_MINUTES = 45  # đúng ngưỡng thống nhất Mục 2.2 / common.py của A
# Đơn "mồ côi" (không bao giờ nhận đủ 4 trạng thái) sẽ bị dọn state sau khoảng thời gian này
ORPHAN_TIMEOUT = 2 * 60 * 60 * 1000

# Schema message Kafka — đúng Mục 2.3, không tự thêm/bớt cột
EVENT_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("status", StringType()),          # placed | cooking | picked | delivered
    StructField("timestamp", StringType()),
    StructField("restaurant_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("city", StringType()),
    StructField("restaurant_lat", DoubleType()),
    StructField("restaurant_lon", DoubleType()),
    StructField("delivery_lat", DoubleType()),
    StructField("delivery_lon", DoubleType()),
])

# State giữ giữa các micro-batch cho từng order_id (đơn chưa hoàn tất)
STATE_SCHEMA = StructType([
    StructField("restaurant_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("city", StringType()),
    StructField("restaurant_lat", DoubleType()),
    StructField("restaurant_lon", DoubleType()),
    StructField("delivery_lat", DoubleType()),
    StructField("delivery_lon", DoubleType()),
    StructField("t_placed", StringType()),
    StructField("t_picked", StringType()),
    StructField("t_delivered", StringType()),
])

# Output — chỉ emit khi đơn đã có đủ 'delivered'
OUTPUT_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("restaurant_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("city", StringType()),
    StructField("restaurant_lat", DoubleType()),
    StructField("restaurant_lon", DoubleType()),
    StructField("delivery_lat", DoubleType()),
    StructField("delivery_lon", DoubleType()),
    StructField("time_ordered", StringType()),
    StructField("time_picked", StringType()),
    StructField("time_delivered", StringType()),
    StructField("distance_km", DoubleType()),
    StructField("prep_time_min", DoubleType()),
    StructField("delivery_time_min", DoubleType()),
    StructField("is_late", BooleanType()),
])


def haversine_km(lat1, lon1, lat2, lon2):
    import math
    r = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def update_order_state(order_id_key, events_iter, state: GroupState):
    """Ghép các sự kiện (placed/cooking/picked/delivered) của cùng 1 order_id.
    Khi đủ 'delivered' -> tính các cột Mục 2.2 và emit 1 dòng kết quả, xoá state.
    Nếu đơn không bao giờ hoàn tất trong ORPHAN_TIMEOUT -> dọn state, không emit gì."""
    order_id = order_id_key[0]

    if state.hasTimedOut:
        state.remove()
        return iter([])

    s = dict(zip(STATE_SCHEMA.fieldNames(), state.get)) if state.exists else {
        f: None for f in STATE_SCHEMA.fieldNames()
    }

    for pdf in events_iter:
        for _, row in pdf.sort_values("timestamp").iterrows():
            status = row["status"]
            if status == "placed":
                s["t_placed"] = row["timestamp"]
                s["restaurant_id"] = row["restaurant_id"]
                s["driver_id"] = row["driver_id"]
                s["city"] = row["city"]
                s["restaurant_lat"] = row["restaurant_lat"]
                s["restaurant_lon"] = row["restaurant_lon"]
                s["delivery_lat"] = row["delivery_lat"]
                s["delivery_lon"] = row["delivery_lon"]
            elif status == "picked":
                s["t_picked"] = row["timestamp"]
            elif status == "delivered":
                s["t_delivered"] = row["timestamp"]
            # 'cooking' không cần cho công thức Mục 2.2 nên không lưu riêng

    if s["t_delivered"] is not None and s["t_placed"] is not None and s["t_picked"] is not None:
        t_placed = pd.to_datetime(s["t_placed"])
        t_picked = pd.to_datetime(s["t_picked"])
        t_delivered = pd.to_datetime(s["t_delivered"])

        distance_km = haversine_km(
            s["restaurant_lat"], s["restaurant_lon"], s["delivery_lat"], s["delivery_lon"]
        )
        prep_time_min = (t_picked - t_placed).total_seconds() / 60.0
        delivery_time_min = (t_delivered - t_picked).total_seconds() / 60.0

        out = pd.DataFrame([{
            "order_id": order_id,
            "restaurant_id": s["restaurant_id"],
            "driver_id": s["driver_id"],
            "city": s["city"],
            "restaurant_lat": s["restaurant_lat"],
            "restaurant_lon": s["restaurant_lon"],
            "delivery_lat": s["delivery_lat"],
            "delivery_lon": s["delivery_lon"],
            "time_ordered": s["t_placed"],
            "time_picked": s["t_picked"],
            "time_delivered": s["t_delivered"],
            "distance_km": distance_km,
            "prep_time_min": prep_time_min,
            "delivery_time_min": delivery_time_min,
            "is_late": bool(delivery_time_min > SLA_LATE_MINUTES),
        }])
        state.remove()  # đơn đã hoàn tất -> không cần giữ state nữa
        return iter([out])

    # Đơn chưa đủ 'delivered' -> lưu state, chờ message tiếp theo
    state.update(tuple(s[f] for f in STATE_SCHEMA.fieldNames()))
    state.setTimeoutDuration(ORPHAN_TIMEOUT)
    return iter([])


def build_spark(app_name: str = "streaming_processing") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write-minio", action="store_true",
                         help="Ngoài in console, còn ghi các đơn hoàn tất vào MinIO curated/streaming/")
    parser.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    parser.add_argument("--checkpoint-dir", default="/tmp/spark-checkpoints/streaming_processing")
    args = parser.parse_args()

    spark = build_spark()

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )

    events = raw_stream.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e")
    ).select("e.*")

    completed_orders = events.groupBy("order_id").applyInPandasWithState(
        update_order_state,
        outputStructType=OUTPUT_SCHEMA,
        stateStructType=STATE_SCHEMA,
        outputMode="update",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
    )

    console_query = (
        completed_orders.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", f"{args.checkpoint_dir}/console")
        .start()
    )

    queries = [console_query]

    if args.write_minio:
        def write_batch_to_minio(batch_df, batch_id):
            if batch_df.rdd.isEmpty():
                return
            (
                batch_df.withColumn("order_date", F.to_date("time_ordered"))
                .write.mode("append")
                .partitionBy("order_date", "city")
                .parquet(CURATED_STREAMING_PATH)
            )

        minio_query = (
            completed_orders.writeStream.outputMode("update")
            .foreachBatch(write_batch_to_minio)
            .option("checkpointLocation", f"{args.checkpoint_dir}/minio")
            .start()
        )
        queries.append(minio_query)

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
