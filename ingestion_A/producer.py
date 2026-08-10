"""
producer.py — Việc #3 và #5 của Thành viên A.

Mô phỏng đơn hàng theo thời gian thực: mỗi đơn sinh 4 sự kiện trạng thái
(placed -> cooking -> picked -> delivered) gửi vào Kafka topic 'order-events',
đúng format JSON ở Mục 2.3. Chạy liên tục (loop) để tạo đủ khối lượng cho B
demo Spark Structured Streaming (windowing / tổng hợp thời gian thực).

Cách chạy:
    python producer.py
    # Ctrl+C để dừng

Biến môi trường tuỳ chỉnh (đặt trong .env hoặc export trước khi chạy):
    SIM_SPEED_SECONDS_PER_MIN=2   # 1 phút mô phỏng = 2 giây thật (mặc định)
    NEW_ORDER_INTERVAL_SEC=1.5    # cứ ~1.5s thật lại có 1 đơn mới
    MAX_ORDERS=0                  # 0 = chạy vô hạn, >0 = dừng sau N đơn (để test)
"""

import asyncio
import json
import os
import random
from datetime import datetime

from kafka import KafkaProducer

from common import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
    haversine_km, make_restaurant_id, random_city, random_prep_minutes,
    simulate_delivery_minutes,
)

SIM_SPEED_SECONDS_PER_MIN = float(os.getenv("SIM_SPEED_SECONDS_PER_MIN", "2"))
NEW_ORDER_INTERVAL_SEC = float(os.getenv("NEW_ORDER_INTERVAL_SEC", "1.5"))
MAX_ORDERS = int(os.getenv("MAX_ORDERS", "0"))  # 0 = vô hạn

DRIVER_IDS = [f"DEL{n:03d}" for n in range(1, 51)]


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        linger_ms=50,
    )


def random_order_geo():
    rlat, rlon = round(random.uniform(10.70, 10.90), 5), round(random.uniform(106.60, 106.80), 5)
    dlat, dlon = round(random.uniform(10.70, 10.90), 5), round(random.uniform(106.60, 106.80), 5)
    return rlat, rlon, dlat, dlon


def make_event(order_id, status, order_ctx):
    """Đúng format JSON ở Mục 2.3."""
    return {
        "order_id": order_id,
        "status": status,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "restaurant_id": order_ctx["restaurant_id"],
        "driver_id": order_ctx["driver_id"],
        "city": order_ctx["city"],
        "restaurant_lat": order_ctx["restaurant_lat"],
        "restaurant_lon": order_ctx["restaurant_lon"],
        "delivery_lat": order_ctx["delivery_lat"],
        "delivery_lon": order_ctx["delivery_lon"],
    }


async def run_one_order(producer: KafkaProducer, order_id: str, counter: dict):
    rlat, rlon, dlat, dlon = random_order_geo()
    order_ctx = {
        "restaurant_id": make_restaurant_id(rlat, rlon),
        "driver_id": random.choice(DRIVER_IDS),
        "city": random_city(),
        "restaurant_lat": rlat,
        "restaurant_lon": rlon,
        "delivery_lat": dlat,
        "delivery_lon": dlon,
    }

    prep_min = random_prep_minutes()
    distance = haversine_km(rlat, rlon, dlat, dlon)
    delivery_min = simulate_delivery_minutes(distance)

    def send(status):
        producer.send(KAFKA_TOPIC, value=make_event(order_id, status, order_ctx))
        counter["sent"] += 1

    # placed: ngay khi đơn được tạo
    send("placed")

    # cooking: giữa chừng lúc chuẩn bị món
    await asyncio.sleep((prep_min / 2) * SIM_SPEED_SECONDS_PER_MIN)
    send("cooking")

    # picked: tài xế lấy món xong (= time_picked trong batch)
    await asyncio.sleep((prep_min / 2) * SIM_SPEED_SECONDS_PER_MIN)
    send("picked")

    # delivered: giao xong (= time_delivered trong batch), dựa trên khoảng cách
    await asyncio.sleep(delivery_min * SIM_SPEED_SECONDS_PER_MIN)
    send("delivered")


async def main():
    producer = make_producer()
    counter = {"sent": 0, "orders": 0}
    tasks = []

    print(f"[START] Kết nối Kafka: {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}")
    print(f"[CONFIG] SIM_SPEED_SECONDS_PER_MIN={SIM_SPEED_SECONDS_PER_MIN}, "
          f"NEW_ORDER_INTERVAL_SEC={NEW_ORDER_INTERVAL_SEC}, MAX_ORDERS={MAX_ORDERS or '∞'}")

    try:
        while MAX_ORDERS == 0 or counter["orders"] < MAX_ORDERS:
            order_id = f"ORD{counter['orders']:06d}"
            tasks.append(asyncio.create_task(run_one_order(producer, order_id, counter)))
            counter["orders"] += 1

            if counter["orders"] % 20 == 0:
                print(f"[PROGRESS] Đã tạo {counter['orders']} đơn, đã gửi {counter['sent']} sự kiện")

            await asyncio.sleep(NEW_ORDER_INTERVAL_SEC)

        # đợi các đơn còn dang dở gửi nốt event delivered
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("\n[STOP] Dừng theo yêu cầu người dùng.")
    finally:
        producer.flush()
        producer.close()
        print(f"[DONE] Tổng: {counter['orders']} đơn, {counter['sent']} sự kiện đã gửi vào '{KAFKA_TOPIC}'.")


if __name__ == "__main__":
    asyncio.run(main())
