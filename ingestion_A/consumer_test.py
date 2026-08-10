"""
consumer_test.py — Việc #4 của Thành viên A.

Consumer đơn giản để TỰ KIỂM TRA (không phải phần B dùng):
- In từng message ra để nhìn bằng mắt xem đúng format Mục 2.3 chưa.
- Theo dõi mỗi order_id có nhận đủ 4 trạng thái, đúng thứ tự
  placed -> cooking -> picked -> delivered hay không.

Cách chạy (mở song song lúc producer.py đang chạy):
    python consumer_test.py
    # Ctrl+C để dừng và xem báo cáo tổng kết
"""

import json
from collections import defaultdict

from kafka import KafkaConsumer

from common import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, ORDER_STATUSES

EXPECTED_SEQUENCE = ORDER_STATUSES  # ["placed", "cooking", "picked", "delivered"]


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="a-consumer-test",
    )

    order_history = defaultdict(list)  # order_id -> [status, status, ...]
    total_messages = 0
    format_errors = 0

    print(f"[LISTEN] Đang lắng nghe topic '{KAFKA_TOPIC}' trên {KAFKA_BOOTSTRAP_SERVERS}...")
    print("Nhấn Ctrl+C để dừng và xem báo cáo.\n")

    required_fields = {
        "order_id", "status", "timestamp", "restaurant_id", "driver_id",
        "city", "restaurant_lat", "restaurant_lon", "delivery_lat", "delivery_lon",
    }

    try:
        for msg in consumer:
            event = msg.value
            total_messages += 1

            missing = required_fields - event.keys()
            if missing or event.get("status") not in ORDER_STATUSES:
                format_errors += 1
                print(f"[LỖI FORMAT] {event} — thiếu field: {missing or 'không có'}")
                continue

            order_history[event["order_id"]].append(event["status"])
            print(f"  {event['order_id']:12s} | {event['status']:10s} | {event['city']:12s} | {event['timestamp']}")

    except KeyboardInterrupt:
        print("\n[STOP] Dừng lắng nghe.\n")
    finally:
        consumer.close()
        print_report(order_history, total_messages, format_errors)


def print_report(order_history, total_messages, format_errors):
    print("=" * 60)
    print("BÁO CÁO KIỂM TRA")
    print("=" * 60)
    print(f"Tổng số message nhận được: {total_messages}")
    print(f"Message sai format: {format_errors}")
    print(f"Số đơn hàng khác nhau đã thấy: {len(order_history)}")

    complete = sum(1 for seq in order_history.values() if seq == EXPECTED_SEQUENCE)
    incomplete = len(order_history) - complete
    print(f"Đơn đã nhận đủ 4 trạng thái đúng thứ tự: {complete}")
    print(f"Đơn chưa đủ / sai thứ tự (bình thường nếu vừa mới dừng producer giữa chừng): {incomplete}")

    wrong_order = [oid for oid, seq in order_history.items()
                   if seq != EXPECTED_SEQUENCE[:len(seq)]]
    if wrong_order:
        print(f"[CẢNH BÁO] {len(wrong_order)} đơn có thứ tự trạng thái SAI: {wrong_order[:5]} ...")


if __name__ == "__main__":
    main()
