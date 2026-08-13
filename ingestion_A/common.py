"""
common.py — Hàm & hằng số dùng chung cho simulate_batch.py và producer.py.
Mọi luật sinh dữ liệu giả lập phải đi qua đây, để batch và streaming
luôn sinh dữ liệu NHẤT QUÁN với nhau (đúng yêu cầu Mục 2.2 của file phân công).
"""
from dotenv import load_dotenv
load_dotenv()

import os
import math
import random
from datetime import datetime, timedelta

# ---------- Cấu hình lấy từ biến môi trường (.env) ----------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "food-delivery-lake")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "order-events")

# ---------- Hằng số nghiệp vụ (thống nhất với cả nhóm — Mục 2.2) ----------
CITIES = ["Ho Chi Minh", "Ha Noi", "Da Nang"]
ORDER_STATUSES = ["placed", "cooking", "picked", "delivered"]  # đúng thứ tự, Mục 2.3
SLA_LATE_MINUTES = 45  # ngưỡng is_late, B sẽ dùng lại số này khi tính ở Spark

PREP_TIME_RANGE_MIN = (10, 30)      # phút chuẩn bị món, random
ORDER_AMOUNT_RANGE_VND = (30_000, 300_000)


def haversine_km(lat1, lon1, lat2, lon2):
    """Khoảng cách theo đường chim bay giữa 2 toạ độ (km).
    Dùng NỘI BỘ để sinh delivery_time_min hợp lý — KHÔNG ghi cột distance_km
    vào raw/, vì đó là việc của B (Mục 2.2 — Nhóm C, do B tính bằng PySpark)."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def make_restaurant_id(lat, lon):
    """Sinh restaurant_id từ toạ độ, đúng format ví dụ trong Mục 2.3:
    REST_10p77_106p70  (lat/lon làm tròn 2 chữ số, dấu '.' -> 'p')"""
    lat_s = f"{lat:.2f}".replace(".", "p").replace("-", "m")
    lon_s = f"{lon:.2f}".replace(".", "p").replace("-", "m")
    return f"REST_{lat_s}_{lon_s}"


def random_city():
    return random.choice(CITIES)


def random_prep_minutes():
    return random.randint(*PREP_TIME_RANGE_MIN)


def random_order_amount():
    return round(random.uniform(*ORDER_AMOUNT_RANGE_VND), 0)


def simulate_delivery_minutes(distance_km, noise_ratio=0.25):
    """Thời gian giao (phút) tính dựa trên khoảng cách + nhiễu ngẫu nhiên,
    để dữ liệu 'có ý nghĩa' thay vì random hoàn toàn (đúng note Mục 2.2).
    Giả định tốc độ trung bình ~18km/h trong nội thành + thời gian tìm đường/gửi xe cố định.
    """
    avg_speed_kmh = 18.0
    base_minutes = (distance_km / avg_speed_kmh) * 60
    base_minutes += 5  # thời gian gửi xe, tìm địa chỉ...
    noise = random.uniform(-noise_ratio, noise_ratio) * base_minutes
    return max(5.0, base_minutes + noise)  # tối thiểu 5 phút


def random_order_date_last_30_days():
    days_ago = random.randint(0, 29)
    return (datetime.now() - timedelta(days=days_ago)).date()


def random_time_of_day():
    return random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)
