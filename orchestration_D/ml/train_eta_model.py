"""
Train model dự đoán delivery_time_min (ETA) — Đồ án 18, Phần D.

Feature dùng (4): distance_km, driver_age, driver_rating, vehicle_type.

KHÔNG dùng order_type: cột này không tồn tại trong warehouse. Theo data
contract Mục 2.4 (file phân công gốc), cả Fact_Order lẫn Dim_Driver đều
thiếu cột này — Kaggle gốc có Type_of_order nhưng nhóm không đưa vào bảng
Postgres khi thiết kế schema. Đây là giới hạn dữ liệu, ghi rõ trong
model_evaluation.md và trong báo cáo.

Nguồn dữ liệu: JOIN trực tiếp 2 bảng gốc mà C đã tạo (Fact_Order, Dim_Driver
— xem warehouse_C/create_tables.sql), qua SQLAlchemy + psycopg2. Không cần
thêm dbt model, không đụng vào phần việc của C.
"""

import numpy as np
import pandas as pd
import joblib
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

# ── Cấu hình kết nối Postgres warehouse (container network) ──
DB_USER = "warehouse"
DB_PASS = "warehouse123"
DB_HOST = "postgres-warehouse"
DB_PORT = 5432
DB_NAME = "food_delivery_dw"

# JOIN Fact_Order + Dim_Driver qua driver_key (đúng tên bảng/cột theo
# warehouse_C/create_tables.sql). Postgres tự hạ chữ thường tên bảng không
# quote nên viết thường ở đây vẫn khớp với Fact_Order/Dim_Driver.
QUERY = """
    select
        f.order_id,
        f.distance_km,
        dd.driver_age,
        dd.driver_rating,
        dd.vehicle_type,
        f.delivery_time_min,
        f.is_late
    from fact_order f
    join dim_driver dd on f.driver_key = dd.driver_key
"""

FEATURES_NUMERIC = ["distance_km", "driver_age", "driver_rating"]
FEATURES_CATEGORICAL = ["vehicle_type"]
TARGET = "delivery_time_min"


def load_data() -> pd.DataFrame:
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return pd.read_sql(QUERY, engine)


def main():
    df = load_data()
    print(f"Đọc {len(df)} dòng từ JOIN Fact_Order + Dim_Driver")

    df = df.dropna(subset=FEATURES_NUMERIC + FEATURES_CATEGORICAL + [TARGET])
    print(f"Sau khi loại NaN: {len(df)} dòng")

    # Lọc outlier tọa độ nhà hàng bị lật dấu (âm thay vì dương) — lỗi/nhiễu
    # trong dataset Kaggle gốc khiến Haversine tính ra khoảng cách ~nửa chu vi
    # Trái Đất (~19,000km) thay vì khoảng cách nội thành thật. Xác nhận: 404/45403
    # dòng (~0.9%) có distance_km > 500km, restaurant_lat/lon mang dấu âm bất
    # thường (vùng Nam Mỹ) dù dữ liệu gốc là Ấn Độ (toạ độ phải dương). Lọc ở đây
    # (tầng ML) vì đây là thực hành chuẩn (luôn loại outlier cực đoan trước khi
    # train); về lâu dài nên báo B chặn từ batch_processing.py để dashboard/dbt
    # mart cũng không bị ảnh hưởng.
    before = len(df)
    df = df[df["distance_km"] <= 20]
    print(f"Sau khi loại outlier tọa độ lỗi: {len(df)} dòng (loại {before - len(df)} dòng)")

    X = df[FEATURES_NUMERIC + FEATURES_CATEGORICAL]
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), FEATURES_CATEGORICAL),
        ],
        remainder="passthrough",
    )

    model = Pipeline(steps=[
        ("preprocess", preprocessor),
        ("regressor", RandomForestRegressor(n_estimators=200, random_state=42, n_jobs=-1)),
    ])

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    baseline_pred = np.full_like(y_test, fill_value=y_train.mean(), dtype=float)
    baseline_mae = mean_absolute_error(y_test, baseline_pred)
    baseline_rmse = np.sqrt(mean_squared_error(y_test, baseline_pred))

    print("\n=== KẾT QUẢ MODEL (RandomForestRegressor, 4 feature) ===")
    print(f"MAE  : {mae:.2f} phút")
    print(f"RMSE : {rmse:.2f} phút")
    print(f"R²   : {r2:.4f}")

    print("\n=== BASELINE (dự đoán = trung bình delivery_time_min) ===")
    print(f"MAE  : {baseline_mae:.2f} phút")
    print(f"RMSE : {baseline_rmse:.2f} phút")

    improvement = (baseline_mae - mae) / baseline_mae * 100
    print(f"\nModel tốt hơn baseline {improvement:.1f}% (theo MAE)")

    joblib.dump(model, "eta_model.joblib")
    print("\nĐã lưu model vào eta_model.joblib")


if __name__ == "__main__":
    main()