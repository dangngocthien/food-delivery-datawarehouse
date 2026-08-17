DROP TABLE IF EXISTS Fact_Order CASCADE;
DROP TABLE IF EXISTS Dim_Restaurant CASCADE;
DROP TABLE IF EXISTS Dim_Driver CASCADE;
DROP TABLE IF EXISTS Dim_Zone CASCADE;
DROP TABLE IF EXISTS Dim_Date CASCADE;

CREATE TABLE Dim_Restaurant (
    restaurant_key SERIAL PRIMARY KEY,
    restaurant_id VARCHAR(50) UNIQUE NOT NULL,
    restaurant_lat FLOAT NOT NULL,
    restaurant_lon FLOAT NOT NULL,
    city VARCHAR(50)
);

CREATE TABLE Dim_Driver (
    driver_key SERIAL PRIMARY KEY,
    driver_id VARCHAR(50) UNIQUE NOT NULL,
    driver_age INT,
    driver_rating FLOAT,
    vehicle_type VARCHAR(50)
);

CREATE TABLE Dim_Zone (
    zone_key SERIAL PRIMARY KEY,
    city VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE Dim_Date (
    date_key SERIAL PRIMARY KEY,
    order_date DATE NOT NULL,
    hour INT,
    day_of_week VARCHAR(10)
);

CREATE TABLE Fact_Order (
    order_id VARCHAR(50) PRIMARY KEY,
    restaurant_key INT REFERENCES Dim_Restaurant(restaurant_key),
    driver_key INT REFERENCES Dim_Driver(driver_key),
    zone_key INT REFERENCES Dim_Zone(zone_key),
    date_key INT REFERENCES Dim_Date(date_key),
    distance_km FLOAT,
    prep_time_min FLOAT,
    delivery_time_min FLOAT,
    order_amount FLOAT,
    is_late BOOLEAN
);
