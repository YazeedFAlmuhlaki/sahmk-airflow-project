CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    calendar_date DATE UNIQUE NOT NULL,
    day_of_week INT,
    month INT,
    year INT,
    is_weekend BOOLEAN,
    is_trading_day BOOLEAN
);

INSERT INTO dim_date (
    date_key,
    calendar_date,
    day_of_week,
    month,
    year,
    is_weekend,
    is_trading_day
)
SELECT
    TO_CHAR(datum, 'YYYYMMDD')::INT,
    datum::DATE,
    EXTRACT(ISODOW FROM datum)::INT,
    EXTRACT(MONTH FROM datum)::INT,
    EXTRACT(YEAR FROM datum)::INT,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (5, 6) THEN TRUE ELSE FALSE END,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (5, 6) THEN FALSE ELSE TRUE END
FROM (
    SELECT generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::interval) AS datum
) d
ON CONFLICT (date_key) DO NOTHING;