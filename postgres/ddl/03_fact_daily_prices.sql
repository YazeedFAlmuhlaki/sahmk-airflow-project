CREATE TABLE IF NOT EXISTS fact_daily_prices(
    date_key INT NOT NULL REFERENCES dim_date (date_key),
    symbol VARCHAR(50) NOT NULL REFERENCES dim_company (symbol), 
    open_price NUMERIC(10, 4) NOT NULL, 
    high_price NUMERIC(10, 4) NOT NULL, 
    low_price  NUMERIC(10, 4) NOT NULL,
    close_price NUMERIC(10, 4) NOT NULL, 
    volume BIGINT NOT NULL, 
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    PRIMARY KEY (date_key, symbol)
);