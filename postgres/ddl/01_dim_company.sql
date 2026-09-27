CREATE TABLE IF NOT EXISTS dim_company (
    symbol VARCHAR(50) PRIMARY KEY,
    name_ar VARCHAR(255),
    name_en VARCHAR(255),
    market VARCHAR(50),
    market_segment VARCHAR(50),
    security_type VARCHAR(50),
    status VARCHAR(50),
    is_etf BOOLEAN,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);