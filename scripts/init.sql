CREATE SCHEMA IF NOT EXISTS stock;

CREATE TABLE IF NOT EXISTS stock.stock_data_raw (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    dividends NUMERIC,
    stock_splits NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);