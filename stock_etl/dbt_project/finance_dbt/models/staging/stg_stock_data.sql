{{ config(materialized='view') }}

SELECT 
  ticker,
  date,
  open,
  high,
  low,
  close,
  volume,
  dividends,
  stock_splits
FROM stock.stock_data_raw