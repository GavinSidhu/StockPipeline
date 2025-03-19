{{ config(materialized='table') }}

WITH stock_data AS (
  SELECT 
    ticker,
    date,
    open,
    high,
    low,
    close,
    volume,
    (close - open) / open * 100 as daily_change_pct,
    AVG(close) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_7d_avg
  FROM {{ ref('stg_stock_data') }}
),

-- Get window sizes from configuration table
window_configs AS (
  SELECT 
    ticker,
    window_size
  FROM stock.window_config
),

-- Get threshold configurations from table
threshold_configs AS (
  SELECT 
    ticker,
    buy_threshold,
    sell_threshold
  FROM stock.threshold_config
),

-- Join stock data with configurations
stock_with_configs AS (
  SELECT 
    s.*,
    COALESCE(w.window_size, 30) as window_size,
    COALESCE(t.buy_threshold, 5.0) as buy_threshold,
    COALESCE(t.sell_threshold, 5.0) as sell_threshold
  FROM stock_data s
  LEFT JOIN window_configs w ON s.ticker = w.ticker
  LEFT JOIN threshold_configs t ON s.ticker = t.ticker
),

-- Create dynamic window for each ticker
windowed_analysis AS (
  SELECT
    s.*,
    -- Get max date in the dataset
    MAX(date) OVER () as latest_date,
    -- Calculate days from latest date
    (latest_date - date) as days_from_latest,
    -- Flag for data within window
    CASE WHEN (latest_date - date) <= s.window_size THEN TRUE ELSE FALSE END as is_in_window
  FROM stock_with_configs s
),

-- Apply window analysis
window_metrics AS (
  SELECT
    ticker,
    date,
    close,
    daily_change_pct,
    rolling_7d_avg,
    days_from_latest,
    window_size,
    buy_threshold,
    sell_threshold,
    -- Track maximum and minimum during exploration period
    MIN(CASE WHEN is_in_window THEN close END) OVER (PARTITION BY ticker) as min_exploration,
    MAX(CASE WHEN is_in_window THEN close END) OVER (PARTITION BY ticker) as max_exploration
  FROM windowed_analysis
),

-- Calculate signals and thresholds
signals AS (
  SELECT
    ticker,
    date,
    close,
    daily_change_pct,
    rolling_7d_avg,
    days_from_latest,
    window_size,
    buy_threshold,
    sell_threshold,
    min_exploration,
    max_exploration,
    -- Calculate price relative to min/max
    ((close / min_exploration) - 1) * 100 as price_vs_min,
    ((close / max_exploration) - 1) * 100 as price_vs_max,
    -- Calculate buy thresholds
    min_exploration * (1 - (buy_threshold * 0.5) / 100) as buy_strong,
    min_exploration as buy_medium,
    min_exploration * (1 + buy_threshold / 100) as buy_weak,
    -- Calculate sell thresholds
    max_exploration * (1 + (sell_threshold * 0.5) / 100) as sell_strong,
    max_exploration as sell_medium,
    max_exploration * (1 - sell_threshold / 100) as sell_weak
  FROM window_metrics
),

-- Generate recommendations
recommendations AS (
  SELECT
    ticker,
    date,
    close,
    daily_change_pct,
    rolling_7d_avg,
    min_exploration,
    max_exploration,
    window_size,
    buy_threshold,
    sell_threshold,
    price_vs_min,
    price_vs_max,
    buy_strong,
    buy_medium,
    buy_weak,
    sell_strong,
    sell_medium,
    sell_weak,
    -- Generate buy/sell signals
    CASE 
      WHEN close < buy_strong THEN 'STRONG_BUY'
      WHEN close <= buy_medium THEN 'MEDIUM_BUY'
      WHEN close <= buy_weak THEN 'WEAK_BUY'
      ELSE NULL
    END as buy_signal,
    CASE 
      WHEN close > sell_strong THEN 'STRONG_SELL'
      WHEN close >= sell_medium THEN 'MEDIUM_SELL'
      WHEN close >= sell_weak THEN 'WEAK_SELL'
      ELSE NULL
    END as sell_signal,
    -- Generate consolidated recommendation
    CASE 
      WHEN close < buy_strong THEN 'STRONG BUY ' || ticker || ' (>' || (buy_threshold * 0.5) || '% below exploration min)'
      WHEN close <= buy_medium THEN 'MEDIUM BUY ' || ticker || ' (at exploration min)'
      WHEN close <= buy_weak THEN 'WEAK BUY ' || ticker || ' (within ' || buy_threshold || '% above exploration min)'
      WHEN close > sell_strong THEN 'STRONG SELL ' || ticker || ' (>' || (sell_threshold * 0.5) || '% above exploration max)'
      WHEN close >= sell_medium THEN 'MEDIUM SELL ' || ticker || ' (at exploration max)'
      WHEN close >= sell_weak THEN 'WEAK SELL ' || ticker || ' (within ' || sell_threshold || '% below exploration max)'
      WHEN ABS(price_vs_min) < buy_threshold * 0.25 THEN 'WATCH ' || ticker || ' - APPROACHING BUY POINT'
      WHEN ABS(price_vs_max) < sell_threshold * 0.25 THEN 'WATCH ' || ticker || ' - APPROACHING SELL POINT'
      ELSE 'HOLD ' || ticker
    END as recommendation
  FROM signals
)

SELECT
  ticker,
  date,
  close,
  daily_change_pct,
  rolling_7d_avg,
  window_size,
  buy_threshold,
  sell_threshold,
  min_exploration,
  max_exploration,
  price_vs_min,
  price_vs_max,
  buy_strong,
  buy_medium,
  buy_weak,
  sell_strong,
  sell_medium,
  sell_weak,
  buy_signal,
  sell_signal,
  recommendation,
  -- Add analysis notes
  'Analysis based on ' || window_size || '-day window. Buy threshold: ' || buy_threshold || '%, Sell threshold: ' || sell_threshold || '%. Current price relative to min: ' || ROUND(price_vs_min, 2) || '%, to max: ' || ROUND(price_vs_max, 2) || '%.' as notes,
  -- Add metadata
  CURRENT_DATE as analysis_date,
  CURRENT_TIMESTAMP as last_updated
FROM recommendations
WHERE date = (SELECT MAX(date) FROM stock_data)  -- Only include latest date
ORDER BY ticker, date DESC