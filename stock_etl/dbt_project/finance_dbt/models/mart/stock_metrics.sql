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

-- Create 30-day window
monthly_window AS (
  SELECT 
    s.*,
    -- Get max date in the dataset
    MAX(date) OVER () as latest_date,
    -- Calculate days from latest date
    (latest_date - date) as days_from_latest,
    -- Only include data from the last 30 days
    CASE WHEN (latest_date - date) <= 30 THEN TRUE ELSE FALSE END as is_last_30d
  FROM stock_data s
),

-- Apply 37% rule to the 30-day window
monthly_analysis AS (
  SELECT
    ticker,
    date,
    close,
    daily_change_pct,
    rolling_7d_avg,
    days_from_latest,
    -- 37% of 30 days = 11.1 days, so we'll use 11 days for exploration
    CASE WHEN days_from_latest > 19 THEN 'exploration' ELSE 'decision' END as phase,
    -- Track maximum and minimum during exploration period
    MIN(CASE WHEN days_from_latest > 19 THEN close END) OVER (PARTITION BY ticker) as min_exploration,
    MAX(CASE WHEN days_from_latest > 19 THEN close END) OVER (PARTITION BY ticker) as max_exploration
  FROM monthly_window
  WHERE is_last_30d = TRUE
),

-- Generate signals based on the 37% rule
signals AS (
  SELECT
    ticker,
    date,
    close,
    daily_change_pct,
    rolling_7d_avg,
    days_from_latest,
    phase,
    min_exploration,
    max_exploration,
    -- Buy signal: During decision phase, current price is lower than min observed in exploration period
    CASE 
      WHEN phase = 'decision' AND close < min_exploration THEN 'BUY'
      ELSE NULL
    END as buy_signal,
    -- Sell signal: During decision phase, current price is higher than max observed in exploration period
    CASE 
      WHEN phase = 'decision' AND close > max_exploration THEN 'SELL'
      ELSE NULL
    END as sell_signal
  FROM monthly_analysis
)

SELECT 
  ticker,
  date,
  close,
  daily_change_pct,
  rolling_7d_avg,
  buy_signal,
  sell_signal,
  -- Generate recommendations
  CASE 
    WHEN buy_signal = 'BUY' THEN 'CONSIDER BUYING ' || ticker
    WHEN sell_signal = 'SELL' THEN 'CONSIDER SELLING ' || ticker
    WHEN phase = 'decision' THEN 'HOLD ' || ticker
    ELSE NULL
  END as recommendation,
  -- Include additional context
  phase,
  min_exploration,
  max_exploration
FROM signals
ORDER BY ticker, date DESC