# Stock ETL Pipeline

An ETL pipeline to extract stock data from YFinance for VTI and VXUS, transform it using a hybrid dbt and Python approach, and analyze it using dynamic window sizes and thresholds to identify potential buy/sell opportunities.

## Features

- Weekly automated data extraction via YFinance
- Incremental historical data loading (fetches only new data each run)
- Hybrid dbt/Python transformations for data cleaning and analysis
- Dynamic window sizing from 14 days to 6 months
- Variable buy/sell thresholds based on optimal backtesting results
- Monthly parameter optimization
- Containerized with Docker for easy deployment

## Requirements

- Docker
- Docker Compose

## Quick Start

1. Clone this repository
2. Run `docker-compose up`
3. Access the Dagster UI at http://localhost:3000
4. Trigger the "daily_stock_etl" job manually or wait for the daily schedule

## Project Structure

- `stock_etl/assets`: Dagster assets for data extraction, transformation, and analytics
- `stock_etl/resources`: Dagster resources (database, window configuration, threshold configuration)
- `stock_etl/dbt_project`: dbt models and transformations
- `scripts`: Database initialization scripts
- `dagster_home`: Dagster configuration

## Jobs and Schedules

- **daily_stock_etl**: Runs daily at 9 AM (Monday-Saturday) to fetch new data and generate recommendations
- **weekly_backtest**: Runs weekly on Sunday at 10 AM to evaluate different window sizes and thresholds
- **monthly_parameter_update**: Runs monthly on the 1st to update optimal parameters

## Customization

- Add more tickers in `stock_etl/assets/extract.py`
- Modify dbt transformation models in `stock_etl/dbt_project/finance_dbt/models/`
- Adjust window size range in `stock_etl/assets/window_backtest.py`
- Adjust threshold ranges in `stock_etl/assets/window_backtest.py`
- Change backtest parameters in `stock_etl/resources/window_size_config.py` and `threshold_config.py`

## Configuration

1. Copy `.env.example` to `.env`
2. Update the `.env` file with your Discord webhook URL
3. Run the Docker containers with `docker compose up`

## Window Sizes and Thresholds

The system tests multiple window sizes (from 14 days to 6 months) and buy/sell thresholds to determine the optimal parameters for each ticker. These parameters are automatically updated on a monthly basis or when significant performance improvements are detected through backtesting.

- **Window Size**: The lookback period used to determine local minimums and maximums
- **Buy Threshold**: The percentage above the minimum price at which to issue buy signals
- **Sell Threshold**: The percentage below the maximum price at which to issue sell signals