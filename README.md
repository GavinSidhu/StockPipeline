# Stock ETL Pipeline

An ETL pipeline to extract stock data from YFinance for VTI and VXUS, transform it using dbt. The initial implementation used the 37% rule with an exploration period of 8 days and decision period of 14 days. 
With this I aimed to identify potential buy/sell opportunities for the above mentioned tickers, which I'm using to adhere to the Boglehead mentality. The current implementation instead tries to test varying
window sizes to check data and base recommendations off of.

## Features

- Daily automated data extraction via YFinance
- Weekly recalculation of window size compared against traditional buy-and-hold strategies
- Incremental historical data loading (fetches only new data each run)
- dbt transformations for data cleaning and analysis
- 37% rule implementation to identify optimal trading points
- Containerized with Docker for easy deployment

## Requirements

- Docker
- Docker Compose

## Quick Start

1. Clone this repository
2. Run `docker-compose up`
3. Access the Dagster UI at http://localhost:3000
4. Trigger the jobs manually or wait for the schedules (Daily at 9:00 AM/ Sundays at 10:00 AM)

## Project Structure

- `stock_etl/assets`: Dagster assets for data extraction
- `stock_etl/resources`: Dagster resources (database, dbt)
- `stock_etl/dbt_project`: dbt models and transformations
- `scripts`: Database initialization scripts
- `dagster_home`: Dagster configuration

## Customization

- Add more tickers in `stock_etl/assets/extract.py`
- Modify dbt transformation models in `stock_etl/dbt_project/finance_dbt/models/`
- Adjust pipeline schedule in `stock_etl/definitions.py`

## Configuration

1. Copy `.env.example` to `.env`
2. Update the `.env` file with your Discord webhook URL
3. Run the Docker containers with `docker compose up`
