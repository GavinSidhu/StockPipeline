# Stock ETL Pipeline

An ETL pipeline to extract stock data from YFinance for VTI and VXUS, transform it using dbt, and analyze it using the 37% rule to identify potential buy/sell opportunities.

## Features

- Weekly automated data extraction via YFinance
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
4. Trigger the "weekly_stock_etl" job manually or wait for the weekly schedule (Saturdays at 5:00 AM)

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