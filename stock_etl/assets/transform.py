from dagster import asset

@asset
def transformed_stock_data(stock_data):
    """Transform stock data without dbt for now."""
    # Simply return the data with a note that it's been "transformed"
    if stock_data is not None and not stock_data.empty:
        stock_data['processed'] = True
        return stock_data
    return stock_data