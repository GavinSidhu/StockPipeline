from dagster import asset, get_dagster_logger
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
import datetime

@asset(
    required_resource_keys={"database_config"}  # Add this line
)
def stock_data(context):
    """Extract stock data from YFinance."""
    logger = get_dagster_logger()
    
    # Define ticker symbols directly in the function instead of as a parameter
    ticker_symbols = ["VTI", "VXUS","FBTC"]
    
    # Get database config from resources
    db_config = context.resources.database_config
    
    # Connect to database
    engine = create_engine(
        f"postgresql://{db_config.username}:{db_config.password}@"
        f"{db_config.host}:{db_config.port}/{db_config.database}"
    )
    
    data_frames = []
    
    for ticker in ticker_symbols:
        logger.info(f"Fetching data for {ticker}")
        
        try:
            # Try to get the latest date for this ticker from the database
            with engine.connect() as conn:
                query = text(f"SELECT MAX(date) as last_date FROM stock.stock_data_raw WHERE ticker = :ticker")
                result = conn.execute(query, {"ticker": ticker}).fetchone()
                
                if result and result[0]:
                    # If we have data, get everything since the last date
                    start_date = result[0] + datetime.timedelta(days=1)
                    logger.info(f"Found existing data for {ticker}, fetching since {start_date}")
                    
                    # Only fetch if start_date is in the past
                    if start_date < datetime.datetime.now().date():
                        hist = yf.Ticker(ticker).history(start=start_date.strftime('%Y-%m-%d'))
                    else:
                        logger.info(f"No new data needed for {ticker}")
                        hist = pd.DataFrame()  # Empty DataFrame if we're already up to date
                else:
                    # If no data exists for this ticker, get data since inception
                    logger.info(f"No existing data for {ticker}, fetching since inception")
                    ticker_info = yf.Ticker(ticker)
                    
                    # Get inception date if available, otherwise use a reasonable default
                    try:
                        inception = ticker_info.info.get('fundInceptionDate')
                        if inception:
                            inception_date = datetime.datetime.fromtimestamp(inception).strftime('%Y-%m-%d')
                        else:
                            inception_date = '2000-01-01'  # Default to 2000 if not found
                    except:
                        inception_date = '2000-01-01'  # Default if there's an error
                    
                    logger.info(f"Fetching {ticker} data since {inception_date}")
                    hist = ticker_info.history(start=inception_date)
        except Exception as e:
            # If there's an error, log it and get data for the past year as fallback
            logger.error(f"Error getting last date for {ticker}: {e}")
            logger.info(f"Falling back to 1 year of data for {ticker}")
            hist = yf.Ticker(ticker).history(period="1y")
        
        if not hist.empty:
            hist["ticker"] = ticker
            data_frames.append(hist)
            logger.info(f"Got {len(hist)} records for {ticker}")
        else:
            logger.info(f"No new data for {ticker}")
    
    # If any data was returned
    if data_frames:
        combined_data = pd.concat(data_frames)
        combined_data.reset_index(inplace=True)  # Move Date from index to column
        return combined_data
    else:
        # Return empty DataFrame with correct structure if no new data
        logger.info("No new data for any tickers")
        return pd.DataFrame(columns=["ticker", "date", "open", "high", "low", "close", "volume", "dividends", "stock_splits"])