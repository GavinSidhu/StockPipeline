import yfinance as yf
import pandas as pd
from dagster import asset

@asset
def stock_data(ticker_symbols: list[str] = ["VTI","VXUS"]):
    data_frames = []

    for ticker in ticker_symbols:
        ticker_data = yf.Ticker(ticker)
        hist = ticker_data.history(period="6mo")
        hist["ticker"] = ticker
        data_frames.append(hist)

    combined_data = pd.concat(data_frames)
    combined_data.reset_index(inplace=True)

    return combined_data    