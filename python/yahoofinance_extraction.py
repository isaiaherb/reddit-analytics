import yfinance as yf
import pandas as pd
import urllib
from sqlalchemy import create_engine
import datetime

def fetch_stock_data(ticker_symbols, start_date, end_date):
    data_frames = []
    for ticker in ticker_symbols:
        stock_data = yf.download(ticker, start=start_date, end=end_date)
        stock_data['Ticker'] = ticker  
        data_frames.append(stock_data)

    return pd.concat(data_frames)

def transform_data(ticker_symbols, stock_data):
    price_data = stock_data.pivot_table(index='Date', columns='Ticker', values='Close').rename_axis(index='timestamp')
    volume_data = stock_data.pivot_table(index='Date', columns='Ticker', values='Volume').rename_axis(index='timestamp')

    price_data.columns = [f'{ticker}_closing_price' for ticker in ticker_symbols]
    volume_data.columns = [f'{ticker}_trading_volume' for ticker in ticker_symbols]

    return price_data, volume_data

def load_data_to_sql(price_data, volume_data):
    engine = create_engine(get_connection_string())

    stock_price_table = 'stock_price_table'
    stock_volume_table = 'stock_volume_table'

    price_data.to_sql(stock_price_table, con=engine, if_exists='replace', index=True)
    volume_data.to_sql(stock_volume_table, con=engine, if_exists='replace', index=True)

def get_connection_string():
    server = 'TABLET-JQOOCB8O\MYSQLSERVER'
    database = 'reddit'
    
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes")
    
    return f"mssql+pyodbc:///?odbc_connect={params}"

def main(ticker_symbols):
    start_date = "2017-01-01"
    end_date = datetime.date.today()

    stock_data = fetch_stock_data(ticker_symbols, start_date, end_date)
    price_data, volume_data = transform_data(ticker_symbols, stock_data)

    load_data_to_sql(price_data, volume_data)
    
if __name__ == "__main__":
    main(['SCHW', '^GSPC', '^DJI', '^IXIC', 'HOOD', 'MS', 'JPM'])


