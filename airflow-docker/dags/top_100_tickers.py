# Extract the top 100 stock ticker symbols from existing records in the SQL Server database.
import pandas as pd
import spacy
import urllib
from collections import Counter
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine, text

def get_connection_string():
    server = 'TABLET-JQOOCB8O\MYSQLSERVER'  # replace with the name of your server
    database = 'reddit'  # or the name of your database
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes")
    return f"mssql+pyodbc:///?odbc_connect={params}"

def extract_stock_tickers(text):
    tickers = []
    for token in nlp(text):
        if token.text.isupper() and len(token.text) <= 5:
            tickers.append(token.text)
    return tickers

def get_comments_and_submissions(connection_string):
    engine = sqlalchemy.create_engine(connection_string)
    query = """
    SELECT 
        body,
        compound_finbert_sentiment,
        compound_vader_sentiment,
        upvotes
    FROM comment_table
    WHERE timestamp >= DATEADD(year, -1, GETDATE())
    UNION ALL
    SELECT 
        body, 
        compound_finbert_sentiment,
        (title_compound_vader_sentiment + body_compound_vader_sentiment) / 2 AS compound_vader_sentiment,
        upvotes
    FROM submission_table
    WHERE timestamp >= DATEADD(year, -1, GETDATE())
    """
    df = pd.read_sql(query, engine)
    return df

nlp = spacy.load('en_core_web_sm')

def main():
    connection_string = get_connection_string()
    df = get_comments_and_submissions(connection_string)
    
    all_tickers = []
    for index, row in df.iterrows():
        tickers = extract_stock_tickers(row['body'])
        for ticker in tickers:
            all_tickers.append((ticker, row['upvotes'], row['compound_finbert_sentiment'], row['compound_vader_sentiment']))
    
    ticker_counts = Counter([ticker[0] for ticker in all_tickers])
    top_100_tickers = ticker_counts.most_common(100)
    
    tickers_df = pd.DataFrame(all_tickers, columns=['ticker', 'upvotes', 'compound_finbert_sentiment', 'compound_vader_sentiment'])

    top_100_ticker_symbols = [ticker[0] for ticker in top_100_tickers]
    top_100_tickers_df = tickers_df[tickers_df['ticker'].isin(top_100_ticker_symbols)]

    top_100_agg = top_100_tickers_df.groupby('ticker').agg({
        'upvotes': 'sum',
        'compound_finbert_sentiment': ['sum', lambda x: (x > 0).sum(), lambda x: (x < 0).sum()],
        'compound_vader_sentiment': ['sum', lambda x: (x > 0).sum(), lambda x: (x < 0).sum()]
    }).reset_index()

    # Flatten multi-index columns
    top_100_agg.columns = ['ticker', 'total_upvotes', 'total_finbert_sentiment', 'pos_finbert_sentiment', 'neg_finbert_sentiment', 'total_vader_sentiment', 'pos_vader_sentiment', 'neg_vader_sentiment']

    engine = create_engine(connection_string)
    with engine.connect() as connection:
        connection.execute(text("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='top_stocks' and xtype='U')
        CREATE TABLE top_stocks (
            ticker VARCHAR(10),
            mentions INT,
            total_upvotes INT,
            total_finbert_sentiment FLOAT,
            pos_finbert_sentiment FLOAT,
            neg_finbert_sentiment FLOAT,
            total_vader_sentiment FLOAT,
            pos_vader_sentiment FLOAT,
            neg_vader_sentiment FLOAT,
        )
        """))
        
        connection.execute(text("TRUNCATE TABLE top_stocks"))
        
        for _, row in top_100_agg.iterrows():
            connection.execute(text("""
            INSERT INTO top_stocks (
                ticker, mentions, total_upvotes, total_finbert_sentiment, 
                pos_finbert_sentiment, neg_finbert_sentiment, 
                total_vader_sentiment, pos_vader_sentiment, neg_vader_sentiment
            ) VALUES (
                :ticker, :mentions, :total_upvotes, :total_finbert_sentiment, 
                :pos_finbert_sentiment, :neg_finbert_sentiment, 
                :total_vader_sentiment, :pos_vader_sentiment, :neg_vader_sentiment
            )
            """), {
                'ticker': row['ticker'],
                'mentions': ticker_counts[row['ticker']],
                'total_upvotes': row['total_upvotes'],
                'total_finbert_sentiment': row['total_finbert_sentiment'],
                'pos_finbert_sentiment': row['pos_finbert_sentiment'],
                'neg_finbert_sentiment': row['neg_finbert_sentiment'],
                'total_vader_sentiment': row['total_vader_sentiment'],
                'pos_vader_sentiment': row['pos_vader_sentiment'],
                'neg_vader_sentiment': row['neg_vader_sentiment']
            })

if __name__ == "__main__":
    main()
