# Compute FinBERT sentiment scores for the comments and submissions in SQL Server.
import pandas as pd
from sqlalchemy import create_engine, text
import urllib
from transformers import BertTokenizer, BertForSequenceClassification
import torch

tokenizer = BertTokenizer.from_pretrained('yiyanghkust/finbert-tone', do_lower_case=True)
model = BertForSequenceClassification.from_pretrained('yiyanghkust/finbert-tone')

def get_connection_string():
    server = 'TABLET-JQOOCB8O\MYSQLSERVER'  # Replace with the name of your server
    database = 'reddit'  # Replace with the name of your database
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes")
    return f"mssql+pyodbc:///?odbc_connect={params}"

def get_finbert_scores(text):
    inputs = tokenizer(text, return_tensors='pt', max_length=512, truncation=True, padding=True)
    outputs = model(**inputs)
    probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
    positive_score = probs[0][0].item()
    negative_score = probs[0][1].item()
    neutral_score = probs[0][2].item()
    compound_score = positive_score - negative_score
    return positive_score, negative_score, neutral_score, compound_score

def add_sentiment_scores_to_dataframe(df, text_column):
    scores = df[text_column].apply(get_finbert_scores)
    df['pos_finbert_sentiment'] = scores.apply(lambda x: x[0])
    df['neg_finbert_sentiment'] = scores.apply(lambda x: x[1])
    df['neu_finbert_sentiment'] = scores.apply(lambda x: x[2])
    df['compound_finbert_sentiment'] = scores.apply(lambda x: x[3])
    return df

def update_database():
    engine = create_engine(get_connection_string())
    
    with engine.connect() as conn:
        conn.execute("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'submission_table' AND COLUMN_NAME = 'text'
        ) ALTER TABLE submission_table ADD text VARCHAR(MAX);

        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'submission_table' AND COLUMN_NAME = 'pos_finbert_sentiment'
        ) ALTER TABLE submission_table ADD pos_finbert_sentiment FLOAT;

        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'submission_table' AND COLUMN_NAME = 'neg_finbert_sentiment'
        ) ALTER TABLE submission_table ADD neg_finbert_sentiment FLOAT;

        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'submission_table' AND COLUMN_NAME = 'neu_finbert_sentiment'
        ) ALTER TABLE submission_table ADD neu_finbert_sentiment FLOAT;

        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'submission_table' AND COLUMN_NAME = 'compound_finbert_sentiment'
        ) ALTER TABLE submission_table ADD compound_finbert_sentiment FLOAT;

        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'comment_table' AND COLUMN_NAME = 'pos_finbert_sentiment'
        ) ALTER TABLE comment_table ADD pos_finbert_sentiment FLOAT;

        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'comment_table' AND COLUMN_NAME = 'neg_finbert_sentiment'
        ) ALTER TABLE comment_table ADD neg_finbert_sentiment FLOAT;

        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'comment_table' AND COLUMN_NAME = 'neu_finbert_sentiment'
        ) ALTER TABLE comment_table ADD neu_finbert_sentiment FLOAT;

        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'comment_table' AND COLUMN_NAME = 'compound_finbert_sentiment'
        ) ALTER TABLE comment_table ADD compound_finbert_sentiment FLOAT;
        """)

        conn.execute("""
        UPDATE submission_table
        SET text = CONCAT(title, ' ', body)
        WHERE text IS NULL OR text = '';
        """)

        submission_df = pd.read_sql('SELECT * FROM submission_table', conn)
        comment_df = pd.read_sql('SELECT * FROM comment_table', conn)

        submission_df = add_sentiment_scores_to_dataframe(submission_df, 'text') 
        comment_df = add_sentiment_scores_to_dataframe(comment_df, 'body')

        for index, row in submission_df.iterrows():
            conn.execute(text("""
                UPDATE submission_table
                SET pos_finbert_sentiment = :positive_score,
                    neg_finbert_sentiment = :negative_score,
                    neu_finbert_sentiment = :neutral_score,
                    compound_finbert_sentiment = :compound_score
                WHERE submission_id = :id
            """), {'positive_score': row['pos_finbert_sentiment'], 
                   'negative_score': row['neg_finbert_sentiment'], 
                   'neutral_score': row['neu_finbert_sentiment'], 
                   'compound_score': row['compound_finbert_sentiment'], 
                   'id': row['submission_id']})

        for index, row in comment_df.iterrows():
            conn.execute(text("""
                UPDATE comment_table
                SET pos_finbert_sentiment = :positive_score,
                    neg_finbert_sentiment = :negative_score,
                    neu_finbert_sentiment = :neutral_score,
                    compound_finbert_sentiment = :compound_score
                WHERE comment_id = :id
            """), {'positive_score': row['pos_finbert_sentiment'], 
                   'negative_score': row['neg_finbert_sentiment'], 
                   'neutral_score': row['neu_finbert_sentiment'], 
                   'compound_score': row['compound_finbert_sentiment'], 
                   'id': row['comment_id']})

if __name__ == "__main__":
    update_database()
