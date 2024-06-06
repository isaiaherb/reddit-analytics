# Financial Reddit Data Extraction and Sentiment Analysis 

Sentiment analysis is done using the VADER and FinBERT models.

This repository contains raw Python and SQL scripts that extract, transform, and load cleaned data from praw into SQL Server. 

This repository also includes an example Apache Airflow DAG to manage workflows. 

Before running any of the Python extraction scripts, you must first create a Reddit account and then a Reddit app to get your credentials, which should be assigned as environment variables.

You also need to create a SQL Server database to store data extracted from the praw API.

Visit **https://hub.docker.com/repository/docker/isaiaherb9264/reddit-charles-schwab** to download Docker images.

Praw documentation here: https://praw.readthedocs.io/en/latest/

Docker documentation: https://docs.docker.com/

Apache Airflow documentation: https://airflow.apache.org/docs/

## Dependencies
* pandas==2.2.2 
* nltk==3.8.1 
* praw==7.7.1 
* sqlalchemy==1.4.52 
* pyodbc==5.1.0 
* apache-airflow==2.9.1 
* apache-airflow-providers-microsoft-mssql==3.7.0 
* yfinance==0.2.38 
* transformers==4.41.1 
* torch==2.3.0
  
## Reddit App and Environment Variables

## SQL Server
Create a 
## Docker and Airflow


