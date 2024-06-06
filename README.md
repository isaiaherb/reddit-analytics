# Praw Extraction and Sentiment Analysis
Reddit extraction and sentiment analysis. Requires SQL Server 2020 and Python 3.13.1. \
This repository includes the raw Python and SQL files needed to extract, transform, and load cleaned data into SQL Server. It also includes an Apache Airflow DAG to manage the workflow.
Visit **https://hub.docker.com/repository/docker/isaiaherb9264/reddit-charles-schwab** to pull the required Docker images.  

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

## Running Airflow in Docker
Dockerfile, docker-compose.yml
DockerHub
DAG

## Data Visualization
main_query.sql
- company, called 'brand'
- market sector, called 'topic'
