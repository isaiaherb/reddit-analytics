# Python Reddit Data Extraction and Sentiment Analysis (Streamlit Visualization)
Note: This is a work in progress, data visualization is not final and can easily be modified. This repository also contains raw Python and SQL scripts that extract, transform, and load cleaned data from the Reddit API (praw) into SQL Server. The sentiment analysis is done using the VADER and FinBERT models and also includes a breakdown of the emotions contained in the comments. Before running, you need to have a Reddit account and create a Reddit app in order to get your credentials. 

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
You will need a Reddit account and a Reddit app to access praw. You will also need to create environment variables on your local machine to store your Reddit credentials. Navigate to the link above once the app has been created and click 'edit' in the bottom left corner of the application window, there you'll find the necessary credentials to use praw.
* CLIENT_ID: the id of your Reddit application, found beneath its name in the editing tab
* CLIENT_SECRET: the secret key that can be found next to the word 'secret' in the editing tab
* USER_AGENT: the name of your Reddit application, found in bold at the top of the editing tab

## SQL Server
Locate the 'sql' folder in the repository and run the create_database.sql and create_database_tables.sql scripts in SQL Server. This will create the initial database with four tables: subreddit, submission, comment, and author. This method involves extracting data from praw and putting it into temporary tables, which are then merged into permanent tables after being preprocessed. The SQL scripts I used for cleaning and adding relationships between the tables are located within the airflow-docker directory in the dags folder. If you choose to run these in Airflow, they are incorporated into the DAG workflow and will automatically run when the data is first extracted.

## Docker and Airflow
As I mentioned previously, instructions on how to run the required Docker images and access the Airflow web interface are on DockerHub: https://hub.docker.com/repository/docker/isaiaherb9264/reddit-charles-schwab/general.

