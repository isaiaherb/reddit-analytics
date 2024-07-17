# Python Reddit Data Extraction and Sentiment Analysis (Streamlit Visualization) 
Contains 2 Python ETLs, one for simpler streamlit visualization, and one for storage in SQL Server. NLP techniques include topic modeling (LDA), sentiment analysis, and emotion analysis. Before running, you need to have a Reddit account and create a Reddit app to get your credentials. 
## Technology Stack
![image](https://github.com/isaiaherb/reddit-analytics/blob/main/images/Screenshot%202024-06-14%20223630.png)

## SQL Server Database ERD
![image](https://github.com/isaiaherb/reddit-analytics/blob/main/images/Screenshot%202024-06-05%20170949.png?raw=true)

## Power BI Screenshots
![image](https://github.com/isaiaherb/reddit-analytics/blob/main/images/Screenshot%202024-06-05%20104407.png?raw=true)
![image](https://github.com/isaiaherb/reddit-analytics/blob/main/images/Screenshot%202024-06-05%20105555.png?raw=true)
![image](https://github.com/isaiaherb/reddit-analytics/blob/main/images/Screenshot%202024-06-05%20111833.png?raw=true)
![image](https://github.com/isaiaherb/reddit-analytics/blob/main/images/Screenshot%202024-06-05%20112923.png?raw=true)

## UPDATE: Streamlit Dashboard
![Positive and Negative Sentiment Example](https://github.com/isaiaherb/Reddit-Sentiment-Analyzer/issues/1#issue-2347298959)
![Emotion Scores Example]([https://github.com/isaiaherb/Reddit-Sentiment-Analyzer/issues/1#issue-2347298959](https://github.com/isaiaherb/Reddit-Sentiment-Analyzer/issues/2#issue-2347300183))
![LDA Topic Modeling]([https://github.com/isaiaherb/Reddit-Sentiment-Analyzer/issues/1#issue-2347298959](https://github.com/isaiaherb/Reddit-Sentiment-Analyzer/issues/3#issue-2347301908))

## Dependencies
* pandas==2.2.2 
* nltk==3.8.1 
* praw==7.7.1
* pyLDAvis
* streamlit
* plotly.express
* plotly.graph_objs
* nrclex
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

