import os
import streamlit as st
import praw
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from gensim import corpora
from gensim.models import LdaModel
import pyLDAvis
import pyLDAvis.gensim_models as gensimvis
from collections import Counter
import pandas as pd
from nrclex import NRCLex
import numpy as np
from datetime import datetime
from transformers import pipeline, BartTokenizer
import plotly.express as px
import plotly.graph_objs as go

# Download NLTK data if not already downloaded
nltk.download('vader_lexicon')
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

# Initialize models
summarizer = pipeline("summarization", model="facebook/bart-large-cnn", framework="tf")
tokenizer = BartTokenizer.from_pretrained("facebook/bart-large-cnn")
analyzer = SentimentIntensityAnalyzer()

# Initialize Reddit API client
reddit = praw.Reddit(
    client_id='SZhCDJt_FNNZET5JTvgUrw',
    client_secret='TTNbsB6xGkSlx3MH30I8U4u9MI4kMw',
    user_agent='streamlit'
)

@st.cache_data(show_spinner=False)
def analyze_sentiment(text):
    sentiment = analyzer.polarity_scores(text)
    return sentiment

@st.cache_data(show_spinner=False)
def analyze_emotion_nrc(text):
    emotion = NRCLex(text).affect_frequencies
    return emotion

@st.cache_data(show_spinner=False)
def fetch_comments(subreddit_name, limit=100):
    subreddit = reddit.subreddit(subreddit_name)
    comments = []
    try:
        for comment in subreddit.comments(limit=limit):
            sentiment_scores = analyze_sentiment(comment.body)
            emotion_scores = analyze_emotion_nrc(comment.body)
            comment_json = {
                "id": comment.id,
                "author": comment.author.name if comment.author else "Deleted",
                "body": comment.body,
                "subreddit": comment.subreddit.display_name.lower(),
                "upvotes": comment.ups,
                "downvotes": comment.downs,
                "over_18": comment.over_18,
                "timestamp": datetime.utcfromtimestamp(comment.created_utc),
                "permalink": comment.permalink,
                "sentiment_score": sentiment_scores['compound'],
                "positive": sentiment_scores['pos'],
                "negative": sentiment_scores['neg'],
                **emotion_scores
            }
            comments.append(comment_json)
    except Exception as e:
        st.error(f"An error occurred while fetching comments: {e}")
    return comments

# @st.cache_data(show_spinner=False)
# def extract_keywords(df):
#     text = " ".join(df['body'].tolist())
#     words = word_tokenize(text.lower())
#     stop_words = set(stopwords.words('english'))
#     keywords = [word for word in words if word.isalnum() and word not in stop_words]
#     keyword_counts = Counter(keywords)
#     return keyword_counts

@st.cache_data(show_spinner=False)
def preprocess_text(text):
    tokens = word_tokenize(text.lower())
    stop_words = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()
    tokens = [lemmatizer.lemmatize(word) for word in tokens if word.isalnum() and word not in stop_words]
    return tokens

@st.cache_data(show_spinner=False)
def perform_lda(df, num_topics=4, num_words=5):
    texts = df['body'].tolist()
    processed_texts = [preprocess_text(text) for text in texts]
    dictionary = corpora.Dictionary(processed_texts)
    corpus = [dictionary.doc2bow(text) for text in processed_texts]
    lda_model = LdaModel(corpus, num_topics=num_topics, id2word=dictionary, passes=5)
    return lda_model, corpus, dictionary

def plot_sentiment(df):
    df_sentiment = df[['timestamp', 'positive', 'negative']].copy()
    df_sentiment.set_index('timestamp', inplace=True)
    df_sentiment.sort_index(inplace=True)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_sentiment.index, y=df_sentiment['positive'], mode='lines', name='Positive Sentiment', line=dict(color='green')))
    fig.add_trace(go.Scatter(x=df_sentiment.index, y=df_sentiment['negative'], mode='lines', name='Negative Sentiment', line=dict(color='red')))

    fig.update_layout(title='Positive and Negative Sentiment Over Time', xaxis_title='Time', yaxis_title='Sentiment Score', showlegend=True)
    st.plotly_chart(fig)

def plot_emotions(df):
    emotion_columns = ['anger', 'anticip', 'disgust', 'fear', 'joy', 'sadness', 'surprise', 'trust']
    df_emotions = df[['timestamp'] + emotion_columns].copy()
    df_emotions.set_index('timestamp', inplace=True)
    df_emotions.sort_index(inplace=True)

    fig = go.Figure()
    colors = {
        'anger': 'red', 'anticip': 'orange', 'disgust': 'brown', 'fear': 'black', 
        'joy': 'yellow', 'sadness': 'blue', 'surprise': 'purple', 'trust': 'pink'
    }

    for emotion in emotion_columns:
        fig.add_trace(go.Scatter(
            x=df_emotions.index, y=df_emotions[emotion], mode='lines', stackgroup='one', name=emotion, fill='tonexty', line=dict(color=colors[emotion])
        ))

    fig.update_layout(title='Emotion Scores Over Time', xaxis_title='Time', yaxis_title='Scores', showlegend=True)
    st.plotly_chart(fig)

def plot_lda_topics(lda_model, corpus, dictionary, num_topics=4):
    topics = lda_model.show_topics(formatted=False, num_words=10)
    topic_frequencies = Counter([max(lda_model[doc], key=lambda x: x[1])[0] for doc in corpus])

    data = []
    for topic_id, freq in topic_frequencies.items():
        words = ", ".join([word for word, _ in topics[topic_id][1]])
        data.append([words, topic_id, freq])

    df_topics = pd.DataFrame(data, columns=['words', 'topic_id', 'frequency'])

    fig = px.scatter(df_topics, x='topic_id', y='frequency', size='frequency', hover_name='words', 
                     title='LDA Topic Modeling', labels={'topic_id': 'Topic ID', 'frequency': 'Frequency'})

    st.plotly_chart(fig)


if __name__ == "__main__":
    st.title("Reddit Sentiment Analyzer")

    # Sidebar for input parameters
    st.sidebar.header("Fetch Data Parameters")
    subreddit_name = st.sidebar.text_input("Enter a subreddit", "all")
    limit = st.sidebar.number_input("Number of comments to fetch", min_value=10, max_value=1000, value=100)

    if st.sidebar.button("Fetch and Analyze"):
        if subreddit_name:
            with st.spinner("Fetching comments..."):
                comments_data = fetch_comments(subreddit_name, limit)
                if comments_data:
                    # keyword_counts = extract_keywords(df)
                    # common_keywords = keyword_counts.most_common(15)
                    # for keyword, count in common_keywords:
                    #     st.write(f"{keyword}: {count}")
                    df = pd.DataFrame(comments_data)
                    plot_sentiment(df)
                    plot_emotions(df)
                    lda_model, corpus, dictionary = perform_lda(df)
                    plot_lda_topics(lda_model, corpus, dictionary)
                    st.dataframe(df)
                else:
                    st.warning("No comments fetched. Please check the subreddit name and try again.")
        else:
            st.warning("Please enter a subreddit name.")

