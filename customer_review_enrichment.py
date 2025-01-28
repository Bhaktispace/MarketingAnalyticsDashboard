import pandas as pd
import logging 
from datetime import datetime
from sqlalchemy import create_engine
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Download the VADER lexicon for sentiment analysis if not already present
nltk.download('vader_lexicon')

# Define a function to fetch data from SQL database using SQL query
def fetch_data_from_sql():

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("Customer review processor")
    logger.info("Starting")
    start_time = datetime.now()
    
    connection_string = (
    "mssql+pyodbc://GTS\\SQLEXPRESS/"
    "PortfolioProject_MarketingAnalytics?"
    "driver=SQL+Server&"
    "Trusted_Connection=yes"
    )

    engine = create_engine(connection_string)
    query = "SELECT ReviewID, CustomerID, ProductID, ReviewDate, Rating, REPLACE(ReviewText, '  ', ' ') as ReviewText FROM customer_reviews"

    df = pd.read_sql_query(query, engine)
    end_time = datetime.now()
    delta = end_time - start_time
    #elapsed_time_ms: datetime.timedelta = datetime.now() - start_time
    logger.info(f'Processed in {int(delta.total_seconds()*1000)}')
    return df

# Fecth the customer reviews data from SQL Database
customer_reviews_df = fetch_data_from_sql()
# Imitialize the VADER sentiment intensity analyzer for analyzing the sentiment of the text data
sia = SentimentIntensityAnalyzer()

# Define a function to calculate sentiment score using VADER
def calculate_sentiment(reviews):
    #reviews = 'I like Bhakti'
    sentiment = sia.polarity_scores(reviews)
    return sentiment['compound'] # {'neg': 0.0, 'neu': 0.286, 'pos': 0.714, 'compound': 0.3612}

# Define a function to categorize sentiment using both the sentiment score and the review rating
def categorize_sentiment(score, rating):
    if score > 0.05: # Positive sentiment score
        if rating >=4:
            return 'Positive' #High rating and positive sentiment
        elif rating == 3:
            return 'Mixed Positive' # Neutral rating with positive sentiment
        else:
            return 'Mixed Negative' # Low rating but positive sentiment
    elif score < -0.05: # Negative sentiment score
        if rating <= 2:
            return 'Negative' # Low rating and negative sentiment
        elif rating == 3:
            return 'Mixed Negative' # Neutral rating with negative sentiment
        else:
            return 'Mixed Positive' # High rating but negative sentiment
    else: # Neutral sentiment score
        if rating >= 4:     
            return 'Positive' # High rating with neutral sentiment
        elif rating <= 2:
            return 'Negative' # Low rating with neutral sentiment
        else:
            return 'Neutral' # Neutral rating and neutral sentiment
        
# Define a function to bucket sentiment scores into text ranges
def sentiment_bucket(score):
    if score >= 0.5:
        return '0.5 to 1.0' # Strongly positive sentiment
    elif 0.0 <= score <0.5:
        return '0.0 to 0.49' # Mildly positive sentiment 
    elif -0.5 <= score < 0.0:
        return '-0.49 to 0.0' # Mildly negative sentiment
    else:
        return '-1.0 to -0.5'
    
# Apply sentiment analysis to calculate sentiment score for each review
customer_reviews_df['SentimentScore'] = customer_reviews_df['ReviewText'].apply(calculate_sentiment)

# Apply sentiment categorization using both text and rating
customer_reviews_df['SentimentCategory'] = customer_reviews_df.apply(
    lambda row: categorize_sentiment(row['SentimentScore'], row['Rating']), axis=1)

# Applying Sentiment bucketing to categorize scores into defined ranges
customer_reviews_df['SentimentBucket'] = customer_reviews_df['SentimentScore'].apply(sentiment_bucket)

# Display the first few rows
print(customer_reviews_df.head())

# Saving the data frame to a new CSV file
customer_reviews_df.to_csv('fact_customer_reviews_with_sentiment.csv', index=False)