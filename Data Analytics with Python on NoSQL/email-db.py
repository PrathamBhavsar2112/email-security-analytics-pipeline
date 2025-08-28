import os
import pymongo
import random
import ijson
import re
from dateutil import parser as date_parser
import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import size, to_timestamp, date_trunc, split, avg, min, max, when, sum, count, explode, col, udf
from pyspark.ml.feature import Tokenizer, StopWordsRemover, VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.types import IntegerType, FloatType
from textblob import TextBlob
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pdb

os.environ['PYSPARK_PYTHON'] = r'C:\Users\Harshil Bhavsar\AppData\Local\Programs\Python\Python313\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\Harshil Bhavsar\AppData\Local\Programs\Python\Python313\python.exe'

logging.basicConfig(filename='insertion_log.txt', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['EmailDB']
collection = db['emails']

collection.create_index([("from", 1), ("to", 1), ("date", 1)], unique=True)

email_regex = r'[^@]+@[^@]+\.[^@]+'
def extract_email(field):
    match = re.search(r'<(.*?)>', field)
    return match.group(1) if match else field.strip()

def validate_email(email):
    return bool(re.match(email_regex, email))

def validate_date(date_str):
    try:
        date_parser.parse(date_str)
        return True
    except ValueError:
        return False

def sanitize_body(body):
    body = re.sub(r'https?://\S+|www\.\S+', '', body)
    body = re.sub(r'#\w+', '', body)
    body = re.sub(r'@\w+', '', body)
    body = re.sub(r'[^\w\s.,!?]', '', body)
    return body

# Data insertion process
for i in range(5):
    filename = f'email-db-{i:02d}.json'    
    with open(filename, 'rb') as f:
        parser = ijson.items(f, 'item')
        batch = []
        batch_size = 1000
        
        for email in parser:
            try:
                from_email = extract_email(email['from'])
                to_email = extract_email(email['to'])
                if not validate_email(from_email):
                    logging.info(f'Invalid "from" email: {email["from"]}')
                    continue
                if not validate_email(to_email):
                    logging.info(f'Invalid "to" email: {email["to"]}')
                    continue
                if not validate_date(email['date']):
                    logging.info(f'Invalid date: {email["date"]}')
                    continue
                
                body = sanitize_body(email['body'])
                if any(ord(char) > 127 for char in body):
                    logging.info(f'Non-ASCII body: {email["from"]}, {email["to"]}, {email["date"]}')
                    continue
                
                if 'system' in email:
                    del email['system']
                
                email['body'] = body
                batch.append(email)
                
                if len(batch) >= batch_size:
                    try:
                        result = collection.insert_many(batch, ordered=False)
                        logging.info(f'Inserted {len(result.inserted_ids)} emails from {filename}')
                    except pymongo.errors.BulkWriteError as e:
                        logging.info(f'Inserted {e.details["nInserted"]} emails from {filename}')
                        for error in e.details['writeErrors']:
                            if error['code'] == 11000:
                                logging.info(f'Duplicate: {error["op"]["from"]}, {error["op"]["to"]}, {error["op"]["date"]}')
                            else:
                                logging.info(f'Other error: {error}')
                    batch = []
            
            except KeyError as e:
                logging.info(f'Missing field {e} in email from {filename}')
                continue
        
        if batch:
            try:
                result = collection.insert_many(batch, ordered=False)
                logging.info(f'Inserted {len(result.inserted_ids)} emails from {filename}')
            except pymongo.errors.BulkWriteError as e:
                logging.info(f'Inserted {e.details["nInserted"]} emails from {filename}')
                for error in e.details['writeErrors']:
                    if error['code'] == 11000:
                        logging.info(f'Duplicate: {error["op"]["from"]}, {error["op"]["to"]}, {error["op"]["date"]}')
                    else:
                        logging.info(f'Other error: {error}')

logging.info('Data insertion completed.')

# Task C - Spark Analytics
spark = SparkSession.builder \
    .appName("MongoDB Spark Test") \
    .config("spark.jars", "C:\\spark\\jars\\mongo-spark-connector_2.12-10.4.1.jar") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

mongo_uri = "mongodb://localhost:27017/EmailDB.emails"
extracted_df = spark.read \
    .format("mongodb") \
    .option("spark.mongodb.read.connection.uri", mongo_uri) \
    .load()

extracted_df.show()

# Downsample to 1/3 of dataset
df = extracted_df.sample(fraction=0.3333, seed=987654321)

# Basic statistics
total_emails = df.count()
avg_attachments = df.select(avg(size("attachments")).alias("avg_attach")).first()["avg_attach"]
sender_counts = df.groupBy("from").count().orderBy("count", ascending=False).limit(10).toPandas()

print(f"Total emails: {total_emails}")
print(f"Average attachments per email: {avg_attachments}")
print("Top 10 senders by email count:")
print(sender_counts)

# Email traffic trends
df_with_parsed_date = df.withColumn("parsed_date", to_timestamp(col("date"), "EEE, dd MMM yyyy HH:mm:ss Z"))
daily_trends = df_with_parsed_date.withColumn("day", date_trunc("day", col("parsed_date"))) \
    .groupBy("day").count().orderBy("day").toPandas()

plt.figure(figsize=(10, 6))
plt.plot(daily_trends["day"], daily_trends["count"])
plt.title("Daily Email Traffic")
plt.xlabel("Date")
plt.ylabel("Number of Emails")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("daily_traffic.png")

# Subject text analysis
tokenizer = Tokenizer(inputCol="subject", outputCol="words")
df_tokenized = tokenizer.transform(df)
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
df_filtered = remover.transform(df_tokenized)
top_words = df_filtered.withColumn("word", explode("filtered_words")) \
    .groupBy("word").count().orderBy("count", ascending=False).limit(10).toPandas()
print("Top 10 frequent words in subjects:")
print(top_words)

# Email length statistics
df_with_word_count = df.withColumn("word_count", size(split("body", " ")))
avg_word_count = df_with_word_count.select(avg("word_count")).first()[0]
min_word_email = df_with_word_count.orderBy("word_count").limit(1).collect()[0]
max_word_email = df_with_word_count.orderBy("word_count", ascending=False).limit(1).collect()[0]
word_counts_pd = df_with_word_count.select("word_count").toPandas()

plt.figure(figsize=(10, 6))
word_counts_pd["word_count"].plot.hist(bins=50)
plt.title("Distribution of Email Word Counts")
plt.xlabel("Word Count")
plt.ylabel("Frequency")
plt.savefig("word_count_dist.png")
print(f"Average word count: {avg_word_count}")
print(f"Shortest email: {min_word_email['from']}, Words: {min_word_email['word_count']}")
print(f"Longest email: {max_word_email['from']}, Words: {max_word_email['word_count']}")

# Attachment analysis
df_with_attachment_count = df.withColumn("attachment_count", size("attachments"))
proportion_with_attach = df_with_attachment_count.filter("attachment_count > 0").count() / total_emails
most_freq_attach_count = df_with_attachment_count.groupBy("attachment_count").count() \
    .orderBy("count", ascending=False).first()["attachment_count"]
sender_attach_stats = df_with_attachment_count.groupBy("from") \
    .agg(avg("attachment_count").alias("avg_attachments")).orderBy("avg_attachments", ascending=False).limit(10).toPandas()

print(f"Proportion of emails with attachments: {proportion_with_attach:.2f}")
print(f"Most frequent attachment count: {most_freq_attach_count}")
print("Top 10 senders by average attachments:")
print(sender_attach_stats)

# Task D - Sentiment Analysis
all_emails = list(collection.find({}))

random.seed(987654321)
sampled_emails = random.sample(all_emails, k=int(len(all_emails) * 0.3333))
print(f"Sampled {len(sampled_emails)} emails out of {len(all_emails)}")

def get_polarity(body):
    try:
        text = str(body) 
        return float(TextBlob(text).sentiment.polarity)
    except Exception:
        return 0.0

def get_subjectivity(body):
    try:
        text = str(body)
        return float(TextBlob(text).sentiment.subjectivity)
    except Exception:
        return 0.0

processed_emails = []
for email in sampled_emails:
    polarity = get_polarity(email.get("body", ""))
    subjectivity = get_subjectivity(email.get("body", ""))
    processed_emails.append({
        "from": email.get("from", ""),
        "to": email.get("to", ""),
        "subject": email.get("subject", ""),
        "body": email.get("body", ""),
        "polarity": polarity,
        "subjectivity": subjectivity
    })

sentiment_df = pd.DataFrame(processed_emails)

scaler = MinMaxScaler()
X = scaler.fit_transform(sentiment_df[["polarity", "subjectivity"]])

plt.figure(figsize=(10, 6), dpi=300)
plt.scatter(X[:, 0], X[:, 1], alpha=0.5)
plt.title("Email Sentiment: Polarity vs. Subjectivity (Scaled)")
plt.xlabel("Scaled Polarity (0 to 1)")
plt.ylabel("Scaled Subjectivity (0 to 1)")
plt.savefig("sentiment_scatter_scaled.png")
plt.close()

# DBSCAN clustering
X = sentiment_df[["polarity", "subjectivity"]].values
db = DBSCAN(eps=0.01, min_samples=20).fit(X)
sentiment_df["cluster_label"] = db.labels_

centroids = {}
closest_emails = {}
for label in set(db.labels_):
    if label != -1:
        cluster_points = X[sentiment_df["cluster_label"] == label]
        centroid = np.mean(cluster_points, axis=0)
        centroids[label] = centroid.tolist()
        cluster_df = sentiment_df[sentiment_df["cluster_label"] == label]
        distances = np.linalg.norm(cluster_df[["polarity", "subjectivity"]] - centroid, axis=1)
        min_idx = np.argmin(distances)
        closest_emails[label] = cluster_df.iloc[min_idx][["from", "to", "subject", "body", "polarity", "subjectivity"]].to_dict()

print("Cluster Centroids (Polarity, Subjectivity):")
for label, centroid in centroids.items():
    print(f"Cluster {label}: {centroid}")
print("\nClosest Emails to Centroids:")
for label, email in closest_emails.items():
    print(f"Cluster {label}: {email}")

# Task E - Save Results
sentiment_df.to_csv("sentiment_data.csv", index=False)

clustering_results = {
    "centroids": centroids,
    "closest_emails": closest_emails
}

sender_counts.to_csv("sender_counts.csv", index=False)
top_words.to_csv("top_words.csv", index=False)
word_counts_pd.to_csv("word_counts.csv", index=False)
sender_attach_stats.to_csv("sender_attach_stats.csv", index=False)
sentiment_df.to_csv("sentiment_data.csv", index=False)
with open("clustering_results.json", "w") as f:
    json.dump({"centroids": {int(k): v for k, v in centroids.items()},
               "closest_emails": {int(k): v for k, v in closest_emails.items()}}, f, indent=4)

spark.stop()
