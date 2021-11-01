#spark modules
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import isnan, when, count, col, split, udf, sum, max,concat

from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import explode

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import NaiveBayes

from pyspark.ml.feature import HashingTF, IDF, Tokenizer

import nltk
from nltk.stem import WordNetLemmatizer
import json
import time
import re
import requests
from kafka import KafkaProducer
import feedparser

from flask import Flask, request, abort, session, jsonify, send_file, redirect, Response

# Updating the db with information from Rapid API
def message_from_rapid():
    producer = KafkaProducer(bootstrap_servers=['broker:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    url = "https://free-news.p.rapidapi.com/v1/search"
    querystring = {"q":"a","lang":"en"}
    headers = {
        'x-rapidapi-host': "free-news.p.rapidapi.com",
        'x-rapidapi-key': "c3b2f57ad2mshf298f21e439d8dbp1a3c9djsn0b6273af2fdd"
        }
    response = requests.request("GET", url, headers=headers, params=querystring)
    msg=response.json()
    for i in range(0,len(msg['articles'])):
        # print(msg['articles'][i])
        title=msg['articles'][i]['title']
        date=msg['articles'][i]['published_date']
        summary=msg['articles'][i]['summary']
        topic=msg['articles'][i]['topic']
        link = msg["articles"][i]['link']
        source=msg['articles'][i]['clean_url']
        feature_dict = {'title': title, 'link': link, 'description': summary, 'pubdate': date,  'topic': topic ,"source": source}
        feature_json = json.dumps(feature_dict)
        # time.sleep(1)
        print(feature_json)
        producer.send(topic='news', value=feature_json)

        if i > 4000:
            break


def get_response_from_feedparser(url):
    feed = feedparser.parse(url)

    posts = feed.entries
    # print(posts)
    post_list = []

    for post in posts:
        # print(post)
        temp = {}

        try:
            temp["title"] = post.title
            temp["link"] = post.link
            temp["description"] = post.description
            temp["pubdate"] = post.published
        except:
            print("Exception")
        print(temp)
        post_list.append(temp)

    return post_list


# Updating the db with information from RSS feed
def update_db():
    producer = KafkaProducer(bootstrap_servers=['broker:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    print("Connected to Kafka!!")


    times_rss = {"Top-Stories": "https://timesofindia.indiatimes.com/rssfeedstopstories.cms", 
            "India": "https://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms",
            "World" :"https://timesofindia.indiatimes.com/rssfeeds/296589292.cms",
            "Business": "https://timesofindia.indiatimes.com/rssfeeds/1898055.cms",
            "Cricket": "https://timesofindia.indiatimes.com/rssfeeds/54829575.cms",
            "Sports": "https://timesofindia.indiatimes.com/rssfeeds/4719148.cms",
            "Science": "https://timesofindia.indiatimes.com/rssfeeds/-2128672765.cms",
            "Environment": "https://timesofindia.indiatimes.com/rssfeeds/2647163.cms",
            "Tech": "https://timesofindia.indiatimes.com/rssfeeds/66949542.cms",
            "Education": "https://timesofindia.indiatimes.com/rssfeeds/913168846.cms",
            "Entertainment": "https://timesofindia.indiatimes.com/rssfeeds/1081479906.cms",
            "Life_style": "https://timesofindia.indiatimes.com/rssfeeds/2886704.cms"}

    hindu_rss = {"Business": "https://www.thehindu.com/business/feeder/default.rss",
                "Agriculture": "https://www.thehindu.com/business/agri-business/feeder/default.rss",
                "Economy": "https://www.thehindu.com/business/Economy/feeder/default.rss",
                "Sports": "https://www.thehindu.com/sport/feeder/default.rss",
                "Cricket": "https://www.thehindu.com/sport/cricket/feeder/default.rss",
                "Football": "https://www.thehindu.com/sport/football/feeder/default.rss",
                "Hockey": "https://www.thehindu.com/sport/hockey/feeder/default.rss",
                "Tennis": "https://www.thehindu.com/sport/tennis/feeder/default.rss",
                "Atheletics": "https://www.thehindu.com/sport/athletics/feeder/default.rss",
                "Races": "https://www.thehindu.com/sport/races/feeder/default.rss",
                "Entertainment": "https://www.thehindu.com/entertainment/feeder/default.rss",
                "Movies": "https://www.thehindu.com/entertainment/movies/feeder/default.rss",
                "Life_style": "https://www.thehindu.com/life-and-style/feeder/default.rss",
                "Fashion": "https://www.thehindu.com/life-and-style/fashion/feeder/default.rss",
                "Travel": "https://www.thehindu.com/life-and-style/travel/feeder/default.rss"
                }

    for topic, url in times_rss.items():
        article_list = get_response_from_feedparser(url)
        
        for article in article_list:
            article["topic"] = topic
            article["source"] = "Times"
            print(article)
            producer.send(topic='news', value=article)

    for topic, url in hindu_rss.items():
        article_list = get_response_from_feedparser(url)
        
        for article in article_list:
            article["topic"] = topic
            article["source"] = "Hindu"
            print(article)
            producer.send(topic='news', value=article)



def create_app():
    app = Flask(__name__)

    stopwords = nltk.corpus.stopwords.words('english')


    # spark = SparkSession.builder.appName("news_model").getOrCreate()
    

    spark = SparkSession \
    .builder \
    .appName("trainer") \
    .config("spark.mongodb.input.uri", "mongodb://mongo/capstone.newsRss") \
    .config("spark.mongodb.output.uri", "mongodb://mongo/capstone.newsRss") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    # spark = SparkSession.builder.config("spark.jars", "/usr/share/java/mysql-connector-java-8.0.25.jar") \
    #     .master("local").appName("PySpark_MySQL_test").getOrCreate()

    @app.route('/train', methods=['GET'])
    def train():
        
        # Uncomment below for reading data frame from mysql

            # df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/sys") \
    #     .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "news") \
    #     .option("user", "root").option("password", "p@ssw0rd1").load()

        # Comment below line and chose the approriate data frame
        # df = spark.read.csv("data.csv", header=True, inferSchema=True)

        update = request.args.get("update")

        update_value = (update == "true") or (update == "True") or (update == "TRUE")

        if update_value:
            update_db()
            message_from_rapid()
        df = spark.read.format("mongo").load()
        # df.show()
        df2=df.select("description","title","topic").filter("topic is not  null and description is not null").distinct()
        df3=df2.filter("topic not like '%according%'")

        wn = WordNetLemmatizer()
        def text_preprocessing(review):
            review = re.sub('[^a-zA-Z]', ' ', review)
            review = review.lower()
            review = review.split()
            review = [wn.lemmatize(word) for word in review if not word in stopwords]
            #review= [i for i in review if i not in string.punctuation]
            #review= [i for i in review if i not in ]
            review = ' '.join(review)
            return review

        text_preprocessing_udf = udf(lambda t: text_preprocessing(t),StringType())
        df4=df3.select(concat("description","title").alias("newstext"),"topic")

        df5 = df4.select("newstext","topic").withColumn('news', text_preprocessing_udf(col("newstext")))

        # df5.show()

        tokenizer = Tokenizer(inputCol="news", outputCol="words")
        wordsData = tokenizer.transform(df5)


        vocab_size = wordsData.select(explode(wordsData.words)).distinct().count()

        # Change the number of features based on the vocab size
        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=vocab_size)

        idf = IDF(inputCol="rawFeatures", outputCol="features")
   
        label_stringIdx = StringIndexer(inputCol = "topic", outputCol = "label")

        feature_pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, label_stringIdx])

        feature_pipeline_fit = feature_pipeline.fit(df5)

        feature_df = feature_pipeline_fit.transform(df5)

        feature_pipeline_fit.write().overwrite().save("/model/feature_pipe")

        (trainingData, testData) = feature_df.randomSplit([0.7, 0.3], seed = 100)
        print("Training Dataset Count: " + str(trainingData.count()))
        print("Test Dataset Count: " + str(testData.count()))

        metrics = {}
        clf = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
        model = clf.fit(trainingData)
        
        predictions = model.transform(testData)

        evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
        
        metrics["Logistic"] = evaluator.evaluate(predictions)
        print(f'Accuracy for LR: {metrics["Logistic"]}')

        clf = NaiveBayes(smoothing=1)
        model = clf.fit(trainingData)
        predictions = model.transform(testData)
        metrics["Naive_Bayes"] = evaluator.evaluate(predictions)
        print(f'Accuracy for NB: {metrics["Naive_Bayes"]}')

        model.write().overwrite().save("/model/lrModel")

        return metrics
        
    return app

if __name__ == "__main__":
    time.sleep(20)
    app = create_app()
    app.run(host='0.0.0.0',port=3000)



