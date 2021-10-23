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

from flask import Flask, request, abort, session, jsonify, send_file, redirect, Response

def create_app():
    app = Flask(__name__)

    stopwords = nltk.corpus.stopwords.words('english')


    spark = SparkSession.builder.appName("news_model").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # spark = SparkSession.builder.config("spark.jars", "/usr/share/java/mysql-connector-java-8.0.25.jar") \
    #     .master("local").appName("PySpark_MySQL_test").getOrCreate()

    @app.route('/', methods=['GET'])
    def train():
        
        # Uncomment below for reading data frame from mysql

            # df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/sys") \
    #     .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "news") \
    #     .option("user", "root").option("password", "p@ssw0rd1").load()

        # Comment below line and chose the approriate data frame
        df = spark.read.csv("data.csv", header=True, inferSchema=True)
        # df.show()
        df2=df.select("summary","title","topic").filter("topic is not  null and summary is not null").distinct()
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
        df4=df3.select(concat("summary","title").alias("newstext"),"topic")

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

        # feature_df.show()

        (trainingData, testData) = feature_df.randomSplit([0.7, 0.3], seed = 100)
        print("Training Dataset Count: " + str(trainingData.count()))
        print("Test Dataset Count: " + str(testData.count()))

        metrics = {}
        clf = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
        model = clf.fit(trainingData)

        # Uncomment below to save the model
        # model.save("/model/lrModel")
        predictions = model.transform(testData)

        evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
        
        metrics["Logistic"] = evaluator.evaluate(predictions)
        print(f'Accuracy for LR: {metrics["Logistic"]}')

        clf = NaiveBayes(smoothing=1)
        model = clf.fit(trainingData)
        predictions = model.transform(testData)
        metrics["Naive_Bayes"] = evaluator.evaluate(predictions)
        print(f'Accuracy for NB: {metrics["Naive_Bayes"]}')

        return metrics
        
    return app

if __name__ == "__main__":
    time.sleep(20)
    app = create_app()
    app.run(host='0.0.0.0',port=3000)



