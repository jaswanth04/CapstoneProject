
#spark modules

import json
import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.classification import NaiveBayesModel
from pyspark.ml.feature import IndexToString

import json
import time

from flask import Flask, request, abort, session, jsonify, send_file, redirect, Response

def create_app():
    app = Flask(__name__)

    spark = SparkSession.builder.appName("predictor").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    @app.route('/predict', methods=['POST'])
    def predict():
        
        # query = '{"query": "India recorded 3,74,397 accidental deaths in 2020, down from 4,21,104 in 2019, with road crashes constituting over 35 per cent of such fatalities"}'

        query = request.json

        print(query)

        df_data = [{'news': query['query'], 'label': "news"}]


        df_to_predict = spark.createDataFrame(df_data)

        feature_pipe_location = "/model/feature_pipe"
        model_location = "/model/lrModel"
        feature_pipe = PipelineModel.load(feature_pipe_location)
        model = NaiveBayesModel.load(model_location)

        raw_features = feature_pipe.transform(df_to_predict)
        predicted_df = model.transform(raw_features)

        label_indexer = feature_pipe.stages[3]
        labels = label_indexer.labels

        index_to_label = IndexToString(inputCol="prediction", outputCol="prediction_label", labels=labels)
        predicted_df_label = index_to_label.transform(predicted_df)

        prediction = predicted_df_label.select("prediction_label").collect()

        return_dict = {"prediction": prediction[0]["prediction_label"]}

        return jsonify(return_dict), 200
        
    return app


if __name__ == "__main__":
    time.sleep(20)
    app = create_app()
    app.run(host='0.0.0.0', port=3000)