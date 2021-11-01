
import json
import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.classification import NaiveBayesModel
from pyspark.ml.feature import IndexToString

spark = SparkSession.builder.appName("predict").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

query = '{"query": "India recorded 3,74,397 accidental deaths in 2020, down from 4,21,104 in 2019, with road crashes constituting over 35 per cent of such fatalities"}'

print(query)

query_dict = json.loads(query)

print(query_dict)

df_data = [{'news': query_dict['query'], 'label': "news"}]


df_to_predict = spark.createDataFrame(df_data)

df_to_predict.show()

feature_pipe_location = "/model/feature_pipe"
model_location = "/model/lrModel"


feature_pipe = PipelineModel.load(feature_pipe_location)
model = NaiveBayesModel.load(model_location)

raw_features = feature_pipe.transform(df_to_predict)

raw_features.show()

predicted_df = model.transform(raw_features)

label_indexer = feature_pipe.stages[3]
labels = label_indexer.labels

index_to_label = IndexToString(inputCol="prediction", outputCol="prediction_label", labels=labels)
predicted_df_label = index_to_label.transform(predicted_df)

prediction = predicted_df_label.select("prediction_label").collect()

# prediction.show(

return_dict = {"prediction": prediction[0]["prediction_label"]}

return_json = json.dumps(return_dict)

