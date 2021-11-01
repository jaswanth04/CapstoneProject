from pyspark.sql import SparkSession

# spark = SparkSession \
#     .builder \
#     .appName("myApp") \
#     .config("spark.mongodb.input.uri", "mongodb://mongo/capstone.newsRss") \
#     .config("spark.mongodb.output.uri", "mongodb://mongo/capstone.newsRss") \
#     .getOrCreate()

# df = spark.read.format("mongo").load()

# df.show()

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['broker:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
print("Connected to Kafka!!")