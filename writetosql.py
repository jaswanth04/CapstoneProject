import json
from pyspark import SparkContext    
from pyspark.ml import PipelineModel
import pyspark as ps
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.functions import udf, col
import math
import time

from pyspark.sql.types import IntegerType
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

#sample_message = '{"month": "10", "hour": "10", "date": "19", "type": "F", "location": "392", "county": "BX", "code": "78"}'




def handle_rdd(rdd):
    if not rdd.isEmpty():
        global ss
        cass_columns = ["title", "date", "summary","topic","source"]
        df = ss.createDataFrame(rdd, schema=cass_columns)
        
        df.show()  
        #df.write.csv('./test.csv')
        #df.write\
    	#.format("org.apache.spark.sql.cassandra")\
    	#.options(table="violation_details", keyspace="parking_keyspace")\
    	#.save(mode="append")
        df.write.format('jdbc').options(
      	 url='jdbc:mysql://localhost:3306/sys',
     	 driver='com.mysql.cj.jdbc.Driver',
     	 dbtable='news',
      	 user='root',
     	 password='p@ssw0rd1').mode('append').save()

sc = SparkContext(appName="news")
ssc = StreamingContext(sc, 5)
ss = SparkSession.builder.appName("news").getOrCreate() 
sample_message = '{"month": "10", "hour": "10", "date": "19", "type": "F", "location": "392", "county": "BX", "code": "78"}'
ks = KafkaUtils.createDirectStream(ssc, ['news'], {'metadata.broker.list': 'localhost:29092'})   
lines = ks.map(lambda x:json.loads(x[1])) 

#transformd = lines.map(lambda tweet: (str(datetime.datetime.now()),tweet['title'],tweet['date'],tweet['summary'],tweet['topic'],tweet['source'])  )
transformd = lines.map(lambda tweet: (tweet['title'],tweet['date'],tweet['summary'],tweet['topic'],tweet['source'])  )
#transformd = lines.map(lambda tweet: (tweet['code'],tweet['location'],tweet['county']))

transformd.foreachRDD(handle_rdd)   


ssc.start()
ssc.awaitTermination()


