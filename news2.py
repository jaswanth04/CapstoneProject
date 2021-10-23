from flask.templating import render_template
from kafka import KafkaProducer
import json
import sys
import os
import requests
from bs4 import BeautifulSoup
import time

import http.client, urllib.parse


KAFKA_BROKER = 'localhost:29092'
KAFKA_TOPIC = 'news'

class KafkaConnector(object):

    def __init__(self, kafka_broker, kafka_topic):
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        
        try:
            self.kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_broker)
        except Exception as e:
            print(f"ERROR --> {e}")                                                                                             
            # sys.exit(1) 
def publish_msg():
    conn = http.client.HTTPConnection('api.mediastack.com')

    params = urllib.parse.urlencode({
    	'access_key': '7299980e94ff37c6ce58ecb390354fc3',
    	'categories': '-general,-sports',
    	'sort': 'published_desc',
    	'limit': 100,
    	'countries' : 'in , us'
    	})

    conn.request('GET', '/v1/news?{}'.format(params))

    res = conn.getresponse()
    data = res.read()
    j=json.loads(data)
    articles=j['data']
    for i in range(0,len(articles)):
        title=articles[i]['title']
        date=articles[i]['published_at']
        summary=articles[i]['description']
        topic=articles[i]['category']
        source=articles[i]['source']
        feature_dict = {'title': title, 'date': date, 'summary': summary, 'topic': topic, "source": source}
        feature_json = json.dumps(feature_dict)
        time.sleep(2)
        kafka_connector.kafka_producer.send(KAFKA_TOPIC, bytes(feature_json, encoding="utf8"))

if __name__ == "__main__":
    #kafka_connector = KafkaConnector(os.getenv("KAFKA_BROKER"), os.getenv("KAFKA_TOPIC"))
    kafka_connector = KafkaConnector(KAFKA_BROKER,KAFKA_TOPIC)        
    publish_msg()


