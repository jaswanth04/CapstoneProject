from flask import Flask, request, abort, session, jsonify, send_file, redirect, Response
from flask.templating import render_template
from kafka import KafkaProducer
import json
import sys
import os
import requests
import time

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
    url = "https://free-news.p.rapidapi.com/v1/search"
    querystring = {"q":"Elon Musk","lang":"en"}
    headers = {
        'x-rapidapi-host': "free-news.p.rapidapi.com",
        'x-rapidapi-key': "c3b2f57ad2mshf298f21e439d8dbp1a3c9djsn0b6273af2fdd"
        }
    response = requests.request("GET", url, headers=headers, params=querystring)
    msg=response.json()
    for i in range(0,len(msg['articles'])):
        title=msg['articles'][i]['title']
        date=msg['articles'][i]['published_date']
        summary=msg['articles'][i]['summary']
        topic=msg['articles'][i]['topic']
        source=msg['articles'][i]['clean_url']
        feature_dict = {'title': title, 'date': date, 'summary': summary, 'topic': topic, "source": source}
        feature_json = json.dumps(feature_dict)
        time.sleep(2)
        kafka_connector.kafka_producer.send(KAFKA_TOPIC, bytes(feature_json, encoding="utf8"))
        
if __name__ == "__main__":
    #kafka_connector = KafkaConnector(os.getenv("KAFKA_BROKER"), os.getenv("KAFKA_TOPIC"))
    kafka_connector = KafkaConnector(KAFKA_BROKER,KAFKA_TOPIC)        
    publish_msg()


