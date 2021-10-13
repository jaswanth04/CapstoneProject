from flask.templating import render_template
from kafka import KafkaProducer
import json
import sys
import os
import requests
from bs4 import BeautifulSoup
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
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36'}
    querystring = {"q":"Elon Musk","lang":"en"}
    url = "https://timesofindia.indiatimes.com/rssfeedstopstories.cms"
    try:
    	response = requests.get(url, headers)
    except Exception as e:
    	print(e)

    try:
    	soup = BeautifulSoup(response.text, 'lxml')
    except Exception as e:
    	print(e)
    articles = soup.find_all('item')
    articles_dicts = [{'title':a.find('title').text,'date':a.find('pubdate').text,'summary':a.find('description').text,'topic':'','source':a.link.next_sibling.replace('\n','').replace('\t','')} for a in articles]
    for article in articles_dicts:
        feature_json = json.dumps(article)
        time.sleep(2)
        kafka_connector.kafka_producer.send(KAFKA_TOPIC, bytes(feature_json, encoding="utf8"))
        
if __name__ == "__main__":
    #kafka_connector = KafkaConnector(os.getenv("KAFKA_BROKER"), os.getenv("KAFKA_TOPIC"))
    kafka_connector = KafkaConnector(KAFKA_BROKER,KAFKA_TOPIC)        
    publish_msg()


