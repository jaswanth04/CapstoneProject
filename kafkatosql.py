from kafka import KafkaConsumer
from json import loads
from time import sleep
import mysql.connector
import json
consumer = KafkaConsumer('news',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group-id',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="p@ssw0rd1",
    database="sys"
)
mycursor = mydb.cursor()

sql = "INSERT INTO sys.news (title, date,summary,topic,source) VALUES (%s, %s,%s,%s,%s)"

for event in consumer:
    event_data = event.value
    # Do whatever you want
    print(event_data)
    data=event_data
    mycursor.execute('select count(*) from sys.news')
    print(mycursor.fetchall())
    val = (data['title'],data['date'],data['summary'],data['topic'],data['source'])
    mycursor.execute(sql, val)
    mydb.commit();
   
    print('--------------commit done----------------')
    sleep(2)


