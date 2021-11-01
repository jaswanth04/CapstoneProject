
Use command to bring up the mongo db and configure mongo sink from kafka


Create a new connector before running the rss feed.

This connector will create a link between kafka connect and mongodb

```
# To create a new connector
curl -X POST -H "Content-Type: application/json" -d @sink-connector.json http://localhost:8083/connectors

# To update the existing mongo sink connector 

curl -X PUT -H "Content-Type: application/json" -d @sink-update-connector.json http://localhost:8083/connectors/mongo-sink/config

```

Check the data after running the rss feed

To check the rows in mongodb

```
# Connect to the mongo docker image
docker exec -it mongo bash

# Connect to the "capstone" database created in the connection
mongosh capstone

# Check the stored data
db.newsRss.find()

# Check the count of data stored
db.newsRss.count()

```