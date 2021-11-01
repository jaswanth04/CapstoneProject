# CapstoneProject

# Services info

This services are to be used by the front end

Create a new directory by name kafka/rs1 in the current directory

This is to store and persist data stored by mongodb docker container

```
mkdir kafka/rs1
```

## Building the docker images

To build the images

```
docker compose build
```

Starting the docker services

```
docker compose up -d
```

Configuring the mongo-sink connector using Kafka connect. This is required to make sure that the entries in Kafka are pushed in the right collection and database in mongodb

This needs to be run, once the above command is successful

```
# Query to see if the kafka connect is up or not

$curl http://localhost:8083
{"version":"6.2.1-ccs","commit":"fa4bec046a2df3a6","kafka_cluster_id":"llvx1bblSU-SjlKzx1CbMA"}

```
If you get the above result it is up

Configuring the mongo sink connector

```
$curl -X POST -H "Content-Type: application/json" -d @sink-connector.json http://localhost:8083/connectors
{"name":"mongo-sink","config":{"connector.class":"com.mongodb.kafka.connect.MongoSinkConnector","tasks.max":"1","topics":"news","connection.uri":" mongodb://mongo:27017/","database":"capstone","collection":"newsRss","key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.json.JsonConverter","value.converter.schemas.enable":"false","name":"mongo-sink"},"tasks":[],"type":"sink"}

```
After executing the above command, you will get the above response. It is successful

Sometime curl might not work properly in certain terminals, like Windows Power Shell, zsh etc. Please find the the necessary commands for those terminals, in order to run the configuration.


## website  - NEWS ARTICLE CLASSIFIER

http://localhost:9000/