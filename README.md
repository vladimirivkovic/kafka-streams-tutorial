# Kafka Streams Tutorial

Kafka Streams is a client library for building applications and microservices, where the input and output data are stored in Kafka clusters. It combines the simplicity of writing and deploying standard Java and Scala applications on the client side with the benefits of Kafka's server-side cluster technology. Read more [here](https://kafka.apache.org/documentation/streams/).

## Prereqs
  - JDK 1.8
  - Maven build tool
  - Eclipse IDE (optional)

## Step 1 - Run Kafka
Step 1 and 2 of the official [Kafka Quickstart](https://kafka.apache.org/quickstart).

## Step 2 - Start Kafka Producer
```sh
$ cd kafka-topic-producer
$ mvn exec:java -Dexec.mainClass="com.example.kafka_topic_producer.App"
```

Dataset used for generating a stream of records can be found on [Kaggle](https://www.kaggle.com/lpisallerl/air-tickets-between-shanghai-and-beijing/).

## Step 3 - Start Stream Processing App
```sh
$ cd kafka-streams-examples
$ mvn exec:java -Dexec.mainClass="com.example.kafka_streams_examples.<ExampleName>"
```

[KaDeck Community Edition](https://www.xeotek.com/kadeck/free-community-edition/) is a free GUI tool for monitoring Kafka topics and messages.