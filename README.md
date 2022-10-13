# java-kafka
### Multi Module Maven Project
- Kafka Basics (Producer & Consumer with different configs)
- Kafka Project : Wikimedia (source) &#8594; Producer &#8594; Kafka Broker &#8594; Consumer &#8594; OpenSearch (destination)
### Run OpenSearch and OpenSeacrh Dashboards
- docker-compose -f docker-compose.yml up
  - http://localhost:9092
  - http://localhost:5601/app/login?nextUrl=%2Fapp%2Fdev_tools#/console
  
Wikimedia stream Url: https://stream.wikimedia.org/v2/stream/recentchange

Kafka Commands - Inside Kafka /bin folder
- zookeeper-server-start.sh ../config/zookeeper.properties
- kafka-server-start.sh ../config/server.properties
- kafka-topics.sh --bootstrap-server localhost:9092 --create --topic demo_topic --partitions 3 replication-factor 1
- kafka-topics.sh --bootstrap-server localhost:9092 --create --topic wikimedia.recentchange --partitions 3 replication-factor 1
