Kafka Basic Log Streaming
====================
> ## Command to Start Kafka Cluster :
##
##
> #### Start The Zookeeper
> bin/zookeeper-server-start.sh config/zookeeper.properties
> #### Start The Kafka Server
> bin/kafka-server-start.sh config/server.properties/
> #### List the Topics
> bin/kafka-topics.sh --list --bootstrap-server localhost:9092