package kafka.AccessLogsWithCustomSerlizer

/** *
  * Start Zookepper
  * bin/zookeeper-server-start.sh config/zookeeper.properties
  * Start Kafka Server
  * bin/kafka-server-start.sh config/server.properties
  * Start Topicl
  * bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic topic-access-log
  */

object KafkaConsumerProducerDemo {
  def main(args: Array[String]): Unit = {

    val isAsync: Boolean = (args.length == 0) || (!args(0).trim().equalsIgnoreCase("sync"))
    val producerThread: Producer = new Producer(KafkaProperties.TOPIC, isAsync)
    producerThread.start()

    val consumerThread: Consumer = new Consumer(KafkaProperties.TOPIC)
    consumerThread.start()
  }
}