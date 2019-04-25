package kafka.AccessLogsWithCustomSerlizer

object KafkaProperties {
  val TOPIC = "accesslogs"
  val KAFKA_SERVER_URL = "localhost"
  val KAFKA_SERVER_PORT = 9092
  val KAFKA_PRODUCER_BUFFER_SIZE: Int = 64 * 1024
  val CONNECTION_TIMEOUT = 100000
  val TOPIC2 = "topic2"
  val TOPIC3 = "topic3"
  val CLIENT_ID = "SimpleConsumerDemoClient"
}

class KafkaProperties private() {
}
