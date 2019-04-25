package kafka.exmaple

object KafkaConsumerProducerDemo {
  def main(args: Array[String]): Unit = {
    val isAsync: Boolean = (args.length == 0) || (!args(0).trim().equalsIgnoreCase("sync"))
    val producerThread: Producer = new Producer(KafkaProperties.TOPIC, isAsync)
    producerThread.start()

    val consumerThread: Consumer = new Consumer(KafkaProperties.TOPIC)
    consumerThread.start()
  }
}