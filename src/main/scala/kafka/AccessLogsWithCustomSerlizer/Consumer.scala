package kafka.AccessLogsWithCustomSerlizer

import java.time.Duration
import java.util.{Collections, Properties}

import kafka.utils.ShutdownableThread
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}


class Consumer(val topic: String) extends ShutdownableThread("KafkaConsumerExample", false) {

  val props = new Properties
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT)
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer")
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
  props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
  props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "kafka.AccessLogsWithCustomSerlizer.AccessLogRecordDeSerializer")

  val consumer = new KafkaConsumer[Integer, AccessLogRecord](props)

  def doWork(): Unit = {

    consumer.subscribe(Collections.singletonList(this.topic))

    val records: ConsumerRecords[Integer, AccessLogRecord] = consumer.poll(Duration.ofSeconds(1))

    import scala.collection.JavaConverters._
    for (record: ConsumerRecord[Integer, AccessLogRecord] <- records.asScala) {
      System.out.println("Received message: (" + record.key + ", " + record.value + ") at offset " + record.offset)
    }

  }


}