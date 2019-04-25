package kafka.exmaple

import java.util.Properties
import java.util.concurrent.ExecutionException

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}


class Producer(val topic: String, val isAsync: Boolean) extends Thread {

  val props: Properties = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  private val producer = new KafkaProducer[Integer, String](props)


  override def run(): Unit = {
    var messageNo = 1
    while (true) {
      val messageStr = "Message_" + messageNo
      val startTime = System.currentTimeMillis
      if (isAsync) { // Send asynchronously
        producer.send(new ProducerRecord[Integer, String](topic, messageNo, messageStr), new DemoCallBack(startTime, messageNo, messageStr))
      }
      else { // Send synchronously
        try {
          producer.send(new ProducerRecord[Integer, String](topic, messageNo, messageStr)).get
          System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")")
        } catch {
          case e@(_: InterruptedException | _: ExecutionException) => e.printStackTrace()
        }
      }
      messageNo += 1
    }
  }
}

class DemoCallBack(val startTime: Long, val key: Int, val message: String) extends Callback {
  /**
    * A callback method the user can implement to provide asynchronous handling of request completion. This method will
    * be called when the record sent to the server has been acknowledged. When exception is not null in the callback,
    * metadata will contain the special -1 value for all fields except for topicPartition, which will be valid.
    *
    * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
    *                  occurred.
    * @param exception The exception thrown during processing of this record. Null if no error occurred.
    */
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    val elapsedTime = System.currentTimeMillis - startTime
    if (metadata != null) System.out.println("message(" + key + ", " + message + ") sent to partition(" + metadata.partition + "), " + "offset(" + metadata.offset + ") in " + elapsedTime + " ms")
    else exception.printStackTrace()
  }
}