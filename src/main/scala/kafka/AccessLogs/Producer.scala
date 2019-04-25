package kafka.AccessLogs

import java.util.Properties
import java.util.concurrent.ExecutionException

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

import scala.io.Source


class Producer(val topic: String, val isAsync: Boolean) extends Thread {

  val props: Properties = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  private val producer = new KafkaProducer[Integer, String](props)


  override def run(): Unit = {
    var messageNo = 1
    val lineParser: LineParser = new LineParser()
    val filename = "data/access.log"
    val accessLog = Source.fromFile(filename)
    var lines = accessLog.getLines
    val filteredLines = lines.map(line => lineParser.parseLine(line)).filter(record => (if (record.isDefined) true else false) && record.get.request.startsWith("GET")).map(x => x.get)


    for (value <- filteredLines) {
      val messageStr = value.toString
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
    accessLog.close()
  }
}

class DemoCallBack(val startTime: Long, val key: Int, val message: String) extends Callback {
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    val elapsedTime = System.currentTimeMillis - startTime
    if (metadata != null) System.out.println("message(" + key + ", " + message + ") sent to partition(" + metadata.partition + "), " + "offset(" + metadata.offset + ") in " + elapsedTime + " ms")
    else exception.printStackTrace()
  }
}