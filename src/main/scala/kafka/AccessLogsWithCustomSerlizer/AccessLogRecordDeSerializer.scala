package kafka.AccessLogsWithCustomSerlizer

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util

import org.apache.kafka.common.serialization.Deserializer


class AccessLogRecordDeSerializer extends Deserializer[AccessLogRecord] {


  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
  }

  def deserialize(topic: String, bytes: Array[Byte]): AccessLogRecord = {
    try {

      val byteInputStream = new ByteArrayInputStream(bytes)
      val inputObject = new ObjectInputStream(byteInputStream)
      val objectDeserialized = inputObject.readObject().asInstanceOf[AccessLogRecord]
      objectDeserialized
    }
    catch {
      case ex: Exception => throw new Exception(ex.getMessage)
    }
  }

  override def close(): Unit = {
  }

}
