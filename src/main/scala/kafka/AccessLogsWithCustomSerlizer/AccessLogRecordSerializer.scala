package kafka.AccessLogsWithCustomSerlizer

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util

import org.apache.kafka.common.serialization.Serializer


class AccessLogRecordSerializer extends Serializer[AccessLogRecord] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
  }

  def serialize(topic: String, data: AccessLogRecord): Array[Byte] = {
    try {

      val byteOutputStream = new ByteArrayOutputStream()
      val objectSerialized = new ObjectOutputStream(byteOutputStream)
      objectSerialized.writeObject(data)
      byteOutputStream.toByteArray
    }
    catch {
      case ex: Exception => throw new Exception(ex.getMessage)
    }
  }

  override def close(): Unit = {
  }
}
