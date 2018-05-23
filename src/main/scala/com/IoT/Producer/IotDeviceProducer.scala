package com.IoT.Producer

import java.util.Properties

import com.IoT.Device.{IoTDevice, Location}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}
import scala.math.BigInt

case class IotDeviceProducer(devices: Int = 3, brokers : String, topic: String){
//  require(devices > 0 && brokers.nonEmpty && topic.nonEmpty)

  private final val serializer = "org.apache.kafka.common.serialization.StringSerializer"
  private lazy val currentKafkaProducer: Try[KafkaProducer[String, String]] = Try(new KafkaProducer[String, String](configuration))
  private def configuration: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", serializer)
    props.put("value.serializer", serializer)
    props
  }

  def send(msg: String): Try[java.util.concurrent.Future[RecordMetadata]] = {
    currentKafkaProducer match {
      case Success(p) => {
        val data = new ProducerRecord[String, String](topic, msg)
        Success(p.send(data))
      }
      case Failure(e) => Failure(e)
    }
  }

  def close:Unit = {
    currentKafkaProducer match {
      case Success(p) => {
        p.close()
      }
      case Failure(e) => throw e
    }
  }

  def isValid : Boolean = devices > 0 && brokers.nonEmpty && topic.nonEmpty
}

object IotDeviceProducer {
  final val minTemp = -40
  final val maxTemp = 40
  private var id: Long = 0
  private val rnd = new scala.util.Random
  val logger = Logger.getLogger(IotDeviceProducer.getClass)
  def generateRandomDeviceData(deviceIds: Array[String]): Array[IoTDevice] = {
    val devices = new Array[IoTDevice](deviceIds.length)

    for (i <- 0 to devices.length - 1) {
      devices(i) = IoTDevice(deviceIds(i), generateTemperature, Location(rnd.nextDouble(), rnd.nextDouble()), System.currentTimeMillis)
    }
    devices
  }

  def generateTemperature: Int = {
    val rnd = new scala.util.Random
    minTemp + rnd.nextInt((maxTemp - minTemp) + 1)
  }

  import java.util.UUID
  def generateGUID(): String = {
    val uuid = new UUID(rnd.nextLong(), rnd.nextLong())
    uuid.toString
  }
}
