package com.IoT.Consumer

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.Logger

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

case class IotDeviceConsumer(brokers: String, pollTimeout:Int, topic: String) {
  val logger = Logger.getLogger(IotDeviceConsumer.getClass)
  private final val deserializer = "org.apache.kafka.common.serialization.StringDeserializer"
  lazy val currentKafkaConsumer: Try[KafkaConsumer[String, String]] = Try(new KafkaConsumer[String, String](configuration))

  private def configuration: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)

    props.put("key.deserializer", deserializer)
    props.put("value.deserializer", deserializer)
    props.put("group.id", "IoT-group")
    props
  }

  import scala.collection.JavaConverters._

  def receive: Try[Seq[String]] = {
    currentKafkaConsumer match {
      case Success(c) => {
        try {
          c.subscribe(util.Collections.singletonList(topic))

          val messages = c.poll(pollTimeout)
          Success(messages.asScala.map { r => r.value() }.toSeq)
        } catch {
          case NonFatal(e) => Failure(e)
        }
      }
      case Failure(e) => Failure(e)
    }
  }

  def close: Unit = {
    currentKafkaConsumer match {
      case Success(c) => {
        c.close()
      }
      case Failure(e) => throw e
    }
  }

  def isValid: Boolean = pollTimeout > 0 && brokers.nonEmpty && topic.nonEmpty
}
