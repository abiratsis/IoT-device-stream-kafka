package com.IoT.Tests

import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import com.IoT.Producer._
import net.manub.embeddedkafka.EmbeddedKafka

class IotDeviceProducerTest extends FlatSpec with EmbeddedKafka with BeforeAndAfterAll{

  implicit val config = Util.embeddedKafkaConfig
  final val currentTopic:String = Util.currentEmbeddedTopic
  final val producer = Util.kafkaEmbeddedProducer

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()
  }

  "IotDeviceProducer instance" should "send message to kafka" in {
    producer.send("test-msg")
    val response = consumeFirstStringMessageFrom(currentTopic)
    assert(Some(response).isDefined && response=="test-msg")
  }

  it should "validate input" in {
    var p = IotDeviceProducer.apply(1, "", "some-topic")//invalid broker
    assert(!p.isValid)

    p = IotDeviceProducer.apply(1, "broker:2222", "")//invalid topic
    assert(!p.isValid)

    p = IotDeviceProducer.apply(0, "broker:2222", "topic")//invalid devices
    assert(!p.isValid)

    p = IotDeviceProducer.apply(1, "localhost:9092", currentTopic)
    assert(p.isValid)
  }

  it should "fail sending message given invalid broker" in{
    val p = IotDeviceProducer.apply(1, "_invalid_broker:666", currentTopic)

    val result = p.send("msg")

    assert(result.isFailure)
    assert(result.failed.get != null)
    assert(result.failed.get.getMessage.equals("Failed to construct kafka producer"))
  }

  "IotDeviceProducer singleton" should "be able to generate GUIDs" in {
    val id = IotDeviceProducer.generateGUID()
    assert(id != null)
  }

  it should "be able to generate temperature" in {
    val temp = IotDeviceProducer.generateTemperature
    assert(temp <= IotDeviceProducer.maxTemp && temp >= IotDeviceProducer.minTemp)
  }

  it should "be able to generate random device data" in {
    val ids = Array(IotDeviceProducer.generateGUID())
    val data = IotDeviceProducer.generateRandomDeviceData(ids)
    assert(data.length == 1)

    val device = data(0)
    assert(device.deviceId.length>0)
    assert(device.location.latitude > 0.0)
    assert(device.location.longitude > 0.0)
    assert(device.time > 0)
    assert(device.temperature <= IotDeviceProducer.maxTemp && device.temperature >= IotDeviceProducer.minTemp)
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    producer.close
  }
}
