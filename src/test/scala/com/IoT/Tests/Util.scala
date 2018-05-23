package com.IoT.Tests

import com.IoT.Device.{IoTDevice, Location}
import com.IoT.HBase.HBaseProxy
import com.IoT.Producer.IotDeviceProducer
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.spark.sql.SparkSession

object Util {

  lazy val embeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

  final val currentEmbeddedTopic: String = "iot-device-topic"
  final val currentEmbeddedBroker: String = "localhost:9092"

  final val currentTopic: String = "iot-device-topic"
  final val currentBroker: String = "ambari3.YOURCLUSTER.office:6667"

  lazy val kafkaEmbeddedProducer = IotDeviceProducer.apply(1, currentEmbeddedBroker, currentEmbeddedTopic)
  lazy val kafkaProducer = IotDeviceProducer.apply(1, currentBroker, currentTopic)

  def createSpark: SparkSession = {
    SparkSession
      .builder
      .appName("IoT-Device-Test")
      .master("local[*]")
      .getOrCreate()
  }

  def createHBaseProxy: HBaseProxy = {
    HBaseProxy.apply(
      tableName = "IoTDeviceTBL",
      zk_quorum = "ambari1.YOURCLUSTER.office",
      zk_port = "2181",
      zk_znode_parent = "/hbase-unsecure",
      rootdir = "hdfs://ambari1.YOURCLUSTER.office:8020/apps/hbase/data",
      master = "ambari1.YOURCLUSTER.office:16000")
  }

  final lazy val sampleDevices = Seq(
    IoTDevice.apply("b138473a-3e15-4528-89ec-9d1b53c3213b", 22, Location(0.4931112854700105, 0.6134570002429856), 1525564902524L),
    IoTDevice.apply("b138473b-3e15-4528-89ec-9d1b53c3213b", 32, Location(0.1231112854700105, 0.4534570002429856), 1525564902824L),
    IoTDevice.apply("b138473c-3e15-4528-89ec-9d1b53c3213b", 32, Location(0.1231112854700105, 0.4534570002429856), 1525564902824L)
  )

}
