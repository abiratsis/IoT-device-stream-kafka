package com.IoT.Tests

import com.IoT.Consumer.KafkaToHBaseJob
import com.IoT.HBase.HBaseProxy
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.util.{Failure, Success}

class KafkaToHBaseJobTest extends FlatSpec with EmbeddedKafka with BeforeAndAfterAll with Eventually with IntegrationPatience {
  implicit val spark: SparkSession = Util.createSpark
  implicit val hbaseProxy: HBaseProxy = Util.createHBaseProxy
  implicit val config = Util.embeddedKafkaConfig
  final val currentTopic: String = Util.currentTopic
  final val producer = Util.kafkaProducer

  override def beforeAll(): Unit = {
    if (hbaseProxy.connection.isClosed) //abort in case we can't connect
      cancel(s"Couldn't connect to HBase with hbase.zookeeper:${hbaseProxy.zk_quorum}:${hbaseProxy.zk_port} hbase.master:${hbaseProxy.master}")
    //    EmbeddedKafka.start()todo: Make this work with embedded version as well. Currently fails when creating spark-streaming-consumer with embedded kafka on windows
  }

  "KafkaToHBaseJob" should " save received messages to HBase table" in {
    if(!hbaseProxy.tableExists(hbaseProxy.tn))
      hbaseProxy.createOrUpdateTable

    val devices = Util.sampleDevices
    val kafkaJob = KafkaToHBaseJob.apply(Util.currentBroker, currentTopic)
    val await = kafkaJob.saveStreamToHBase

    for (d <- devices) {
      var res = producer.send(d.toJson)

      res match {
        case Success(f: java.util.concurrent.Future[RecordMetadata]) => f.get()
        case Failure(e: Throwable) => fail(e)
      }
    }

    import scala.concurrent.duration._
    eventually(timeout(1.minutes), interval(1.seconds)) {
      assert(hbaseProxy.getCount > 0)
      val actual = hbaseProxy.getAllIoTDevices
      assert(devices.forall(d => actual.contains(d)))
    }

    if (await.isActive)
      await.stop()
  }

  override def afterAll(): Unit = {
    //    EmbeddedKafka.stop
    producer.close
  }
}
