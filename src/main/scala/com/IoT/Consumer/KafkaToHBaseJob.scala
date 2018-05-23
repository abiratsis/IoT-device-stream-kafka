package com.IoT.Consumer

import com.IoT.Common.Util
import com.IoT.Device.IoTDevice
import com.IoT.HBase.{HBaseProxy, HBaseSink}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.SparkSession

case class KafkaToHBaseJob(brokers: String, topic :String)(implicit val spark: SparkSession,val hbase: HBaseProxy) {

  import org.apache.spark.sql.streaming.StreamingQuery
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.DataFrame

  lazy val IoTDeviceSchema = StructType(Array(
    StructField("data", StructType(Array(
      StructField("deviceId", StringType),
      StructField("temperature", IntegerType),
      StructField("location", StructType(Array(StructField("latitude", DoubleType), StructField("longitude", DoubleType)))),
      StructField("time", LongType))))))

  private lazy val kafkaStreamReader: DataFrame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", topic)
    .option("failOnDataLoss", "false")
    .load()

  private val writer = new HBaseSink(hbase.tableName,
    hbase.zk_quorum,
    hbase.zk_port,
    hbase.zk_znode_parent,
    hbase.rootdir,
    hbase.master)

  def saveStreamToHBase: StreamingQuery = {
    if (!hbase.tableExists(hbase.tn))
      hbase.createOrUpdateTable

    import spark.implicits._
    import scala.concurrent.duration._
    kafkaStreamReader.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
      .select($"value".as("data").cast("string"))
      .map(r => r.getString(0))
      .writeStream
      .foreach(writer)
      .trigger(Trigger.ProcessingTime((1.seconds)))
      .outputMode("append")
      .option("checkpointLocation", "/tmp/iot-devices-checkpoint")
      .start()
  }

  def stop: Unit = {
    if (!spark.sparkContext.isStopped)
      spark.stop()

    hbase.close
  }

  def isValid: Boolean = brokers.nonEmpty && topic.nonEmpty
}

object KafkaToHBaseJob{

  def main( args:Array[String] ):Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder
      .appName("IoT-Device")
      .master("local[*]")
      .getOrCreate()

    implicit val hbase: HBaseProxy = HBaseProxy.apply(
      tableName = "IoTDeviceTBL",
      zk_quorum = "ambari1.YOURCLUSTER.office",
      zk_port = "2181",
      zk_znode_parent = "/hbase-unsecure",
      rootdir = "hdfs://ambari1.YOURCLUSTER.office:8020/apps/hbase/data",
      master = "ambari1.YOURCLUSTER.office:16000")

    val kafkaJob = KafkaToHBaseJob.apply("ambari3.YOURCLUSTER.office:6667","iot-device-topic")

    val streamingQuery = kafkaJob.saveStreamToHBase
    streamingQuery.awaitTermination()

//    hbase.createOrUpdateTable
//    val devices = hbase.getAllIoTDevices.foreach(println)
//    Util.greenPrint("HBase saved devices:" + hbase.getCount())
  }
}
