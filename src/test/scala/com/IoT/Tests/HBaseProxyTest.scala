package com.IoT.Tests

import com.IoT.Device.{IoTDevice, Location}
import com.IoT.HBase.HBaseProxy
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

class HBaseProxyTest extends FlatSpec with BeforeAndAfterAll {
  implicit val spark : SparkSession = Util.createSpark
  implicit val hbaseProxy : HBaseProxy = Util.createHBaseProxy

  override def beforeAll(): Unit = {
    if (hbaseProxy.connection.isClosed) //abort in case we can't connect
      cancel(s"Couldn't connect to HBase with hbase.zookeeper:${hbaseProxy.zk_quorum}:${hbaseProxy.zk_port} hbase.master:${hbaseProxy.master}")
  }

  "HBaseProxy" should "create or update table" in {
    if(!hbaseProxy.tableExists(hbaseProxy.tn))
      hbaseProxy.createOrUpdateTable

    assert(hbaseProxy.tableExists(hbaseProxy.tn))
  }

  it should "be able to save IoTDevice objects" in {
    val expected = IoTDevice.apply("b138473a-3e15-4528-89ec-9d1b53c3213b", 22, Location(0.4931112854700105, 0.6134570002429856), 1525564902524L)

    hbaseProxy.saveIoTDevice(expected)

    val devicesRDD = spark.sparkContext.newAPIHadoopRDD(hbaseProxy.configuration,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
      .map {
        case (_: ImmutableBytesWritable, value: Result) => HBaseProxy.parseResult(value)
      }

    assert(devicesRDD.count() > 0)
    val actual = devicesRDD.filter(_.deviceId == "b138473a-3e15-4528-89ec-9d1b53c3213b").first()
    assert(expected == actual)
  }

  it should "be able to retrieve all devices" in {
    val expected = Util.sampleDevices

    expected.foreach{hbaseProxy.saveIoTDevice}

    val actual = hbaseProxy.getAllIoTDevices

    actual.foreach{ d =>
      assert(expected.contains(d))
    }
  }
}
