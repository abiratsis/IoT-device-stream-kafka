package com.IoT.HBase

import com.IoT.Device.IoTDevice
case class HBaseSink(tableName:String, zk_quorum: String, zk_port: String, zk_znode_parent: String, rootdir: String, master: String) extends org.apache.spark.sql.ForeachWriter[String]{

  lazy val hbase: HBaseProxy = HBaseProxy.apply(
    tableName,
    zk_quorum,
    zk_port,
    zk_znode_parent,
    rootdir,
    master)

  override def open(partitionId: Long, version: Long) = {
    true
  }

  override def process(value: String): Unit = {
    hbase.saveIoTDevice(IoTDevice.parse(value).get)
  }

  override def close(errorOrNull: Throwable): Unit = {
    if(!hbase.connection.isClosed)
      hbase.connection.close()
  }
}
