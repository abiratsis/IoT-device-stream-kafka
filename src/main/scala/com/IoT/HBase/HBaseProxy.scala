package com.IoT.HBase

import java.io.StringWriter
import java.text.SimpleDateFormat
import com.IoT.Common.Util
import com.IoT.Device.{IoTDevice, Location}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.sql.SparkSession
import scala.util.control.NonFatal

case class HBaseProxy(tableName:String, zk_quorum: String, zk_port: String, zk_znode_parent: String, rootdir: String, master: String) {

  lazy val configuration : Configuration = configure
  lazy val connection = ConnectionFactory.createConnection(configuration)
  lazy val admin = connection.getAdmin
  lazy val currentTable = connection.getTable(TableName.valueOf(tableName))

  private def configure : Configuration = {
    val conf:Configuration = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set("hbase.zookeeper.quorum", zk_quorum)
    conf.set("hbase.zookeeper.property.clientPort", zk_port)
    conf.set("hbase.master", master)
    conf.set("zookeeper.znode.parent", zk_znode_parent)
    conf.set("hbase.rootdir", rootdir)
    conf
  }

  val tn = TableName.valueOf(tableName)

  private def tryTableFunc[P, T <: AnyVal](func: P => T, param : P, maxRetries: Int = 6) : T = {
    var tried: Int = 1
    var done = false
    var result : T = null.asInstanceOf[T]
    val sw = new StringWriter
    var er : java.io.IOException = null

    while (!done && tried <= maxRetries) {
      try {
        result = func(param)
        done = true
      }
      catch {
        case e: java.io.IOException => {
          tried += 1
          Util.randomSleep
          er = e
        }
        case NonFatal(t: Throwable) => throw t
      }
    }

    if(!done)
      throw new Throwable( s"Failed to execute ${func.getClass.getName} after ${maxRetries} attempts.", er)

    result
  }

  def tableExists(tname: TableName) : Boolean = tryTableFunc(admin.tableExists, tname)
  def deleteTable(tname: TableName) : Unit = tryTableFunc(admin.deleteTable, tname)
  def tableIsEnabled(tname: TableName) : Boolean = tryTableFunc(admin.isTableEnabled, tname)
  def enableTable(tname: TableName) : Unit = tryTableFunc(admin.enableTableAsync, tname)
  def tableIsDisabled(tname: TableName) : Boolean = tryTableFunc(admin.isTableDisabled, tname)
  def disableTable(tname: TableName) : Unit = tryTableFunc(admin.disableTableAsync, tname)
  def createTable(desc: HTableDescriptor) : Unit = tryTableFunc(admin.createTable, desc)

  def createOrUpdateTable : Unit = {
    if (tableExists(tn)) {
      if (tableIsEnabled(tn)) {
        disableTable(tn)
        while (!tableIsDisabled(tn)) {
          Util.randomSleep
        }
      }
      deleteTable(tn)
    }

    val tableDescriptor = new HTableDescriptor(tn)
    val columnDescriptor = new HColumnDescriptor(HBaseProxy.columnFamilyName)
    tableDescriptor.addFamily(columnDescriptor)

    if (!tableExists(tn)) createTable(tableDescriptor)

    enableTable(tn)
    while (!tableIsEnabled(tn)){ Util.randomSleep }
  }

  def itemExists(id: String) : Boolean = {
    val devicesTable = connection.getTable(tn)
    val get = new Get(id.getBytes)
    devicesTable.exists(get)
  }

  def saveIoTDevice(device: IoTDevice): Unit ={
    if(itemExists(device.deviceId)) {
      Util.redPrint(s"${device} already exists")
      return
    }

    val devicesTable = connection.getTable(tn)
    val put = new Put(device.deviceId.getBytes)

    val date:String = HBaseProxy.iso8601.format(device.time)
    put.addColumn(HBaseProxy.columnFamilyName.getBytes, "deviceId".getBytes, device.deviceId.getBytes)
    put.addColumn(HBaseProxy.columnFamilyName.getBytes, "temperature".getBytes, Bytes.toBytes(device.temperature))
    put.addColumn(HBaseProxy.columnFamilyName.getBytes, "latitude".getBytes, Bytes.toBytes(device.location.latitude))
    put.addColumn(HBaseProxy.columnFamilyName.getBytes, "longitude".getBytes, Bytes.toBytes(device.location.longitude))
    put.addColumn(HBaseProxy.columnFamilyName.getBytes, "time".getBytes, date.getBytes)
    devicesTable.put(put)
    devicesTable.close()

    Util.greenPrint(s"${device} successfully saved.")
  }

  def getAllIoTDevices()(implicit spark: SparkSession) : Array[IoTDevice] = {
    val devicesRDD = spark.sparkContext.newAPIHadoopRDD(configuration,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
      .map {
        case (_: ImmutableBytesWritable, value: Result) => HBaseProxy.parseResult(value)
      }

    devicesRDD.collect()
  }

  def getCount()(implicit spark: SparkSession) : Long = {
    val devicesRDD = spark.sparkContext.newAPIHadoopRDD(configuration,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
      .map {
        case (_: ImmutableBytesWritable, value: Result) => HBaseProxy.parseResult(value)
      }

    devicesRDD.count
  }

  def close : Unit = {
      if(!connection.isClosed)
        connection.close()
  }

  val isValid : Boolean = tableName.nonEmpty && zk_quorum.nonEmpty && zk_port.nonEmpty && zk_znode_parent.nonEmpty && rootdir.nonEmpty && master.nonEmpty
}

object HBaseProxy {
  final val columnFamilyName = "IoTDeviceCF"
  final val iso8601:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX")

  def parseResult(value: Result) : IoTDevice ={
    val deviceId = Bytes.toString(value.getRow())
    val temperature = Bytes.toInt(value.getValue(columnFamilyName.getBytes, Bytes.toBytes("temperature")))
    val latitude = Bytes.toDouble(value.getValue(columnFamilyName.getBytes, Bytes.toBytes("latitude")))
    val longitude = Bytes.toDouble(value.getValue(columnFamilyName.getBytes, Bytes.toBytes("longitude")))
    val time = Bytes.toString(value.getValue(columnFamilyName.getBytes, Bytes.toBytes("time")))

    IoTDevice(deviceId, temperature, Location(latitude, longitude), iso8601.parse(time).getTime)
  }
}
