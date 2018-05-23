package com.IoT.Consumer

import java.awt.Dimension

import com.IoT.HBase.HBaseProxy
import javax.swing.BorderFactory
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

import scala.swing.Dialog.Message
import scala.swing.{BoxPanel, SimpleSwingApplication, _}
import scala.swing.event.ButtonClicked
import scala.util.control.NonFatal

object KafkaToHBaseWindow extends SimpleSwingApplication{
  def top = new MainFrame {
    val logger = Logger.getLogger(KafkaToHBaseWindow.getClass)
//        preferredSize = new Dimension(700, 300)

    title = "Kafka to HBase receiver"

    def getMessages: Unit = {

      try {
        if(squery.isActive)
          totalSavedMessagesTxt.text = hbase.getCount.toString
        else{
          throw new Throwable("Spark job failed. Please check logs.")
        }
      } catch {
        case NonFatal(e) => {
          if(timer.isRunning) timer.stop()
          sparkJob.stop
          logger.error("Failed getting messages", e)
          Dialog.showMessage(contents.head, e.getMessage, title = "Error", Message.Error)
        }
      }
    }

    val onTimer = new javax.swing.AbstractAction() {
      def actionPerformed(e: java.awt.event.ActionEvent) = getMessages
    }
    val timer = new javax.swing.Timer(2000, onTimer)

    /**
      * Kafka settings
      */
    val pollTimeoutLbl = new Label {
      text = "Polling timeout(ms):"
    }
    val pollTimeoutTxt = new TextField {
      text = "500"
    }
    val brokersLbl = new Label {
      text = "Brokers:"
    }
    val brokersTxt = new TextField {
      text = "ambari3.YOURCLUSTER.office:6667"
    }
    val topicLbl = new Label {
      text = "Topic:"
    }
    val topicTxt = new TextField {
      text = "iot-device-topic"
    }

    /**
      * ZookKeeper settings
      */
    val zkTableLbl = new Label {
      text = "Table name:"
    }
    val zkTableTxt = new TextField {
      text = "IoTDeviceTBL"
    }

    val zkQuorumLbl = new Label {
      text = "Quorum:"
    }
    val zkQuorumTxt = new TextField {
      text = "ambari1.YOURCLUSTER.office"
    }

    val zkPortLbl = new Label {
      text = "Port:"
    }
    val zkPortTxt = new TextField {
      text = "2181"
    }

    val zkZNodeParentLbl = new Label {
      text = "ZNode parent:"
    }
    val zkZNodeParentTxt = new TextField {
      text = "/hbase-unsecure"
    }

    val zkRootDirLbl = new Label {
      text = "Root dir:"
    }
    val zkRootDirTxt = new TextField {
      text = "hdfs://ambari1.YOURCLUSTER.office:8020/apps/hbase/data"
    }

    val zkMasterLbl = new Label {
      text = "Master:"
    }
    val zkMasterTxt = new TextField {
      text = "ambari1.YOURCLUSTER.office:16000"
    }

    val totalSavedMessagesLbl = new Label {
      text = "Total saved(HBase):"
    }
    val totalSavedMessagesTxt = new TextField {
      text = "0";
      editable = false
    }
    val startBtn = new Button("Start")
    val stopBtn = new Button("Stop")
    listenTo(startBtn, stopBtn)

    reactions += {
      case ButtonClicked(`startBtn`) => startSparkJob
      case ButtonClicked(`stopBtn`) => stopSparkJob
    }

    contents = new BoxPanel(Orientation.Vertical) {
      contents += Swing.VStrut(10)
      val kafkaSettingsPanel = new BoxPanel(Orientation.Horizontal) {
        contents += pollTimeoutLbl
        contents += Swing.HStrut(10)
        contents += pollTimeoutTxt
        contents += Swing.HStrut(20)
        contents += brokersLbl
        contents += Swing.HStrut(10)
        contents += brokersTxt
        contents += Swing.HStrut(20)
        contents += topicLbl
        contents += Swing.HStrut(10)
        contents += topicTxt
        border = BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Kafka settings")
      }

      contents += kafkaSettingsPanel

      contents += Swing.VStrut(10)
      val zookeeperSettingsPanel = new BoxPanel(Orientation.Horizontal) {
        contents += zkTableLbl
        contents += Swing.HStrut(5)
        contents += zkTableTxt
        contents += Swing.HStrut(10)
        contents += zkQuorumLbl
        contents += Swing.HStrut(5)
        contents += zkQuorumTxt
        contents += Swing.HStrut(10)
        contents += zkPortLbl
        contents += Swing.HStrut(5)
        contents += zkPortTxt
        contents += Swing.HStrut(10)
        contents += zkZNodeParentLbl
        contents += Swing.HStrut(5)
        contents += zkZNodeParentTxt
        contents += Swing.HStrut(10)
        contents += zkRootDirLbl
        contents += Swing.HStrut(5)
        contents += zkRootDirTxt
        contents += Swing.HStrut(10)
        contents += zkMasterLbl
        contents += Swing.HStrut(5)
        contents += zkMasterTxt
        border = BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Zookeeper settings")
      }

      contents += zookeeperSettingsPanel
      contents += Swing.VStrut(10)
      contents += new BoxPanel(Orientation.Horizontal) {
        contents += startBtn
        contents += Swing.HStrut(10)
        contents += stopBtn
        contents += Swing.HStrut(500)
        contents += totalSavedMessagesLbl
        contents += totalSavedMessagesTxt
      }
      contents += Swing.VStrut(10)
    }

    private implicit var spark: SparkSession = _
    private implicit var hbase : HBaseProxy = _
    private var sparkJob : KafkaToHBaseJob = _
    private var squery : StreamingQuery = _

    def startSparkJob: Unit = {

      spark = SparkSession
        .builder
        .appName("IoT-device-stream-kafka")
        .master("local[*]")
        .getOrCreate()

      hbase = HBaseProxy.apply(
        tableName = zkTableTxt.text,
        zk_quorum = zkQuorumTxt.text,
        zk_port = zkPortTxt.text,
        zk_znode_parent = zkZNodeParentTxt.text,
        rootdir = zkRootDirTxt.text,
        master = zkMasterTxt.text)

      sparkJob = KafkaToHBaseJob.apply(brokersTxt.text, topicTxt.text)
      if (!sparkJob.isValid) {
        logger.warn(s"Invalid user input:${sparkJob}")
        Dialog.showMessage(contents.head, "Invalid input. Please specify brokers and kafka topic.", title = "Invalid input", Message.Error)
        return
      }

      if (!hbase.isValid) {
        logger.warn(s"Invalid user input:${hbase}")
        Dialog.showMessage(contents.head, "Invalid input. Please specify zookeeper values for table name, quorum, port, znode-parent, rootdir and master.", title = "Invalid input", Message.Error)
        return
      }

      try {
        squery = sparkJob.saveStreamToHBase
        timer.start()
        logger.info("Timer was started.")

      } catch {
        case NonFatal(e) => {
          timer.stop()
          logger.error("Failed receiving messages", e)
          Dialog.showMessage(contents.head, e.getMessage, title = "Error", Message.Error)
        }
      }
    }

    def stopSparkJob: Unit = {
      logger.info("Stopping timer.")

      try {
        if (timer.isRunning)
          timer.stop()

        if(squery.isActive) {
          squery.stop()
        }

        sparkJob.stop
        logger.info("Timer was stopped.")
      } catch {
        case NonFatal(e) => {
          logger.error("Failed stopping Spark Job", e)
          Dialog.showMessage(contents.head, e.getMessage, title = "Error", Message.Error)
        }
      }
    }
  }
}
