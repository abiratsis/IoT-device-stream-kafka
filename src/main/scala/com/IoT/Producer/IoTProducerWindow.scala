package com.IoT.Producer

import java.awt.Dimension

import org.apache.log4j.Logger

import scala.swing.Dialog.Message
import scala.swing._
import scala.swing.event.ButtonClicked
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object IoTProducerWindow extends SimpleSwingApplication{
  def top = new MainFrame {
    val logger = Logger.getLogger(IoTProducerWindow.getClass)
    preferredSize = new Dimension(500, 140)

    title = "IoT device producer"

    import IotDeviceProducer._

    private var totalSentMessages: Int = 0

    def sendMessage: Unit = {
      val devices = generateRandomDeviceData(deviceIds)

      devices.foreach { d => logger.info(s"Generated measurement:${d}") }

      try {
        for (d <- devices) {
          logger.info(s"Sending message:${d.toJson()}")

          val results = IoTProducer.send(d.toJson)
          results match {
            case Success(_) => {
              totalSentMessages += 1
              totalSentMessagesTxt.text = totalSentMessages.toString
              logger.info("Message was successfully sent.")
            }
            case Failure(e) => throw e
          }

        }
      } catch {
        case NonFatal(e) => {
          timer.stop()

          logger.error("Failed sending message", e)
          Dialog.showMessage(contents.head, e.getMessage, title = "Error", Message.Error)
        }
      }

    }

    val onTimer = new javax.swing.AbstractAction() {
      def actionPerformed(e: java.awt.event.ActionEvent) = sendMessage
    }
    val timer = new javax.swing.Timer(1000, onTimer)

    val devicesLbl = new Label {
      text = "Devices:"
    }
    val devicesTxt = new TextField {
      text = "3"
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

    val totalSentMessagesLbl = new Label {
      text = "Total sent:"
    }
    val totalSentMessagesTxt = new TextField {
      text = "0";
      editable = false
    }
    val startBtn = new Button("Start")
    val stopBtn = new Button("Stop")
    listenTo(startBtn, stopBtn)

    reactions += {
      case ButtonClicked(`startBtn`) => startSending
      case ButtonClicked(`stopBtn`) => stopSending
    }

    contents = new BoxPanel(Orientation.Vertical) {
      contents += new BoxPanel(Orientation.Horizontal) {
        contents += devicesLbl
        contents += devicesTxt
        contents += Swing.HStrut(10)
        contents += brokersLbl
        contents += brokersTxt
        contents += Swing.HStrut(10)
        contents += topicLbl
        contents += topicTxt
      }

      contents += Swing.VStrut(40)
      contents += new BoxPanel(Orientation.Horizontal) {
        contents += startBtn
        contents += Swing.HStrut(10)
        contents += stopBtn
        contents += Swing.HStrut(100)
        contents += totalSentMessagesLbl
        contents += totalSentMessagesTxt
      }

      border = Swing.EmptyBorder(10, 10, 10, 10)
    }
    var IoTProducer: IotDeviceProducer = null
    var deviceIds: Array[String] = null

    def startSending: Unit = {
      IoTProducer = IotDeviceProducer(devicesTxt.text.toInt, brokersTxt.text, topicTxt.text)

      if (!IoTProducer.isValid) {
        logger.warn(s"Invalid user input:${IoTProducer}")
        Dialog.showMessage(contents.head, "Invalid input. Please specify number of devices, brokers and kafka topic.", title = "Invalid input", Message.Error)
        return
      }

      deviceIds = (for (i <- 0 to IoTProducer.devices) yield generateGUID()).toArray[String]
      deviceIds.foreach { id => logger.info(s"Device with id:${id} was generated.") }

      timer.start()
      logger.info("Timer was started.")
    }

    def stopSending: Unit = {
      logger.info("Stopping timer.")

      IoTProducer.close
      logger.info(s"Producer:${IoTProducer} was closed.")

      if (timer.isRunning) timer.stop()
      logger.info("Timer was stopped.")
    }
  }
}