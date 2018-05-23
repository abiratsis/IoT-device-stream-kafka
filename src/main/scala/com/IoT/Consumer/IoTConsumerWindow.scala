package com.IoT.Consumer

import scala.swing._
import scala.swing.event.ButtonClicked
import scala.swing.SimpleSwingApplication
import java.awt.Dimension
import scala.swing.Dialog.Message
import org.apache.log4j.Logger
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object IoTConsumerWindow extends SimpleSwingApplication{
  def top = new MainFrame {
    val logger = Logger.getLogger(IoTConsumerWindow.getClass)
    preferredSize = new Dimension(550, 140)

    title = "IoT device consumer"

    private var totalReceivedMessages: Int = 0

    def receiveMessages: Unit = {

      try {
        val messages = IoTConsumer.receive

        messages match {
          case Success(seq: Seq[String]) => {
            for (m <- seq) {
              logger.info(s"Received message:\n${m}")
              totalReceivedMessages += 1
              totalReceivedMessagesTxt.text = totalReceivedMessages.toString
            }
          }
          case Failure(e) => throw e
        }
      } catch {
        case NonFatal(e) => {
          timer.stop()

          logger.error("Failed receiving messages", e)
          Dialog.showMessage(contents.head, e.getMessage, title = "Error", Message.Error)
        }
      }
    }

    val onTimer = new javax.swing.AbstractAction() {
      def actionPerformed(e: java.awt.event.ActionEvent) = receiveMessages
    }
    val timer = new javax.swing.Timer(2000, onTimer)

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

    val totalReceivedMessagesLbl = new Label {
      text = "Total received:"
    }
    val totalReceivedMessagesTxt = new TextField {
      text = "0";
      editable = false
    }
    val startBtn = new Button("Start")
    val stopBtn = new Button("Stop")
    listenTo(startBtn, stopBtn)

    reactions += {
      case ButtonClicked(`startBtn`) => startReceiving
      case ButtonClicked(`stopBtn`) => stopReceiving
    }

    contents = new BoxPanel(Orientation.Vertical) {
      contents += new BoxPanel(Orientation.Horizontal) {
        contents += pollTimeoutLbl
        contents += pollTimeoutTxt
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
        contents += totalReceivedMessagesLbl
        contents += totalReceivedMessagesTxt
      }

      border = Swing.EmptyBorder(10, 10, 10, 10)
    }
    var IoTConsumer: IotDeviceConsumer = null

    def startReceiving: Unit = {
      IoTConsumer = IotDeviceConsumer(brokersTxt.text, pollTimeoutTxt.text.toInt, topicTxt.text)

      if (!IoTConsumer.isValid) {
        logger.warn(s"Invalid user input:${IoTConsumer}")
        Dialog.showMessage(contents.head, "Invalid input. Please specify poll timeout, brokers and kafka topic.", title = "Invalid input", Message.Error)
        return
      }

      timer.start()
      logger.info("Timer was started.")
    }

    def stopReceiving: Unit = {
      logger.info("Stopping timer.")

      IoTConsumer.close
      logger.info(s"Consumer:${IoTConsumer} was closed.")

      if (timer.isRunning) timer.stop()
      logger.info("Timer was stopped.")
    }
  }
}
