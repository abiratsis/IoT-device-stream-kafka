package com.IoT.Device

case class Location(latitude: Double, longitude: Double)
case class IoTDevice(deviceId: String, temperature: Int, location: Location, time: Long){

  def canEqual(a: Any) = a.isInstanceOf[IoTDevice]

  override def equals(that: Any): Boolean =
    that match {
      case that: IoTDevice => that.canEqual(this) &&
        this.deviceId == that.deviceId &&
        this.temperature == that.temperature &&
        this.location == this.location
      case _ => false
    }

  def toJson() : String = {
    s"""
       |{
       |  "data": {
       |    "deviceId": "${deviceId}",
       |    "temperature": ${temperature},
       |    "location": {
       |      "latitude": ${location.latitude},
       |      "longitude": ${location.longitude}
       |    },
       |    "time": ${time}
       |  }
       |}""".stripMargin
  }
}

object IoTDevice {
  def parse(json: String) : Option[IoTDevice] = {
    import org.json4s._
    implicit val formats = DefaultFormats
    val data = (org.json4s.jackson.JsonMethods.parse(json) \ "data").toOption

    data match {
      case Some(_) => Some(data.get.extract[IoTDevice])
      case _ => None
    }
  }
}