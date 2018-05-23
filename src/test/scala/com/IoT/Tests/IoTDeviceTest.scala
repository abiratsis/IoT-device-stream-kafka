package com.IoT.Tests

import com.IoT.Device._

import org.scalatest.FlatSpec

class IoTDeviceTest extends FlatSpec{

  val location = Location(45.005456, 67.797009)
  val device = IoTDevice("e83eeee8-3e2c-41db-86e6-c4d038e4f8eb", 35, location, 342393984)

  "An IoTDevice object " should "have valid toJson method" in {
    import org.json4s._
    import org.json4s.jackson.JsonMethods
    implicit val formats = DefaultFormats
    val json = device.toJson

    assert(JsonMethods.parse(json) \ "data" \ "deviceId" == JString("e83eeee8-3e2c-41db-86e6-c4d038e4f8eb"))
    assert(JsonMethods.parse(json) \ "data" \ "temperature" == JInt(35))
    assert(JsonMethods.parse(json) \ "data" \ "deviceId" == JString("e83eeee8-3e2c-41db-86e6-c4d038e4f8eb"))
    assert(JsonMethods.parse(json) \ "data" \ "location" \ "latitude" == JDouble(45.005456))
    assert(JsonMethods.parse(json) \ "data" \ "location" \ "longitude" == JDouble(67.797009))
    assert(JsonMethods.parse(json) \ "data" \ "time" == JInt(342393984))
  }

  it should "parse json data into IoTDevice" in {
    val json = s"""
                  |{
                  |  "data": {
                  |    "deviceId": "e83eeee8-3e2c-41db-86e6-c4d038e4f8eb",
                  |    "temperature": 35,
                  |    "location": {
                  |      "latitude": 45.005456,
                  |      "longitude": 67.797009
                  |    },
                  |    "time": 342393984
                  |  }
                  |}""".stripMargin

    val fromJson = IoTDevice.parse(json).get
    assert(fromJson === device)
  }

}
