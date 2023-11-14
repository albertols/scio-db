package com.db.myproject.async.nhub.model

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.DefaultJsonProtocol


object DeviceRequest extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val format = jsonFormat3(DeviceRequest.apply)

  case class DeviceRequest(
    appPassword: String,
    appUserName: String,
    pushMessageText: String
  )
}
