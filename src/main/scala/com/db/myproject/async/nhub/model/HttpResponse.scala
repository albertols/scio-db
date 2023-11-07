package com.db.pwcclakees.mediation.nhub.model

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.DefaultJsonProtocol


object HttpResponse extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val format = jsonFormat7(HttpResponse.apply)

  case class HttpResponse(
    appId: String,
    deviceId: String,
    deviceModel: String,
    deviceName: String,
    result: String,
    resultDescr: String,
    serverTrxId: String
  )
}
