package com.db.myproject.mediation.nhub.model

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.DefaultJsonProtocol

object MyHttpRequest extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val format = jsonFormat6(HttpRequest.apply)

  case class HttpRequest(
    appPassword: String,
    appUserName: String,
    customerId: String,
    notificationInfo: String,
    pushMessageText: String,
    systemFrom: String
  )
}
