package com.db.pwcclakees.mediation.nhub.model

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.DefaultJsonProtocol


object PushRequest extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val format = jsonFormat3(PushRequest.apply)

  case class PushRequest(
    appPassword: String,
    appUserName: String,
    pushMessageText: String
  )
}
