package com.db.myproject.mediation.nhub.model

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.DefaultJsonProtocol

object MyHttpResponse extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val format = jsonFormat7(NotificationResponse.apply)

  def koNotificationResponse(exceptionDescr: String) =
    NotificationResponse("", "", "", "", NHUBResultEnum.NHUB_KO.toString, exceptionDescr, "")

  def emptyNotificationResponse(nHUBResultEnum: NHUBResultEnum.Value) =
    NotificationResponse("", "", "", "", nHUBResultEnum.toString, "", "")

  def isSuccessAndFullResultDescr(response: NotificationResponse) = {
    val fullDescr = s"${response.resultDescr}#${response.result}"
    val success = fullDescr.toLowerCase match {
      case x if x.contains("error") => false
      case x if x.contains("ko")    => false
      case _                        => true
    }
    (success, fullDescr)
  }

  case class NotificationResponse(
    appId: String,
    deviceId: String,
    deviceModel: String,
    deviceName: String,
    result: String,
    resultDescr: String,
    serverTrxId: String
  )

  object NHUBResultEnum extends Enumeration {
    val NHUB_KO, NHUB_NOT_ATTEMPTED_ALREADY_SENT_OR_DUPLICATED = Value
  }
}
