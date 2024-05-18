package com.db.myproject.mediation.notification.model

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.DefaultJsonProtocol

object MyHttpResponse extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val format = jsonFormat4(NotificationResponse.apply)

  val NOT_HTTP_RESPONSE = 0

  val SENT_OR_DUPLICATED: NotificationResponse = emptyNotificationResponse(NHUBResultEnum.NHUB_NOT_ATTEMPTED_ALREADY_SENT_OR_DUPLICATED)

  def koNotificationResponse(exceptionDescr: String): NotificationResponse =
    NotificationResponse(NOT_HTTP_RESPONSE, "", NHUBResultEnum.NHUB_KO.toString, 0)

  def emptyNotificationResponse(nHUBResultEnum: NHUBResultEnum.Value): NotificationResponse =
    NotificationResponse(NOT_HTTP_RESPONSE, "", nHUBResultEnum.toString, 0)

  def isSuccessAndFullResultDescr(response: NotificationResponse): (Boolean, String) = {
    val fullDescr = response.body
    val success = fullDescr.toLowerCase match {
      case x if x.contains("error") => false
      case x if x.contains("ko") => false
      case _ => true
    }
    (success, fullDescr)
  }

  /*
   * https://jsonplaceholder.typicode.com/posts
   */
  case class NotificationResponse(
    id: Int,
    title: String,
    body: String,
    userId: Int
  )

  object NHUBResultEnum extends Enumeration {
    val NHUB_KO, NHUB_NOT_ATTEMPTED_ALREADY_SENT_OR_DUPLICATED = Value
  }
}
