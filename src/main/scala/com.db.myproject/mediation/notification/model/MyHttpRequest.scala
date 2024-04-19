package com.db.myproject.mediation.notification.model

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.DefaultJsonProtocol

object MyHttpRequest extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val format = jsonFormat3(HttpRequest.apply)

  /*
   * https://jsonplaceholder.typicode.com/guide/
   */
  case class HttpRequest(
    title: String,
    body: String,
    userId: Int
  )
}
