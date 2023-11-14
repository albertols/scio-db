package com.db.myproject.async.model

import com.google.gson.Gson
import io.circe.generic.JsonCodec
import io.circe._
import org.joda.time.DateTime

import java.io.Serializable
import java.time.Duration

object BusinessEvent {

  def isValid(be: BusinessEventCirce): Boolean =
    be.event != null && be.eventType != null && be.notification != null &&
      be.customer != null && be.alertWhiteList != null

  def formatCustomerID(id: String): String = id.dropRight(2)

  def maskBERCustomerName(ber: BusinessEventCirce): BusinessEventCirce = {
    if (ber != null) {
      val fullName = ber.customer.fullName.replaceAll(".", "*")
      ber.copy(customer = ber.customer.copy(fullName = fullName))
    } else {
      null
    }
  }

  def maskedBERCustomerNameGson(ber: BusinessEventCirce): BusinessEventCirce = {
    if (ber != null) {
      val maskedBER = new Gson().fromJson(ber.toString, classOf[BusinessEventCirce])
      val fullName = maskedBER.customer.fullName.replaceAll(".", "*")
      maskedBER.copy(customer = maskedBER.customer.copy(fullName = fullName))
    } else {
      null
    }
  }

  def berCommonLoggingWithMsg(ber: BusinessEventCirce): String = {
    if (ber != null)
      berCommonLogging(ber) + ", message='***'"
    else
      "null"
  }

  def berCommonLogging(ber: BusinessEventCirce): String = {
    if (ber != null)
      s"trade_key=${ber.event.transactionId}, customer=${ber.customer.id}, retries=${ber.notification.retries}"
    else
      "null"
  }

  def getAlertWhiteListBerSink(alertWhiteList: List[Int]): String = s"'$alertWhiteList'"

  @JsonCodec
  case class BusinessEventCirce(
    event: Event,
    eventType: EventType,
    customer: Customer,
    notification: Notification,
    lastTryTimestamp: Option[Long],
    alertWhiteList: List[Int],
    delayed: Option[String]
  ) extends Serializable

  @JsonCodec
  case class Customer(
    id: String
  ) extends Serializable

  @JsonCodec
  case class Event(
    id: String,
    transactionId: String
  ) extends Serializable

  @JsonCodec
  case class EventType(family: Int, useCase: Int) extends Serializable

  implicit val dateTimeEncoder: Encoder[DateTime] = Encoder.encodeString.contramap(_.toString)
  implicit val dateTimeDecoder: Decoder[DateTime] = Decoder.decodeString.emap { str =>
    try Right(DateTime.parse(str))
    catch {
      case e: Exception => Left(s"Invalid DateTime format: $str")
    }
  }

  implicit val durationEncoder: Encoder[Duration] = new Encoder[Duration] {
    override def apply(a: Duration): Json = Json.fromString(a.toString)
  }

  implicit val durationDecoder: Decoder[Duration] = (c: HCursor) =>
    c.as[String].flatMap { str =>
      try {
        Right(Duration.parse(str))
      } catch {
        case e: Exception =>
          Left(DecodingFailure(s"Invalid Duration format: $str", c.history))
      }
    }

  @JsonCodec
  case class Notification(
    id: String,
    retries: Option[Int],
    title: String,
    isPromiscuous: Boolean,
    nhubSuccess: Option[Boolean],
    amount: Double,
    successDescr: Option[String],
    serviceTarget: String,
    timeout: Duration
  ) extends Serializable

  @JsonCodec
  case class User(id: String, name: String) extends Serializable
}
