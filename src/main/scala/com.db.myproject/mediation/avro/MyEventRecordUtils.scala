package com.db.myproject.mediation.avro

import com.db.myproject.mediation.MediationService.INITIAL_LOAD_PREFIX
import com.db.myproject.utils.time.TimeUtils.jodaNowGetMillis
import org.slf4j.{Logger, LoggerFactory}

/**
 * Contains some methods "newBer" for workarounds due to: org.apache.beam.sdk.util.IllegalMutationException: PTransform
 * map@{53}:2/ParMultiDo(Anonymous) illegaly mutated value
 */
object MyEventRecordUtils {

  val log: Logger = LoggerFactory getLogger getClass.getName

  def isBerValid(record: MyEventRecord): Boolean = {
    Option(record.getEvent)
      .flatMap(event => Option(event.getTransactionId))
      .isDefined &&
    Option(record.getNotification)
      .flatMap(notification => Option(notification.getId))
      .isDefined &&
    Option(record.getCustomer)
      .flatMap(customer => Option(customer.getId))
      .isDefined
  }

  def mockTransactionId(record: MyEventRecord) = {
    val event = record.getEvent
    val modifiedEvent = new Event(
      event.getId,
      mockTradeKey, // new
      event.getNhubTimestamp
    )
    newEventRecord(
      modifiedEvent,
      record.getCustomer,
      record.getNotification
    )
  }

  def mockTradeKey = {
    val patternOptions = List("E2F3D2BA080E8434A8046870", "2301BED86F331261E380F745")
    val selectedPattern = scala.util.Random.shuffle(patternOptions).head
    // Start with E2 or 23
    val prefix = selectedPattern.substring(0, 2)
    val randomChars = "0123456789ABCDEF"
    val prefixLength = prefix.length
    val requiredLength = selectedPattern.length - prefixLength
    val randomString = (1 to requiredLength)
      .map(_ => randomChars(scala.util.Random.nextInt(randomChars.length)))
      .mkString
    prefix + randomString
  }

  def getIdempotentNotificationKey(record: MyEventRecord) = s"${record.getEvent.getTransactionId}-${record.getCustomer.getId}"

  def newBerWithInitalLoadEventId(record: MyEventRecord) = {
    val event = record.getEvent
    val modifiedEvent = new Event(
      s"${INITIAL_LOAD_PREFIX}_${event.getId}",
      event.getTransactionId,
      event.getNhubTimestamp
    )
    newEventRecord(modifiedEvent, record.getCustomer, record.getNotification)
  }

  def newBerWithLastNHubTimestamp(record: MyEventRecord, beforeNhubTs: Long) = {
    val afterNhubTs = jodaNowGetMillis
    log.info(s"notification_latency=${afterNhubTs - beforeNhubTs} ms")
    val event = record.getEvent
    val modifiedEvent = new Event(event.getId,
      event.getTransactionId,
      afterNhubTs // new
    )
    newEventRecord(modifiedEvent, record.getCustomer, record.getNotification)
  }

  def newEventRecord(event: Event, customer: Customer, notification: Notification) =
    new MyEventRecord(event, customer, notification)

  def newEventRecordOK(record: MyEventRecord, beforeNhubTs: Long, resultDescr: String): MyEventRecord = {
    val afterNhubTs = jodaNowGetMillis
    log.info(s"notification_latency=${afterNhubTs - beforeNhubTs} ms")
    val event = record.getEvent
    val modifiedEvent = new Event(
      event.getId,
      event.getTransactionId,
      afterNhubTs // new
    )
    val notification = record.getNotification
    val modifiedNotification = new Notification(
      notification.getId,
      notification.getMessage,
      notification.getRetries,
      notification.getNhubSuccess, // new
      notification.getAmount,
      resultDescr // new
    )
    newEventRecord(modifiedEvent, record.getCustomer, modifiedNotification)
  }

  def newEventRecordWithSuccess(
    record: MyEventRecord,
    success: Boolean,
    successDescr: Option[String],
    usedRetry: Option[Int]
  ): MyEventRecord = {
    val notification = record.getNotification
    val modifiedNotification = new Notification(
      notification.getId,
      notification.getMessage,
      usedRetry.getOrElse(notification.getRetries).asInstanceOf[Integer],
      success,
      notification.getAmount,
      successDescr.getOrElse(notification.getSuccessDescr)
    )
    newEventRecord(record.getEvent, record.getCustomer, modifiedNotification)
  }

  def newEventRecordWithRetryIncrement(record: MyEventRecord): MyEventRecord = {
    val notification = record.getNotification
    val modifiedNotification = new Notification(
      notification.getId,
      notification.getMessage,
      if (null == notification.getRetries) 0 else notification.getRetries + 1,
      notification.getNhubSuccess,
      notification.getAmount,
      notification.getSuccessDescr
    )
    newEventRecord(record.getEvent, record.getCustomer, modifiedNotification)
  }
}
