package com.db.myproject.mediation.testing

import com.db.myproject.mediation.avro._
import com.db.myproject.mediation.avro.MyEventRecordUtils.newEventRecordWithSuccess

import scala.collection.JavaConverters._

object NotificationsMockData {
  val not_sent_debit_quique: MyEventRecord = {
    val event = new Event()
    event.setId("11")
    event.setTransactionId("unique_kcop")
    event.setNhubTimestamp(1707680509490L)

    val customer = new Customer()
    customer.setId("1")
    customer.setFullName("Quique Cortés")

    val notification = new Notification()
    notification.setId("DEBIT_PURCHASE")
    notification.setMessage(
      "tienes un cargo de 101.0 EUR en tu cuenta *67890. Si quieres puedes pagarlo a plazos."
    )

    MyEventRecord.newBuilder
      .setEvent(event)
      .setCustomer(customer)
      .setNotification(notification)
      .build
  }

  val not_sent_debit_abuela: MyEventRecord = {
    val event = new Event()
    event.setId("22")
    event.setTransactionId("unique_abu")
    event.setNhubTimestamp(1707680509490L)

    val customer = new Customer()
    customer.setId("2")
    customer.setFullName("Abuela Moreno")

    val notification = new Notification()
    notification.setId("DEBIT_PURCHASE")
    notification.setMessage(
      "tienes un cargo de 80000.0 EUR en tu cuenta *67890. Si quieres puedes pagarlo a plazos."
    )

    MyEventRecord.newBuilder
      .setEvent(event)
      .setCustomer(customer)
      .setNotification(notification)
      .build
  }

  val true_sent_debit_quique: MyEventRecord =
    newEventRecordWithSuccess(not_sent_debit_quique, true, Some("SUCESSFUL PUSH"), Some(0))

}
