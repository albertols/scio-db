package com.db.myproject.mediation.testing

import com.db.myproject.mediation.avro._
import com.db.myproject.mediation.avro.MyEventRecordUtils.newEventRecordWithSuccess

import scala.collection.JavaConverters._

object NotificationsMockData {
  val null_nhub_debit_quique: MyEventRecord = {
    val event = new Event()
    event.setId("5e309591-b26f-4272-8a56-5e27bfdcc6e6")
    event.setTransactionId("unique_kcop")
    event.setNhubTimestamp(1707680509490L)

    val customer = new Customer()
    customer.setId("80696970")
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

  val null_nhub_debit_abuela: MyEventRecord = {
    val event = new Event()
    event.setId("5e309591-b26f-4272-8a56-5e27bfdcc6e6")
    event.setTransactionId("unique_abu")
    event.setNhubTimestamp(1707680509490L)

    val customer = new Customer()
    customer.setId("356802")
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

  val true_nhub_debit_quique: MyEventRecord =
    newEventRecordWithSuccess(null_nhub_debit_quique, true, Some("SUCESSFUL PUSH"), Some(0))

}
