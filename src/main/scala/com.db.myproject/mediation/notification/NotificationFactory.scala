package com.db.myproject.mediation.notification

import com.db.myproject.mediation.avro.MyEventRecord
import com.db.myproject.mediation.configs.MediationConfig
import com.db.myproject.mediation.notification.model.MyHttpRequest

import scala.collection.mutable.LinkedHashMap

object NotificationFactory {

  def getRequest(record: MyEventRecord): MyHttpRequest.HttpRequest =
    MyHttpRequest.HttpRequest(
      title = record.getNotification.getId.toString,
      body = record.getNotification.getMessage.toString,
      userId = record.getCustomer.getId.toString.toInt
    )


}
