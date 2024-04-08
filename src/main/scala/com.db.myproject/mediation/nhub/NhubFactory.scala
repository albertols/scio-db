package com.db.myproject.mediation.nhub

import com.db.myproject.mediation.avro.MyEventRecord
import com.db.myproject.mediation.configs.MediationConfig
import com.db.myproject.mediation.nhub.model.MyHttpRequest

import scala.collection.mutable.LinkedHashMap

object NhubFactory {

  def getRequest(record: MyEventRecord, config: MediationConfig): MyHttpRequest.HttpRequest =
     createMyHttpRequest(record, config)

  def createMyHttpRequest(
    record: MyEventRecord,
    mediationConfig: MediationConfig
  ): MyHttpRequest.HttpRequest =
    MyHttpRequest.HttpRequest(
      appPassword = mediationConfig.mediation.endpoint.password,
      appUserName = mediationConfig.mediation.endpoint.username,
      customerId = record.getCustomer.getId.toString,
      notificationInfo = "notificationInfo",
      pushMessageText = record.getNotification.getMessage.toString,
      systemFrom = "MEDIATION_PROXY"
    )

}
