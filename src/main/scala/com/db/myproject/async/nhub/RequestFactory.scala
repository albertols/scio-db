package com.db.myproject.async.nhub

import com.db.myproject.async.SCIOAsyncService.mediationConfig
import com.db.myproject.async.model.BusinessEvent.BusinessEventCirce
import com.db.myproject.async.nhub.model.{DeviceRequest, PushRequest}

import scala.collection.mutable.LinkedHashMap

object RequestFactory {

  def getDeviceRequest(
    ber: BusinessEventCirce
  ): DeviceRequest.DeviceRequest =
    DeviceRequest.DeviceRequest(
      appPassword = mediationConfig.mediation.nhub.password,
      appUserName = mediationConfig.mediation.nhub.username,
      pushMessageText = ber.notification.message
    )

  def getPushRequest(
    ber: BusinessEventCirce
  ): PushRequest.PushRequest =
    PushRequest.PushRequest(
      appPassword = mediationConfig.mediation.nhub.password,
      appUserName = mediationConfig.mediation.nhub.username,
      pushMessageText = ber.notification.message
    )
}
