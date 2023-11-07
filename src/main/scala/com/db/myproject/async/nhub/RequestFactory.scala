package com.db.pwcclakees.mediation.nhub

import com.db.pwcclakees.mediation.SCIOAsyncService.mediationConfig
import com.db.pwcclakees.mediation.model.BusinessEvent.BusinessEventCirce
import com.db.pwcclakees.mediation.nhub.model.{DeviceRequest, PushRequest}

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
