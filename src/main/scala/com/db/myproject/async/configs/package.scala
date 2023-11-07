package com.db.pwcclakees.mediation


package object configs {

  case class MediationConfig(gcsBucket: String, mediation: Mediation)

  case class Mediation(
    version: String,
    configBlobPath: String,
    pubsubSub: String,
    retryTopic: String,
    berWindow: String,
    nhub: Nhub
  )

  case class Nhub(
    domain: String,
    soapUrl: String,
    pushUrl: String,
    lastDeviceUrl: String,
    enabled: Boolean,
    username: String,
    password: String,
    platform: Int,
    appCategory: Int,
    clientTrxId: String,
    app: App
  )

  case class App(
    costId: Int,
    `type`: Int,
    entityId: String,
    price: Int,
    countryCode: String,
    currency: String,
    textId: Int
  )

}
