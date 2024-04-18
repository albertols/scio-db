package com.db.myproject.mediation

import com.db.myproject.streaming.utils.dofn.ssl.SslConfig.SslConfigPath

package object configs {

  def absoluteURL(mediationConfig: MediationConfig) =
    mediationConfig.mediation.endpoint.fullUrl + mediationConfig.mediation.endpoint.domain

  case class MediationConfig(gcsBucket: String, gcp: Gcp, mediation: Mediation)

  case class Gcp(project: String, database: String)

  case class Mediation(
                        version: String,
                        configBlobPath: String,
                        pubsubAvro: String,
                        pubsubAvroAnalytics: String,
                        gcsHistoricalRelativePath: String,
                        retryNotifications: Boolean,
                        initialLoadBersDays: Int,
                        berWindow: String,
                        httpClientType: String,
                        ttlTime: Int,
                        akka: Option[Akka],
                        sslConfigPath: SslConfigPath,
                        endpoint: Endpoint
  )

  case class Akka(
                   maxOpenRequests: Int,
                   maxOpenConnection: Int,
                   initialTimeout: Long,
                   completionTimeout: Long,
                   buffer: Int,
                   throttleRequests: Int,
                   throttlePerSecond: Int,
                   throttleBurst: Int
                 )

  case class Endpoint(
                       fullUrl: String,
                       url: String,
                       domain: String,
                       enabled: Boolean,
                       username: String,
                       password: String)

}
