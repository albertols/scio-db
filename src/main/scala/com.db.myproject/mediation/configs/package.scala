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
                        sslConfigPath: SslConfigPath,
                        endpoint: Endpoint
  )

  case class Endpoint(
                       targetDevice: String,
                       fullUrl: String,
                       url: String,
                       domain: String,
                       enabled: Boolean,
                       username: String,
                       password: String)

}