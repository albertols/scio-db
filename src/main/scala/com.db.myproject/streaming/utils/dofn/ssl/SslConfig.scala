package com.db.myproject.streaming.utils.dofn.ssl

import com.db.myproject.utils.core.Using
import com.db.myproject.utils.gcp.SecretManagerClient
import org.slf4j.{Logger, LoggerFactory}

object SslConfig extends Serializable {

  val log: Logger = LoggerFactory getLogger getClass.getName
  case class SslConfigPath(
                            keystoreSecret: String,
                            truststoreSecret: String,
                            keystorePasswordSecret: String,
                            truststorePasswordSecret: String,
                            sslKeystoreLocation: String,
                            sslTruststoreLocation: String
  ) extends Serializable

  def getSslConfig(sslConfigPath: SslConfigPath, gcpProject: String): SslConfig = {
    log.info (s"mapping SslConfigPath to SslConfig with $sslConfigPath")
    Using.tryWithResources(new SecretManagerClient(gcpProject)) { sm =>
      SslConfig(
        sm.getSecretData(sslConfigPath.keystoreSecret).toByteArray,
        sm.getSecretData(sslConfigPath.truststoreSecret).toByteArray,
        sm.getSecretString(sslConfigPath.keystorePasswordSecret),
        sm.getSecretString(sslConfigPath.truststorePasswordSecret),
        sslConfigPath.sslKeystoreLocation,
        sslConfigPath.sslTruststoreLocation
      )
    }
  }

}
case class SslConfig(
                      keystoreSecretValue: Array[Byte],
                      truststoreSecretValue: Array[Byte],
                      keystorePassword: String,
                      truststorePassword: String,
                      keystoreLocation: String,
                      truststoreLocation: String
) extends Serializable
