package com.db.myproject.streaming.utils.dofn.ssl

import com.db.myproject.streaming.utils.dofn.ssl.SslConfig.SslConfigPath
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File}
import scala.util.{Failure, Success, Try}

trait SslConfigHelper extends Serializable {
  val log: Logger = LoggerFactory getLogger getClass.getName
  val sslConfigPath: SslConfigPath
  val gcpProject: String

  val sslConfig: SslConfig = Try {
    val sllConfig = SslConfig.getSslConfig(sslConfigPath, gcpProject)
    initSslConfigAndCopyCertValueToWorkersFromSecrets(sllConfig)
    sllConfig
  } match {
    case Success(config) => config
    case Failure(ex)     => throw sslException(ex)
  }
  def sslException(ex: Throwable) = new RuntimeException(s"not set up SslConfigHelper", ex)

  def initSslConfigAndCopyCertValueToWorkersFromSecrets(sslConfig: SslConfig) = {
    log.info(s"init 'sslConfig' and copyCertValueToWorkersFromSecrets...using $SslConfigPath")
    copyCertValueToWorkersFromSecrets(sslConfig.truststoreSecretValue, sslConfig.truststoreLocation)
    copyCertValueToWorkersFromSecrets(sslConfig.keystoreSecretValue, sslConfig.keystoreLocation)
  }

  private def copyCertValueToWorkersFromSecrets(certValue: Array[Byte], location: String) = {
    try {
      val targetFile: File = new File(location)
      if (!targetFile.exists) {
        log.info(s"loading Array[Bytes] certValue: $location (from secrets)")

        import java.io.FileOutputStream
        import java.util.Base64

        val stream = new FileOutputStream(targetFile.getPath)
        try stream.write(Base64.getMimeDecoder.decode(certValue))
        finally if (stream != null) stream.close()
        assureFileExists(location) // assuring the store file exists
      } else log.info(s"no need of new cert location: $location !")

    } catch {
      case e: Exception => throw new Exception("Failed to copy certificate to workers", e)
    }
  }

  private def assureFileExists(name: String): Unit = {
    val f = new File(name) // assuring the store file exists
    if (!f.exists) throw new Exception(s"'$name' file not copied to workers")
    else log.info(s"'$name' copied to worker. File size: ${f.length}")
  }

}
