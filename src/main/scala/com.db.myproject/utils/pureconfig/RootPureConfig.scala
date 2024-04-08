package com.db.myproject.utils.pureconfig

import org.slf4j.{Logger, LoggerFactory}
import pureconfig._

import scala.reflect.ClassTag

object RootPureConfig {
  private val log: Logger = LoggerFactory getLogger getClass.getName

  def readConfig[T](configEnvRoot: String, pureConfigSourceEnum: PureConfigSourceEnum.Value, optionalPathOrRawConfig: Option[String])(implicit reader: ConfigReader[T]): T = {
    log.info(s"readConfig(pureconfig) for env=$configEnvRoot, pureConfigSourceEnum=$pureConfigSourceEnum")
    lazy val illegalPathStringException = throw new IllegalArgumentException(s"None 'optionalPathOrRawConfig'=$optionalPathOrRawConfig")
    val configMethod = pureConfigSourceEnum match {
      // resources/application.conf
      case PureConfigSourceEnum.DEFAULT => ConfigSource.default
        .at(configEnvRoot)
        .load[T]
      case PureConfigSourceEnum.STRING => ConfigSource
        .string(optionalPathOrRawConfig.getOrElse(illegalPathStringException))
        .at(configEnvRoot)
        .load[T]
      case PureConfigSourceEnum.FILE => ConfigSource
        .file(optionalPathOrRawConfig.getOrElse(illegalPathStringException))
        .at(configEnvRoot)
        .load[T]
      case PureConfigSourceEnum.RESOURCES => ConfigSource
        .resources(optionalPathOrRawConfig.getOrElse(illegalPathStringException))
        .at(configEnvRoot)
        .load[T]
      case PureConfigSourceEnum.SYSTEMPROPERTIES => ConfigSource
        .systemProperties
        .at(configEnvRoot)
        .load[T]
      case _ => throw new IllegalPureConfigSourceEnumException(s"Wrong 'pureConfigSourceEnum'=$pureConfigSourceEnum")
    }
    configMethod match {
      case Right(conf) => conf
      case Left(err) =>
        err.toList.foreach(e => log.error(s"$e"))
        throw new PureConfigException("Wrong config")
    }
  }

  def readConfigFromEnv[T](configEnvRoot: PureConfigEnvEnum.Value, pureConfigSourceEnum: PureConfigSourceEnum.Value, optionalPathOrRawConfig: Option[String])(implicit reader: ConfigReader[T]): T = {
    log.info(s"Reading config (env=$configEnvRoot) $reader...")
    configEnvRoot match {
      case PureConfigEnvEnum.local => readConfig(PureConfigEnvEnum.local.toString, pureConfigSourceEnum, optionalPathOrRawConfig)
      case PureConfigEnvEnum.test => readConfig(PureConfigEnvEnum.test.toString, pureConfigSourceEnum, optionalPathOrRawConfig)
      case PureConfigEnvEnum.dev => readConfig(PureConfigEnvEnum.dev.toString, pureConfigSourceEnum, optionalPathOrRawConfig)
      case PureConfigEnvEnum.uat => readConfig(PureConfigEnvEnum.uat.toString, pureConfigSourceEnum, optionalPathOrRawConfig)
      case PureConfigEnvEnum.prod => readConfig(PureConfigEnvEnum.prod.toString, pureConfigSourceEnum, optionalPathOrRawConfig)
      case _ => throw new IllegalPureConfigSourceEnvException(s"Wrong 'PureConfigEnvEnum'=$configEnvRoot")
    }
  }

  // application.conf from GCS or resources/
  def readConfigFromGcsOrResources[GenericCaseClassConfig: ClassTag](project: String, bucketName: String, blobPath: String, envEnum: PureConfigEnvEnum.Value, defaultConfigFromResources: GenericCaseClassConfig)
                                                                    (implicit configReader: pureconfig.ConfigReader[GenericCaseClassConfig]): GenericCaseClassConfig = {
    com.db.myproject.utils.gcp.GCSCommonUtils.rawStringConfigFromGcs(project, bucketName, blobPath) match {
      case Some(rawConfigFromGcs) => readConfigFromEnv(envEnum, PureConfigSourceEnum.STRING, Some(rawConfigFromGcs))
      case None => defaultConfigFromResources
    }
  }


}
