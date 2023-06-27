package com.db.myproject.configs

import org.slf4j.{Logger, LoggerFactory}
import pureconfig._
import pureconfig.generic.auto._

/**
 * TODO: maybe with a trait RootConfig and GenericTypes [T] we can reuse this in utils
 */
object RootConfig {
  val log: Logger = LoggerFactory getLogger getClass.getName

  def readConfig(env: String) = {
    ConfigSource.default
      .at(env)
      .load[ParentConfig] match {
      case Right(conf) => conf
      case Left(err) =>
        err.toList.foreach(e => log.error(s"$e"))
        throw new Exception("Wrong ParentConfig")
    }
  }
  def readConfigFromString(rawConfig: String, env: String) = {
    log.info(s"reading 'readConfigFromString' for env=$env")
    ConfigSource
      .string(rawConfig)
      .at(env)
      .load[ParentConfig] match {
      case Right(conf) => conf
      case Left(err) =>
        err.toList.foreach(e => log.error(s"$e"))
        throw new Exception("Wrong ParentConfig")
    }
  }

  def apply(env: String): ParentConfig = {
    log.info(s"Reading ParentConfig...")
    env.toLowerCase match {
      case "dev"  => readConfig("dev")
      case "uat"  => readConfig("uat")
      case "prod" => readConfig("prod")
    }
  }

}
