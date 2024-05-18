package com.db.myproject.utils

package object pureconfig {

  object PureConfigEnvEnum extends Enumeration {
    val local, test, dev, uat, prod = Value
  }

  // ConfigSource Enum types: pureconfig.ConfigSource
  object PureConfigSourceEnum extends Enumeration {
    val DEFAULT, FILE, STRING, RESOURCES, SYSTEMPROPERTIES  = Value
  }

  class IllegalPureConfigSourceEnumException (msg: String) extends IllegalArgumentException(msg)
  class IllegalPureConfigSourceEnvException (msg: String) extends IllegalArgumentException(msg)
  class PureConfigException (msg: String) extends RuntimeException(msg)
}
