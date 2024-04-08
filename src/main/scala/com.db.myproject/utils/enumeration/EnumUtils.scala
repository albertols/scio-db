package com.db.myproject.utils.enumeration

import scala.util.{Failure, Success, Try}

object EnumUtils {
  def matchEnum[Enum <: Enumeration](input: String, enum: Enum): Option[enum.Value] =
    Try(Some(enum.withName(input)))
    match {
      case Success(value) => value
      case Failure(ex) => None
    }
}
