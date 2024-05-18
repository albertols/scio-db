package com.db.myproject.utils.core

import scala.util.control.NonFatal

/**
  * Homebrew development, Using is not available until scala 2.13.
  */
object Using {

  def tryWithResources[T <: AutoCloseable, V](r: => T)(f: T => V): V = {
    val resource: T = r
    require(resource != null, "resource is null")
    var exception: Throwable = null
    try {
      f(resource)
    } catch {
      case NonFatal(e) =>
        exception = e
        throw e
    } finally {
      closeAndAddSuppressed(exception, resource)
    }
  }

  def closeAndAddSuppressed(e: Throwable, resource: AutoCloseable): Unit = {
    if (e != null) {
      try {
        resource.close()
      } catch {
        case NonFatal(suppressed) =>
          e.addSuppressed(suppressed)
      }
    } else {
      resource.close()
    }
  }

  def tryWithResourcesCustomized[T <: AutoClosableCustomized, V](r: => T)(f: T => V): V = {
    val resource: T = r
    require(resource != null, "resource is null")
    var exception: Throwable = null
    try {
      f(resource)
    } catch {
      case NonFatal(e) =>
        exception = e
        throw e
    } finally {
      closeAndAddSuppressedCustomized(exception, resource)
    }
  }

  def closeAndAddSuppressedCustomized(e: Throwable, resource: AutoClosableCustomized): Unit = {
    if (e != null) {
      try {
        resource.customClose()
      } catch {
        case NonFatal(suppressed) =>
          e.addSuppressed(suppressed)
      }
    } else {
      resource.customClose()
    }
  }
}

trait AutoClosableCustomized extends AutoCloseable {
  def customClose()
}
