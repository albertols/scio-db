package com.db.myproject.utils

import org.slf4j.{Logger, LoggerFactory}

import java.io.InputStream
import java.net.URLClassLoader
import java.util.jar.JarFile
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object ResourceReader {
  val log: Logger = LoggerFactory getLogger getClass.getName

  def readResource(resourceName: String): InputStream =
    getClass.getClassLoader.getResourceAsStream(resourceName)

  def getStringFromLocalResource(resourceName: String): String = {
    val resource = scala.io.Source.fromResource(resourceName)
    resource.getLines.mkString("\n")
  }

  def listAllResources(jarNames: List[String], dirName: String): Option[List[String]] = {
    log.info(s"from jarName=$jarNames, listing dirName=$dirName... ")
    val jarURLs = jarNames.map(jarName => new java.net.URL(s"jar:file:$jarName!/")).toArray
    val jar = singleJarFromName(jarURLs, dirName) match {
      case Some(jar) => jar
      case None      => throw new Exception(s"Unknown jarName=$jarNames with dirName=$dirName")
    }

    val fileNames = jar
      .entries()
      .asScala
      .toList
      .filter(!_.isDirectory)
      .filter(_.getName.contains(dirName))
      .map(_.getName)
    jar.close
    Some(fileNames)
  }

  def fileFromRresourceJarPath(file: String) = file.substring(file.lastIndexOf("file:") + 5, file.lastIndexOf('!'))

  def singleJarFromName(jarURLs: Array[java.net.URL], dirName: String): Option[JarFile] = {
    val jarList = Try(
      new URLClassLoader(jarURLs)
        .getResources(dirName)
        .asScala
        .map { f =>
          log.info(s"resource (from classLoader)=$f")
          val myFile = fileFromRresourceJarPath(f.getFile)
          log.info(s"resource (from classLoader)(trimmed)=$myFile")
          new JarFile(myFile)
        }
        .toList
    ) match {
      case Success(jar) => jar
      case Failure(ex) =>
        throw new Exception(s"Unknown URLClassLoader with resources/ dirName=$dirName", ex)
    }
    jarList.size match {
      case size if size > 0 => Some(jarList(0))
      case _                => None
    }
  }

  def getLines(resourceStream: InputStream) =
    scala.io.Source.fromInputStream(resourceStream).getLines().mkString("\n")

  def close(is: InputStream) = is.close
}
