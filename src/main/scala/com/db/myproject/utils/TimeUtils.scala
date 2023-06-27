package com.db.myproject.utils

import org.joda.time.{DateTime, Instant}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.slf4j.{Logger, LoggerFactory}

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import scala.util.{Failure, Success, Try}

object TimeUtils {
  val log: Logger = LoggerFactory getLogger getClass.getName

  /**
   * @param startInstant,
   *   2023-03-13T12:54:30.000Z
   * @param endInstant,
   *   2023-03-13T12:55:00.000Z
   * @return
   *   String 20230313T125430_125500
   */
  def windowStartAndEndFormatString(startInstant: Instant, endInstant: Instant) = {
    val DateTimeFormatter = ISODateTimeFormat.dateHourMinuteSecond
    val EndTimeFormatter = ISODateTimeFormat.hourMinuteSecond
    Try(
      "%s_%s"
        .format(
          DateTimeFormatter.print(startInstant),
          EndTimeFormatter.print(endInstant)
        )
        .replace(":", "")
        .replace("-", "")
    ) match {
      case Success(v) =>
        log.info(s"outputShard=$v")
        v
      case Failure(e) =>
        log.error("Parsing ", e)
        Instant.now().toString
    }
  }

  /**
   * @param field,
   *   String 13/03/2023 12:54:30 or 2023-03-13T12:54:30.000000000000
   * @return
   *   Instant 2023-03-13T12:54:30.000Z
   */
  def parseStringToInstant(field: String): Instant = {
    val firstFormatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss").withZoneUTC
    val secondFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSSSS").withZoneUTC

    Try(DateTime.parse(field, firstFormatter).toInstant).orElse(
      Try(DateTime.parse(field, secondFormatter).toInstant)
    ).getOrElse(
      DateTime.parse(field).toInstant
    )
  }

  def getNowISOTimestamp(): String = {
    val now = ZonedDateTime.ofInstant(java.time.Instant.now(), ZoneId.systemDefault())
    now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
  }

}
