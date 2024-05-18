package com.db.myproject.utils.time

import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat, ISOPeriodFormat}
import org.joda.time.{DateTime, Duration, Instant, format}
import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util.Date
import scala.util.{Failure, Success, Try}

object TimeUtils {
  val log: Logger = LoggerFactory getLogger getClass.getName

  val DATE: String = "yyyy-MM-dd"
  val DATE_NO_SEP: String = "yyyyMMdd"
  val DATETIME: String = "yyyy-MM-dd HH:mm:ss"
  val ISO_DATETIME: String = "yyyy-MM-dd'T'HH:mm:ss.SSS"
  val FILE_DATETIME: String = "yyyyMMddHHmmss"
  val HOUR_MINUTE_SECOND = "HH:mm:ss"

  /**
   * Common Date and time pattern from host (E.g: ENTRY_TS) or TIMESTAMP_CDC (IIDR_CDC)
   */
  val nanoSecondsDateTimeFormatter: format.DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSSSS").withZoneUTC

  def jodaNowGetMillis: Long = Instant.now().getMillis

  /**
   * @param startInstant ,
   *                     2023-03-13T12:54:30.000Z
   * @param endInstant   ,
   *                     2023-03-13T12:55:00.000Z
   * @return
   * String 20230313T125430_125500
   */
  def getShardingStartToEndInstant(startInstant: Instant, endInstant: Instant): String = {
    val dateTimeFormatter = ISODateTimeFormat.dateHourMinuteSecond
    val endTimeFormatter = ISODateTimeFormat.hourMinuteSecond
    Try(
      "%s_%s"
        .format(
          dateTimeFormatter.print(startInstant),
          endTimeFormatter.print(endInstant)
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
   * TODO: task for unit testing
   *
   * @param field ,
   *              String 13/03/2023 12:54:30 or 2023-03-13T12:54:30.000000000000
   * @return
   * Instant 2023-03-13T12:54:30.000Z
   */
  def parseStringToInstant(field: String): Instant = {
    val maybeInstant = Array(
      "yyyy-MM-dd HH:mm:ss.SSSSSS 'UTC'",
      "dd/MM/yyyy HH:mm:ss",
      "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSSSS",
      "yyyy-MM-dd'T'HH:mm:ss.SSS",
      "yyyy-MM-dd HH:mm:ss.SSS",
      "yyyy-MM-dd")
      .map(format => DateTimeFormat.forPattern(format).withZoneUTC)
      .map(formatter => Try(DateTime.parse(field, formatter).toInstant))
      .find(_.isSuccess).getOrElse(Try(DateTime.parse(field).toInstant))

    maybeInstant match {
      case Success(instant) => instant
      case Failure(e) => throw e
    }

  }

  def isoDurationToNextDateTime(isoDuration: String): Either[String, (DateTime, org.joda.time.Duration)] = {
    try {
      val period = ISOPeriodFormat.standard().parsePeriod(isoDuration)
      val now = org.joda.time.DateTime.now()
      // When trying to convert a Period to a Duration, if the period contains months or years,
      // there would be an exception because their lengths might vary. Using `now.plus` approach instead.
      val endDateTime = now.plus(period)

      val startDateTime = if (period.getYears > 0) {
        now.withDayOfYear(1).withTimeAtStartOfDay() // start of current year
      } else if (period.getMonths > 0) {
        now.withDayOfMonth(1).withTimeAtStartOfDay() // start of current month
      } else if (period.getWeeks > 0) {
        now.dayOfWeek().withMinimumValue().withTimeAtStartOfDay() // start of current week
      } else if (period.getDays > 0) {
        now.withTimeAtStartOfDay() // start of current day
      } else if (period.getHours > 0) {
        now.hourOfDay().roundFloorCopy() // start of current hour
      } else {
        now.minuteOfHour().roundFloorCopy() // start of current minute
      }

      val parsedDuration = new Duration(now, endDateTime)
      Right((startDateTime, parsedDuration ))
    } catch {
      case _: IllegalArgumentException => Left(s"Invalid ISO 8601 duration format $isoDuration")
    }
  }

  def filterWeekendsByCalendar: DateTime => Boolean = date =>
    (date.getDayOfWeek, date.getMonthOfYear) match {
      case (7, _) => false //filter every sunday
      case (6, 5 | 6 | 7 | 8 | 9) => false //filter saturday when is May, June, July, August or September
      case _ => true
    }

  def getNowISOTimestamp(): String = {
    val now = ZonedDateTime.ofInstant(java.time.Instant.now(), ZoneId.systemDefault())
    now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
  }

  def formatTime(ms: Long): String = {
    val seconds = ms / 1000
    val hours = seconds / 3600
    val minutes = (seconds % 3600) / 60
    val remainingSeconds = seconds % 60

    val hoursStr = if (hours > 0) s"${hours}h " else ""
    val minutesStr = if (minutes > 0) s"${minutes}m " else ""
    val secondsStr = if (remainingSeconds > 0) s"${remainingSeconds}sec" else ""

    hoursStr + minutesStr + secondsStr
  }

  def convertTimestampToLegible(timestamp: Long, sdf: SimpleDateFormat): String = {
    val date: Date = new Date(timestamp)
    sdf.format(date)
  }

  def addTodayDateInPath(path: String, partitionDateName: String, dateFormatPattern: String): String = {
    val updatedPath = path + partitionDateName + "=" + DateTime.now().toString(DateTimeFormat.forPattern(dateFormatPattern))
    log.info("Storing at path:" + updatedPath)
    updatedPath
  }
}
