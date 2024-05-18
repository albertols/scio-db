package com.db.myproject.utils

import org.joda.time.DateTime

/**
 * scala-utils would be its home
 */
object TimeUtilsInterim {

  def getLastDateDaysFrom (lastNoOfDays : Int) = {
    (0 until lastNoOfDays).map { offset =>
      DateTime.now.minusDays(offset).toString("yyyy/MM/dd")
    }
  }

}
