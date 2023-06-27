package com.db.myproject.avro

import com.db.myproject.avro.MyLookUpTable.MyLookUpTableRows
import com.spotify.scio.streaming.DISCARDING_FIRED_PANES
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior
import org.apache.beam.sdk.transforms.windowing.{AfterPane, Repeatedly}

/** WATCH OUT: INTERIM object for integration test quickly Windowing for BigBoard */
object BBWindows {

  def bbFixedWindow(bb: SCollection[MyLookUpTableRows]) = {
    bb
     // .withFixedWindows(
        //duration = org.joda.time.Duration.standardDays(1)
        // )
//        ,
//        options = com.spotify.scio.values.WindowOptions(
//          allowedLateness = org.joda.time.Duration.ZERO,
          //        //allowedLateness = org.joda.time.Duration.standardDays(1),
          //              trigger = Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()),
//          trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
//          accumulationMode = DISCARDING_FIRED_PANES,
//          closingBehavior = ClosingBehavior.FIRE_ALWAYS
          //               timestampCombiner = TimestampCombiner.EARLIEST
//        )
      //)
  }

  def bbGlobalWindow(bb: SCollection[MyLookUpTableRows]) =
    bb
      .withFixedWindows(org.joda.time.Duration.standardDays(1))
//      .withGlobalWindow(
//        com.spotify.scio.values.WindowOptions(
//                         allowedLateness = org.joda.time.Duration.ZERO,
    //        //allowedLateness = org.joda.time.Duration.standardDays(1),
    //              trigger = Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()),
    //         //trigger=Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
    //                accumulationMode = DISCARDING_FIRED_PANES,
    //                closingBehavior = ClosingBehavior.FIRE_ALWAYS,
    //               timestampCombiner = TimestampCombiner.EARLIEST
//        )
//      ) // Single global window: is bounded (the size is fixed), all the elements to a single global window.

}
