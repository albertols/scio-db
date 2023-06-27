package com.db.myproject.avro

import com.db.myproject.avro.MyAvroRecordProducer.myRtRecordToAvro
import com.db.myproject.avro.MyLookUpTable.MyLookUpTableRows
import com.db.myproject.model.tables.MyRtTable
import com.db.myproject.model.tables.MyRtTable.MyRtRecords
import com.db.myproject.utils.TimeUtils.windowStartAndEndFormatString
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.streaming.DISCARDING_FIRED_PANES
import com.spotify.scio.values.{SCollection, WindowedSCollection}
import org.apache.beam.sdk.transforms.windowing._
import org.slf4j.{Logger, LoggerFactory}

object BTRWindows {
  val log: Logger = LoggerFactory getLogger getClass.getName

  /**
   * Group messages in windows using Beam.
   *
   * @param ps
   *   SCollection of messages to be grouped in windows
   * @return
   *   SCollection of tuples, with the window and an iterable over the messages
   */
  def windowIn[T](windowSize: Long, ps: SCollection[T]): SCollection[(IntervalWindow, Iterable[T])] =
    ps
      .withFixedWindows(org.joda.time.Duration.standardSeconds(windowSize))
      .withWindow[IntervalWindow]
      .swap
      .groupByKey

  def applyWindowToPart[T](name: String, in: SCollection[T], duration: org.joda.time.Duration): SCollection[T] = {
    in
      .withFixedWindows(duration)
      .withGlobalWindow( // not events for SideInput without trigger
        com.spotify.scio.values.WindowOptions(
          //timestampCombiner  = TimestampCombiner.END_OF_WINDOW,
          allowedLateness = org.joda.time.Duration.ZERO,
          //             trigger = Repeatedly.forever(AfterWatermark.pastEndOfWindow()), // NOT triggered
          //             trigger= Repeatedly.forever(AfterFirst.of( // OK: at first
          //               AfterPane.elementCountAtLeast(1),
          //               AfterProcessingTime.pastFirstElementInPane().plusDelayOf(org.joda.time.Duration.standardSeconds(1)))),
          //            trigger = Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()), // OK: at first
          // trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)), // OK: at first + 1
          trigger = Repeatedly.forever(
            AfterFirst.of( // OK: at first
              AfterPane.elementCountAtLeast(1),
              AfterProcessingTime.pastFirstElementInPane()
            )
          ),
          accumulationMode = DISCARDING_FIRED_PANES
          // PCollection with more than one element accessed as a singleton view. Consider using Combine.globally().asSingleton()
          // accumulationMode = ACCUMULATING_FIRED_PANES
        )
      )
      .withName(name)
  }

  def keyAndkw111(windowedRecords: SCollection[MyRtRecords], sc: ScioContext) = {
    val keyedKw111 = windowedRecords
      .map { row =>
        val account_no_1 = row.ACCOUNT_NO_1
        val bank_id = row.BANK_ID
        val branch_no_main = row.BRANCH_NO_MAIN
        (account_no_1.toString + bank_id.toString + branch_no_main.toString, row)
      }.withName("Keyed MyRtRecord")
    keyedKw111.map(k => log.info(s"sided_kw111=${k}"))
    keyedKw111
  }

  // HashJoin(big_left (big_board), small_right = Side Input Copy (MyRtRecord))
  def btrHashJoin(
                   windowedRecords: SCollection[MyRtRecords],
                   keyedBigBoard: SCollection[(String, MyLookUpTableRows)],
                   // keyedBigBoard: SCollection[(String, Iterable[BigBoardParquetRow])],
                   sc: ScioContext
  )(implicit c: Coder[MyRtRecords]) = {
    val keyedKw111 = keyAndkw111(windowedRecords, sc)
    keyedBigBoard.take(5).map(k => log.info(s"keyed_bb=$k"))
    val btrs =
      keyedBigBoard
        .hashJoin(keyedKw111)
        .map { btr =>
          val (key, (bb_part, kw111_part)) = btr
          log.info(s"key=$key, bb_part=$bb_part, kw111_part=$kw111_part")
          kw111_part
        }.withName("MyRtRecord with BJH")
        .map(myRtRecordToAvro(_))
    btrs
  }
  def btrHashJoin2(
                    windowedRecords: SCollection[MyRtRecords],
                    //    keyedBigBoard: SCollection[(String, BigBoardParquetRow)],
                    // keyedBigBoard: SCollection[(String, Iterable[BigBoardParquetRow])],
                    keyedBigBoard: WindowedSCollection[(String, MyLookUpTableRows)],
                    sc: ScioContext
  )(implicit c: Coder[MyRtRecords]) = {
    val keyedKw111 = keyAndkw111(windowedRecords, sc)
    //keyedBigBoard.map{
    //  //k=>log.info(s"[KBB]maxTs=${k.window.maxTimestamp()},pane=${k.pane},win=${k.window},timestamp=${k.timestamp}, ")
    //    k=>k
    //}
    val bb = keyedBigBoard.toSCollection
    bb.take(5).map(k => log.info(s"keyed_bb=$k"))
    val btrs =
      bb
        .hashJoin(keyedKw111)
        .map { btr =>
          val (key, (bb_part, kw111_part)) = btr
          log.info(s"key=$key, bb_part=$bb_part, kw111_part=$kw111_part")
          kw111_part
        }.withName("MyRtRecord with BJH2")
        .map(myRtRecordToAvro(_))
    btrs
  }

  // BigBoard is cached in each worker as SideInput
  def bigBoardAsSideInputJoin(
                               windowedRecords: SCollection[MyRtRecords],
                               keyedBigBoard: SCollection[(String, MyLookUpTableRows)],
                               // keyedBigBoard: SCollection[(String, Iterable[BigBoardParquetRow])],
                               // keyedBigBoard: WindowedSCollection[(String, BigBoardParquetRow)],
                               sc: ScioContext
  )(implicit c: Coder[MyRtRecords]) = {
    val keyedKw111 = keyAndkw111(windowedRecords, sc)
    val bigSide = keyedBigBoard.asMapSideInput
    val btrs = keyedKw111
      .withSideInputs(bigSide)
      .map { (kw111, bbside) =>
        val (key, trans) = kw111
        val side2side = bbside(bigSide)
        val client = side2side.get(key)
        //        val client = side2side.get(key).getOrElse()
        log.info(s"key=$key, bb_part=$client, kw111_part=$trans")
        trans
      }.withName("MyRtRecord with bbside")
      .toSCollection
      .map(myRtRecordToAvro(_)).withName("MyRtRecord Mapping")
    btrs
  }

  def transformWithWindow[T](windowSize: Long, messages: SCollection[T]): SCollection[(IntervalWindow, Iterable[T])] = {
    val groups: SCollection[(IntervalWindow, Iterable[T])] =
      messages.transform("Windowing BTRSink")(windowIn(windowSize, _))
    groups
  }

}
