package com.db.myproject.slack

import com.spotify.scio.bigquery.{BigQueryType, Table, bigQueryScioContextOps}
import com.spotify.scio.pubsub.PubsubIO
import com.spotify.scio.streaming.DISCARDING_FIRED_PANES
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.apache.beam.sdk.transforms.windowing._
import org.slf4j.{Logger, LoggerFactory}

/**
 * background:
 * 1) main PubSub joins PubSub Side input with FixedWindows: https://spotify.github.io/scio/examples/RefreshingSideInputExample.scala.html
 * 2) GlobalWindow SideInput for lookups: https://beam.apache.org/documentation/patterns/side-inputs/#slowly-updating-global-window-side-inputs
 */
object MinimalPubSubBQLookup {
  val log: Logger = LoggerFactory getLogger getClass.getName

  // Massive inserts
  // bq load --autodetect --source_format=CSV --skip_leading_rows=0 project.dataset.scio_test gs://my_bucket/massive.sql
  @BigQueryType.toTable
  case class Row(key: String, opt: Option[String])

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    implicit val scioContext: ScioContext = sc
    val bqTable: String = ???
    val pubsubTopic: String = ???

    val bigSide_1 = bigSideWindow("fixed", sc.typedBigQuery[Row](Table.Spec(bqTable)))
    bigSide_1.take(5).map(k => log.info(s"bigSide=>$k"))
    val bigSide = bigSide_1.map { row => row.key -> (row.key, row.opt) }
      //.asMapSideInput
      .asMapSingletonSideInput

    val myRtRows = mainInputWindow("fixed", sc.read(PubsubIO.string(pubsubTopic))(PubsubIO.ReadParam(PubsubIO.Subscription)))
      .withSideInputs(bigSide)
      .map { (key, bbside) => key -> bbside(bigSide).get(key) }
      .toSCollection
    myRtRows.map(k => log.info(s"join=>$k"))

    sc.run()
  }

  def bigSideWindow(winStrategy: String = "fixed", bigSide: SCollection[Row]) =
    winStrategy match {
      case "fixed" => bigSide.withFixedWindows(org.joda.time.Duration.standardDays(1))
      case _ => bigSide // global
    }

  def mainInputWindow(winStrategy: String = "fixed", bigSide: SCollection[String]) = {
    val opt = com.spotify.scio.values.WindowOptions(
      allowedLateness = org.joda.time.Duration.ZERO,
      trigger = Repeatedly.forever(
        AfterFirst.of(
          AfterPane.elementCountAtLeast(1),
          AfterProcessingTime.pastFirstElementInPane()
        )
      ),
      accumulationMode = DISCARDING_FIRED_PANES
    )
    winStrategy match {
      case "fixed" => bigSide.withFixedWindows(org.joda.time.Duration.standardSeconds(10), options = opt)
      case "global" => bigSide.withGlobalWindow(options = opt)
      case _ => bigSide // global
    }
  }
}
