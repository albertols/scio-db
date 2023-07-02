package com.db.myproject.slack

import com.db.pwcclakees.model.tables.KW111T.KW111TF
import com.spotify.scio.bigquery.{bigQueryScioContextOps, BigQueryType, Table}
import com.spotify.scio.pubsub.PubsubIO
import com.spotify.scio.streaming.{DISCARDING_FIRED_PANES}
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.apache.beam.sdk.transforms.windowing._
import org.checkerframework.checker.nullness.qual.Nullable
import org.slf4j.{Logger, LoggerFactory}

object MinimalPubSubBQLookup {
  val log: Logger = LoggerFactory getLogger getClass.getName

  // Massive inserts
  // bq load --autodetect --source_format=CSV --skip_leading_rows=0 db-dev-apyd-pwcclake-es.CDH_dataset.scio_test gs://europe-west3-db-dev-pwcclak-3c7ede6b-bucket/dags/big.sql
  @BigQueryType.toTable
  case class Row(key: String, opt: Option[String])

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    implicit val scioContext: ScioContext = sc
    val bqTable: String = ???
    val pubsubTopic: String = ???

    val bigSide_1 = sc
      .typedBigQuery[Row](Table.Spec(bqTable))
      .withFixedWindows(org.joda.time.Duration.standardDays(1))
      .withGlobalWindow()
    bigSide_1.take(5).map(k => log.info(s"bside=>$k"))
    val bigSide = bigSide_1.map(row => row.key -> (row.key, row.opt)).asMapSideInput

    val myRtRows = sc
      .read(PubsubIO.string(pubsubTopic))(PubsubIO.ReadParam(PubsubIO.Subscription))
      .withFixedWindows(
        org.joda.time.Duration.standardSeconds(1),
        // .withGlobalWindow( // Caused by: java.lang.IllegalArgumentException: Attempted to get side input window for GlobalWindow from non-global WindowFn
        options = com.spotify.scio.values.WindowOptions(
          allowedLateness = org.joda.time.Duration.ZERO,
          trigger = Repeatedly.forever(
            AfterFirst.of(
              AfterPane.elementCountAtLeast(1),
              AfterProcessingTime.pastFirstElementInPane()
            )
          ),
          accumulationMode = DISCARDING_FIRED_PANES
        )
      )
      .withSideInputs(bigSide)
      .map((key, bbside) => key -> bbside(bigSide).get(key))
      .toSCollection
    myRtRows.map(k => log.info(s"join=>$k"))
    sc.run()
  }
}
