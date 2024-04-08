package com.db.myproject.slack

import com.spotify.scio.bigquery.{BigQueryType, Table, bigQueryScioContextOps}
import com.spotify.scio.pubsub.PubsubIO
import com.spotify.scio.streaming.DISCARDING_FIRED_PANES
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.apache.beam.sdk.transforms.windowing._
import org.slf4j.{Logger, LoggerFactory}

/**
 * open repo: for scaling issues to SCIO and Beam
 * https://github.com/albertols/scio-db/blob/develop/src/main/scala/com/db/myproject/slack/MinimalPubSubBQLookup.scala
 *
 * Exception: using DataFlowRunner throws "java.lang.ClassCastException" when reading from avro GCS interim files (from BQ)
 * https://stackoverflow.com/questions/76267480/scio-dataflow-error-message-from-worker-java-lang-classcastexception-class-ca
 *
 * Background:
 * 1) main PubSub joins PubSub Side input with FixedWindows: https://spotify.github.io/scio/examples/RefreshingSideInputExample.scala.html
 * 2) GlobalWindow SideInput for lookups: https://beam.apache.org/documentation/patterns/side-inputs/#slowly-updating-global-window-side-inputs
 */
object MinimalPubSubBQLookup {
  val log: Logger = LoggerFactory getLogger getClass.getName

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    implicit val scioContext: ScioContext = sc
    val bqTable: String = args.getOrElse("bqTable", "db-dev-apyd-pwcclake-es.CDH_dataset.scio_test")
    val pubSubTopic: String = args.getOrElse("pubSubTopic", "projects/db-dev-apyd-pwcclake-es/subscriptions/pc_kw111t-sub")
    val windowLengthSideInput: Long = args.getOrElse("windowLengthSideInput", "86400").toLong // 1 day
    val windowStrategySideInput: String = args.getOrElse("windowStrategySideInput", "default")
    val windowLengthPubSub: Long = args.getOrElse("windowLengthPubSub", "10").toLong
    val windowStrategyPubSub: String = args.getOrElse("windowStrategyPubSub", "fixed")

    val windowedBigQuery = getCustomWindow(windowStrategySideInput, sc.typedBigQuery[Row](Table.Spec(bqTable)), org.joda.time.Duration.standardSeconds(windowLengthSideInput))
    windowedBigQuery.take(5).map(k => log.info(s"BQ lookup=>$k"))
    val keyedBigSide = windowedBigQuery.map(row => row.key -> (row.key, row.opt)).asMapSideInput
    // .asMapSingletonSideInput

    val myRtRows = getCustomWindow(windowStrategyPubSub, sc.read(PubsubIO.string(pubSubTopic))(PubsubIO.ReadParam(PubsubIO.Subscription)), org.joda.time.Duration.standardSeconds(windowLengthPubSub))
      .withSideInputs(keyedBigSide)
      .map((key, bbside) => key -> bbside(keyedBigSide).get(key))
      .toSCollection
    myRtRows.map(k => log.info(s"join=>$k"))

    sc.run()
  }

  def getCustomWindow[T](winStrategy: String, bigSide: SCollection[T], duration: org.joda.time.Duration) = {
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
      case "fixed" => bigSide.withFixedWindows(duration, options = opt)
      case "global" => bigSide.withGlobalWindow(options = opt)
      case _ => bigSide // global
    }
  }

  // Massive inserts to insert quickly key,opt entries for BQ table
  // bq load --autodetect --source_format=CSV --skip_leading_rows=0 db-dev-apyd-pwcclake-es.CDH_dataset.scio_test gs://europe-west3-db-dev-pwcclak-3c7ede6b-bucket/dags/big.sql
  @BigQueryType.toTable
  case class Row(key: String, opt: Option[String])
}
