package com.db.myproject.async

import com.db.myproject.async.configs.MediationConfig
import com.db.myproject.async.http.StateAsyncParDoWithAkka
import com.db.myproject.async.model.BusinessEvent.BusinessEventCirce
import com.db.myproject.async.nhub.model.HttpResponse.HttpResponse
import com.db.myproject.model.parsers.CDHJsonParserCirce
import com.db.myproject.utils.RootPureConfig
import com.db.myproject.utils.RootPureConfig.applyFromFile
import com.db.myproject.utils.pubsub.PubSubConsumer
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.slf4j.{Logger, LoggerFactory}

import org.apache.beam.sdk.transforms.windowing.{AfterProcessingTime, Repeatedly, TimestampCombiner}
import org.apache.beam.sdk.values.KV
import org.slf4j.{Logger, LoggerFactory}
import pureconfig.generic.auto._

object SCIOAsyncService {
  implicit val configReader = pureconfig.ConfigReader[MediationConfig]
  lazy val mediationConfig: MediationConfig = applyFromFile(env, "mediation/application.conf")
  val log: Logger = LoggerFactory getLogger getClass.getName
  var env: String = "dev"

  def windowOptions = com.spotify.scio.values.WindowOptions(
    allowedLateness = org.joda.time.Duration.ZERO,
    trigger = Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()),
    accumulationMode = DISCARDING_FIRED_PANES,
    timestampCombiner = TimestampCombiner.LATEST
  )

  def main(cmdlineArgs: Array[String]): Unit = {
    try {
      log.info("Init SCIO Context...")
      val (sc, args) = ContextAndArgs(cmdlineArgs)
      implicit val scioContext: ScioContext = sc

      // --input args
      log.info(s"SCIO args=$args")
      this.env = args.getOrElse("env", "dev")
      val config = readConfig
      val pubSubs: String = args.getOrElse("pubsub-sub", mediationConfig.mediation.pubsubSub)
      val streamingWindow: Long = args.getOrElse("stream-window", mediationConfig.mediation.berWindow).toLong
      val applyWindow: Boolean = args.getOrElse("apply-window", "true").toBoolean
      val windowType: String = args.getOrElse("window-type", "global")
      val stateType: StateType.Value = StateType.mapToStateType(args.getOrElse("state-type", "stateasync"))
      log.info(s"mediationConfig=$mediationConfig")

      // raw messages (with Window)
      val rawMsgs = PubSubConsumer.getMessagesFromPubSubScio(pubSubs)
      val winrawMsgs = rawMsgs.withGlobalWindow(windowOptions)

      // parsing BER
      val (bers: SCollection[BusinessEventCirce], bqErrors) = new CDHJsonParserCirce[BusinessEventCirce].parseJSONStrings(winrawMsgs)
      val distinctBers = bers
        .map(ber => (idempotentBer(ber), ber)).distinctByKey
        .map(ber => KV.of(ber._1, ber._2)) // distinct does not work here, due to: ParDo requires its input to use KvCoder in order to use state and timers
        .withGlobalWindow() // for keeping State among elements

      // Key & State => HttpResponse
      distinctBers.map { ber => log.info(s"**** input_ber=${ber.getKey}") }
      distinctBers.applyTransform(ParDo.of(new StateAsyncParDoWithAkka(mediationConfig))).map { m =>
        log.info(s"ber=${m.getKey}")
        log.info(s"NHUB=${m.getValue}")
      }

      sc.run()
    } catch {
      case e: Exception => log.error("Mediation exception", e)
    }
  }

  def idempotentBer(ber: BusinessEventCirce) = s"${ber.event.transactionId}-${ber.customer.id}"

  // application.conf from GCS or resources/
  def readConfig = com.db.pwcclakees.utils.gcp.GCSCommonUtils.rawStringConfigFromGcs(
    mediationConfig.gcp.project,
    mediationConfig.gcsBucket,
    mediationConfig.mediation.configBlobPath
  ) match {
    case Some(conf) => RootPureConfig.readConfigFromString(conf, env)
    case None => mediationConfig
  }

  object StateType extends Enumeration {
    val STATE_ASYNC, STATE_CACHE = Value

    def mapToStateType(deviceType: String): StateType.Value = {
      deviceType.replaceAll("[^a-zA-Z0-9]", "").toLowerCase.toLowerCase match {
        case "stateasync" => StateType.STATE_ASYNC
        case "state" => StateType.STATE_CACHE
        case _ => throw new IllegalArgumentException("Invalid StateType")
      }
    }
  }

}
