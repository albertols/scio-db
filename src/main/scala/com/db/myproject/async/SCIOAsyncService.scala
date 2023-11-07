package com.db.pwcclakees.mediation

import com.db.myproject.model.parsers.CDHJsonParserCirce
import com.db.myproject.utils.pubsub.PubSubConsumer
import com.db.pwcclakees.btr.BTRProducerAvro.applyWindowToSCollection
import com.db.pwcclakees.mediation.configs.MediationConfig
import com.db.pwcclakees.mediation.http.StateAsyncParDoWithAkka
import com.db.pwcclakees.mediation.http.StateParDoWithAkka.StateParDoWithAkka
import com.db.pwcclakees.mediation.model.BusinessEvent.BusinessEventCirce
import com.db.pwcclakees.model.parsers.CDHJsonParserCirce
import com.db.pwcclakees.streaming.utils.pubsub.PubSubConsumer
import com.db.pwcclakees.utils.RootPureConfig
import com.db.pwcclakees.utils.RootPureConfig.applyFromFile
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.slf4j.{Logger, LoggerFactory}

object SCIOAsyncService {
  implicit val configReader = pureconfig.ConfigReader[MediationConfig]
  lazy val mediationConfig: MediationConfig = applyFromFile(env, "mediation/application.conf")
  val log: Logger = LoggerFactory getLogger getClass.getName
  var env: String = ""

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
      val stateType: StateType.Value = StateType.mapToStateType(args.getOrElse("state-type", "stateasync"))

      // raw messages (with Window)
      val rawMsgs = PubSubConsumer.getMessagesFromPubSubScio(pubSubs)

      // parsing BER
      val (bers: SCollection[BusinessEventCirce], bqErrors) = new CDHJsonParserCirce[BusinessEventCirce].parseJSONStrings(rawMsgs)
      val windowedBers = bers

      //      windowedBers.count.map(c => log.info(s"Windowed ber_count=$c"))
      //      windowedBers.map(ber => log.info(s"ok_customer=${ber.customer}"))
      val keyedBer = windowedBers.map(ber => KV.of(s"${ber.event.transactionId}-${ber.customer.id}", ber))

      // Key & State => HttpResponse
      val res = stateType match {
        case StateType.STATE => keyedBer.applyTransform(ParDo.of(new StateParDoWithAkka()))
        case StateType.STATE_ASYNC => keyedBer.applyTransform(ParDo.of(new StateAsyncParDoWithAkka()))
      }

      res.map { m =>
        log.info(s"ber=${m.getKey}")
        log.info(s"NHUB=${m.getValue}")
      }

      //out.saveAsCustomOutput("new topic", org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.writeStrings().to(config.mediation.retryTopic))

      sc.run()
    } catch {
      case e: Exception => log.error("Mediation exception", e)
    }
  }

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
    val STATE_ASYNC, STATE = Value

    def mapToStateType(deviceType: String): StateType.Value = {
      deviceType.replaceAll("[^a-zA-Z0-9]", "").toLowerCase.toLowerCase match {
        case "stateasync" => StateType.STATE_ASYNC
        case "state" => StateType.STATE
        case _ => throw new IllegalArgumentException("Invalid device type")
      }
    }
  }

}
