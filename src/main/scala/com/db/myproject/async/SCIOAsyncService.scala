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

object SCIOAsyncService{
  implicit val configReader = pureconfig.ConfigReader[MediationConfig]
  lazy val mediationConfig: MediationConfig = applyFromFile(env, "mediation/application.conf")
  val log: Logger = LoggerFactory getLogger getClass.getName
  var env: String = "dev"

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
      log.info(s"mediationConfig=$mediationConfig")

      // raw messages (with Window)
      val rawMsgs = PubSubConsumer.getMessagesFromPubSubScio(pubSubs)

      // parsing BER
      val (bers: SCollection[BusinessEventCirce], bqErrors) = new CDHJsonParserCirce[BusinessEventCirce].parseJSONStrings(rawMsgs)
      val keyedBer = bers.map(ber => KV.of(idempotentBer(ber), ber))

      // Key & State => HttpResponse
      keyedBer.applyTransform(ParDo.of(new StateAsyncParDoWithAkka(mediationConfig))).map { m =>
        log.info(s"ber=${m.getKey}")
        log.info(s"NHUB=${m.getValue}")
      }

      sc.run()
    } catch {
      case e: Exception => log.error("SCIOAsyncService exception", e)
    }
  }

  def idempotentBer(ber: BusinessEventCirce) = s"${ber.event.transactionId}-${ber.customer.id}"

  // application.conf from GCS or resources/
  def readConfig = com.db.myproject.utils.gcp.GCSCommonUtils.rawStringConfigFromGcs(
    mediationConfig.gcp.project,
    mediationConfig.gcsBucket,
    mediationConfig.async.configBlobPath
  ) match {
    case Some(conf) => RootPureConfig.readConfigFromString(conf, env)
    case None => mediationConfig
  }

}
