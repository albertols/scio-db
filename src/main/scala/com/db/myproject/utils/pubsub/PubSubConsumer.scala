package com.db.myproject.utils.pubsub

import com.spotify.scio.pubsub.PubsubIO
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.slf4j.{Logger, LoggerFactory}

/** --runner=DirectRunner --pubsub-sub=projects/db-dev-apyd-pwcclake-es/subscriptions/pc_kw111t-sub */
object PubSubConsumer {
  val log: Logger = LoggerFactory getLogger getClass.getName

  def getMessagesFromPubSubScio(pubsubTopic: String)(implicit sc: ScioContext): SCollection[String] = {
    log.info("Reading Messages from PubSub (Scio)...")
    val pubsubIO: PubsubIO[String] = PubsubIO.string(pubsubTopic /*, timestampAttribute = "ts"*/ )
    val pubsubParams: PubsubIO.ReadParam = PubsubIO.ReadParam(PubsubIO.Subscription)
    /*_*/
    sc.read(pubsubIO)(pubsubParams).withName(s"Records from $pubsubTopic")
  }

  def getMessagesFromPubSub(pubsubTopic: String) = {
    log.info("Reading Messages from PubSub...")
    import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
    PubsubIO.readStrings().fromSubscription(pubsubTopic)
  }

}
