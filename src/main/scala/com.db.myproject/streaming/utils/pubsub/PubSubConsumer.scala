package com.db.myproject.streaming.utils.pubsub

import com.db.myproject.mediation.avro.MyEventRecord
import com.spotify.scio.pubsub.PubsubIO
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.apache.avro.specific.SpecificRecord
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.ClassTag

/**
 * --runner=DirectRunner --pubsub-sub=projects/your_pro/subscriptions/your_sub
 */
object PubSubConsumer {
  val log: Logger = LoggerFactory getLogger getClass.getName

  def main(cmdlineArgs: Array[String]): Unit = {
    log.info("Init SCIO Context...")
    val (scioContext, args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scioContext

    // DEFAULT KW111T JSON
    val recordType: String = args.getOrElse("record-type", "avros")
    val pubsubSub: String = args.getOrElse("pubsub-sub", "projects/mypro/subscriptions/mysub")
    log.info(s"args=$args")

    val messages = recordType.toLowerCase match {
      case "jsons" => readStringsScio(pubsubSub,  PubsubIO.ReadParam(PubsubIO.Subscription))
      case "avros" => PubSubConsumer.readGenericAvroScio[MyEventRecord](pubsubSub, PubsubIO.ReadParam(PubsubIO.Subscription))
      case _ => throw new IllegalAccessException(s"Incorrect recordType=$recordType")
    }

    messages.debug()
    sc.run()
  }

  def readGenericAvroScio[T <: SpecificRecord](pubsub: String,
                                               pubsubParams: PubsubIO.ReadParam
                                              )(implicit ct: ClassTag[T], sc: ScioContext): SCollection[T] = {
    val pubsubIO: PubsubIO[T] = PubsubIO.avro[T](pubsub)
    sc.read(pubsubIO)(pubsubParams)
      .withName(s"AVRO records from $pubsub")
  }

  def readStringsScio(pubsub: String, pubsubParams: PubsubIO.ReadParam)(implicit sc: ScioContext): SCollection[String] = {
    log.info("Reading Messages from PubSub (Scio)...")
    // p.apply(PubsubIO.writeString().withTimestampAttribute("timestamp").to(topic)); // timestamp could be needed PubSubIO.attributes is added
    val pubsubIO: PubsubIO[String] = PubsubIO.string(pubsub /*, timestampAttribute = "timestamp"*/)
    /*_*/
    sc.read(pubsubIO)(pubsubParams).withName(s"Records from $pubsub")
  }

}
