package com.db.myproject.streaming.utils.pubsub

import com.db.myproject.mediation.avro.MyEventRecord
import com.db.myproject.mediation.avro.MyEventRecordUtils.{mockTransactionId, newBerWithLastNHubTimestamp}
import com.db.myproject.utils.time.TimeUtils.{getNowISOTimestamp, jodaNowGetMillis}
import com.spotify.scio.avro.avroScioContextOps
import com.spotify.scio.coders.kryo.fallback
import com.spotify.scio.pubsub.PubsubIO
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.apache.avro.specific.SpecificRecord
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.ClassTag

/**
 * for mocking records in pubsub
 */
object PubSubProducer {
  val log: Logger = LoggerFactory getLogger getClass.getName

  def main(cmdlineArgs: Array[String]): Unit = {
    log.info("Init SCIO Context...")
    val (scioContext, args) = ContextAndArgs(cmdlineArgs)
    implicit val sc: ScioContext = scioContext

    val recordType: String = args.getOrElse("record-type", "avros")
    val pubsubTopic: String = args.getOrElse("pubsub-topic", "projects/mypro/topics/mytopic")
    val mockFile: String = args.getOrElse("inputFile", "src/main/resources/model/json/kw111t.json")
    val sinkRaw: Boolean = args.getOrElse("sink-raw", "false").toBoolean
    val numberOfMockedRecords: Int = args.getOrElse("mocks-number", "1").toInt
    val takeAll: Boolean = args.getOrElse("take-all", "true").toBoolean
    val out = recordType.toLowerCase match {
      case "avros" => val inputSCollection = if (!takeAll) sc.avroFile[MyEventRecord](s"$mockFile*").take(numberOfMockedRecords)
      else sc.avroFile[MyEventRecord](s"$mockFile*")
        val mockedBers = inputSCollection.map { record =>
          if (sinkRaw) record
          else {
            newBerWithLastNHubTimestamp(mockTransactionId(record), jodaNowGetMillis)
          }
        }
        PubSubProducer.writeGenericAvroScio[MyEventRecord](mockedBers, pubsubTopic)
        mockedBers
      case _ => throw new IllegalAccessException(s"Incorrect recordType=$recordType")
    }
    out.debug()
    sc.run()
    log.info(s"exit_time=$getNowISOTimestamp")
  }

  def writeGenericAvroScio[T <: SpecificRecord](in: SCollection[T],
                                                pubsub: String
                                               )(implicit ct: ClassTag[T], sc: ScioContext) = {
    val pubsubIO: PubsubIO[T] = PubsubIO.avro[T](pubsub)
    in.withName(s"Writing Avro $pubsub").write(pubsubIO)(PubsubIO.WriteParam())
  }

}
