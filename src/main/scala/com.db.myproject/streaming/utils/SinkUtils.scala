package com.db.myproject.streaming.utils

import com.db.myproject.utils.time.TimeUtils.getShardingStartToEndInstant
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import org.apache.avro.specific.SpecificRecord
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.util.MimeTypes
import org.slf4j.{Logger, LoggerFactory}

import java.io.OutputStream
import java.nio.channels.Channels
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

object SinkUtils {
  val log: Logger = LoggerFactory getLogger getClass.getName

  /**
   * Loading Avro into GCS ,from KafkaIO or unbounded PCollection prodcues : "java.lang.IllegalArgumentException: Must
   * use windowed writes when applying WriteFiles to an unbounded PCollection"
   *
   * @param records
   * , we should now have a collection of objects, grouped in windows. We will write each window in a separate file in
   * GCS, encoded as Avro.
   * @param location
   * @param sc
   * @param c
   * @tparam T
   * @return
   */
  def sinkAvroInGCS[T](records: SCollection[(IntervalWindow, Iterable[T])], location: String)(implicit
                                                                                              c: Coder[T]
  ) = {
    records
      .map { case (w: IntervalWindow, msgs: Iterable[T]) =>
        val outputShard = location + getShardingStartToEndInstant(w.start, w.end) + ".avro"
        writeAvroToBlob(msgs, outputShard)
      }
      .withName("Sink Avro into GCS")
  }

  def writeAvroToBlob[T](msgs: Iterable[T], outputShard: String): Unit = {
    val resourceId: ResourceId = FileSystems.matchNewResource(outputShard, false)
    val out: OutputStream = Channels.newOutputStream(FileSystems.create(resourceId, MimeTypes.BINARY))
    log.debug(s"avroToGCS_count=${msgs.size}")
    com.db.myproject.utils.core.AvroUtils.avroToBytes(msgs) match {
      case Success(avroBytes) => out.write(avroBytes)
      case Failure(ex) => log.error(s"Failing converting avroToBytes for OutputStream", ex)
    }
    out.close()
  }

  def saveAsAvroFile[T <: SpecificRecord](rawMsgs: SCollection[T],
                                          gcsPath: String,
                                          outputFilenamePrefix: String,
                                          shardCount: Int,
                                          outputFilenameSuffix: String)
                                         (implicit ct: ClassTag[T]): ClosedTap[T] = {
    rawMsgs
      .saveAsAvroFile(
        path = gcsPath,
        suffix = outputFilenameSuffix,
        numShards = shardCount,
        prefix = outputFilenamePrefix
      )
  }

}
