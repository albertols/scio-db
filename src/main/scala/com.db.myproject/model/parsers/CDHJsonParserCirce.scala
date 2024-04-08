package com.db.myproject.model.parsers

import com.db.myproject.model.parsers.CDHJsonParserCirce.JsonError
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import io.circe
import io.circe.Decoder
import io.circe.generic.decoding.DerivedDecoder
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser.decode
import org.joda.time.Instant
import org.slf4j.{Logger, LoggerFactory}
import shapeless.Lazy

/**
 * @param lazyDecoder,
 *   needed, otherwise throws a "could not find Lazy implicit value of type io.circe.generic.decoding.DerivedDecoder[T]
 *   implicit def defaultDecoder: Decoder[T] = deriveDecoder[T]"
 * @tparam T
 */
case class CDHJsonParserCirce[T](implicit val lazyDecoder: Lazy[DerivedDecoder[T]]) {
  val log: Logger = LoggerFactory getLogger getClass.getName
  implicit def defaultDecoder: Decoder[T] = deriveDecoder

  def parseJSONStrings(
    messages: SCollection[String]
  )(implicit coder: Coder[T]): (SCollection[T], SCollection[JsonError]) = {
    log.info("Parsing JSON Strings...")
    val jsons: SCollection[Either[String, T]] = messages.map { rawJson: String =>
      json2CaseClass(rawJson) match {
        /* HINT: DecodingFailure causes Mutation Exception. Thus circe.Error is only logged
          org.apache.beam.sdk.util.IllegalMutationException:
          PTransform map@{CDHJsonParserCirce.scala:34}:2/ParMultiDo(Anonymous) mutated value Left(DecodingFailure(
         */
        case Left(value) =>
          log.error(s"Wrong message for parsing json: ${value.toString} - $rawJson")
          Left(rawJson)
        case Right(value) => Right(value)
      }
    }.withName("Circe Mapping")
    val errorsEither +: kw111tEither +: Nil = jsons.partition(
      2,
      { json =>
        log.trace(s"myJson=$json")
        json match {
          case Left(_)  => 0
          case Right(_) => 1
        }
      }
    )
    val typedRows: SCollection[T] = kw111tEither.map(_.right.get).withName(s"OK Typed Class")
    val jsonErrors: SCollection[JsonError] = errorsEither.map(_.left.get).map(circeError2BQError).withName("KO Json")
    (typedRows, jsonErrors)
  }

  // String to Json using Circe
  def json2CaseClass(jsonStr: String): Either[circe.Error, T] =
    decode(jsonStr)

  def circeError2BQError(toxicRow: String): JsonError =
    JsonError(toxicRow, "Parsing error")
}

object CDHJsonParserCirce {
  @BigQueryType.toTable
  case class JsonError(msg: String, error: String, timestamp: Instant = Instant.now())

  @BigQueryType.toTable
  case class JobError(jobId: String, msg: String, error: String, timestamp: Instant = Instant.now())

}
