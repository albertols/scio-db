package com.db.myproject.mediation.http.clients.akka

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.util.ByteString
import com.db.myproject.mediation.MediationService.MySslConfig
import com.db.myproject.mediation.avro.MyEventRecordUtils.newBerWithLastNHubTimestamp
import com.db.myproject.mediation.configs.absoluteURL
import com.db.myproject.mediation.http.StateAndTimerType.{FutureKVOutputBerAndHttpResponse, InputBer, OutputBer}
import com.db.myproject.mediation.http.clients.AbstractHttpClient
import com.db.myproject.mediation.nhub.NhubFactory.getRequest
import com.db.myproject.mediation.nhub.model.MyHttpResponse.NotificationResponse
import com.db.myproject.mediation.nhub.model.MyHttpRequest
import com.db.myproject.utils.time.TimeUtils.jodaNowGetMillis
import org.apache.beam.sdk.values.KV
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class AkkaHttpClient extends AbstractHttpClient {

  import AkkaHttpClient._
  override def sendPushWithFutureResponse(record: OutputBer): FutureKVOutputBerAndHttpResponse = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    val beforeNhubTs = jodaNowGetMillis
    sendPush(record)
      .flatMap {
        case (Success(response), _) =>
          log.info(s"Unmarshalling $response")
          log.info(s"\tentity=${response.entity}")
          Unmarshal(response.entity).to[NotificationResponse]
        case (Failure(ex), _) =>
          Future.failed(new Exception(s"YOUR_ENDPOINT Request failed with exception: $ex"))
      }
      .map(k => KV.of(newBerWithLastNHubTimestamp(record, beforeNhubTs), k))
  }
}

object AkkaHttpClient extends Serializable {

  import com.db.myproject.mediation.MediationService.{domain, mediationConfig, url}

  private val log: Logger = LoggerFactory.getLogger(getClass.getName)
  implicit val sslConfig = MySslConfig(mediationConfig.mediation.sslConfigPath, mediationConfig.gcp.project).sslConfig
  val httpsConnectionContext = AkkaSSLContextFromSecretManager.httpsConnectionContext

  lazy val poolClientFlow = poolClient
    .initialTimeout(FiniteDuration(30L, TimeUnit.SECONDS))
    .completionTimeout(FiniteDuration(60L, TimeUnit.SECONDS))
    .buffer(2000, OverflowStrategy.backpressure)

  lazy val poolClient: Flow[(HttpRequest, Any), (Try[HttpResponse], Any), Any] =
    if (url.contains("https")) {
      Http().cachedHostConnectionPoolHttps(domain, connectionContext = httpsConnectionContext)
    } else Http().cachedHostConnectionPool(domain)

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  def sendPush(record: InputBer) =
      getResponseFuture(
        getRequest(record, mediationConfig)
          .asInstanceOf[MyHttpRequest.HttpRequest]
          .toJson
          .toString,
        absoluteURL(mediationConfig)
      )

  def getResponseFuture(reqRawString: String, url: String): Future[(Try[HttpResponse], Any)] = {
    log.info(s"reqRawString=$reqRawString")
    val entity = HttpEntity(ContentTypes.`application/json`, ByteString(reqRawString))
    log.info(s"HttpEntity=$entity")
    val httpRequest = HttpRequest(
      method = HttpMethods.POST,
      uri = url,
      entity = entity
    )
    log.info(s"httpRequest=${httpRequest}")
      Source
        .single(httpRequest -> ())
        .via(AkkaHttpClient.poolClientFlow)
        .runWith(Sink.head)
  }

}
