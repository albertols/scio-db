package com.db.myproject.mediation.http.clients.akka

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy, ThrottleMode}
import akka.util.ByteString
import com.db.myproject.mediation.MediationService.MySslConfig
import com.db.myproject.mediation.avro.MyEventRecordUtils.newBerWithLastNHubTimestamp
import com.db.myproject.mediation.configs.absoluteURL
import com.db.myproject.mediation.http.StateAndTimerType.{FutureKVOutputBerAndHttpResponse, InputBer, OutputBer}
import com.db.myproject.mediation.http.clients.AbstractHttpClient
import com.db.myproject.mediation.notification.NotificationFactory.getRequest
import com.db.myproject.mediation.notification.model.MyHttpResponse.NotificationResponse
import com.db.myproject.mediation.notification.model.MyHttpRequest
import com.db.myproject.utils.time.TimeUtils.jodaNowGetMillis
import org.apache.beam.sdk.values.KV
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class AkkaHttpClient extends AbstractHttpClient {

  import AkkaHttpClient._
  override def sendPushWithFutureResponse(ber: OutputBer): FutureKVOutputBerAndHttpResponse = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    val beforeNhubTs = jodaNowGetMillis
    sendPush(ber)
      .flatMap {
        case (Success(response), _) =>
          log.info(s"Unmarshalling $response")
          Unmarshal(response.entity).to[NotificationResponse]
        case (Failure(ex), _) =>
          Future.failed(new Exception(s"NHUB Request failed with exception: $ex"))
      }
      .map(k => KV.of(newBerWithLastNHubTimestamp(ber, beforeNhubTs), k))
  }
}

object AkkaHttpClient extends Serializable {

  import akka.http.scaladsl.settings.ConnectionPoolSettings
  import com.db.myproject.mediation.MediationService.{akkaConfig, domain, mediationConfig, fullUrl, url}
  import scala.concurrent.duration._

  lazy val connectionPoolSettings = ConnectionPoolSettings(system)
    .withMaxOpenRequests(akkaConfig.maxOpenRequests)
    .withMaxConnections(akkaConfig.maxOpenConnection)

  private val log: Logger = LoggerFactory.getLogger(getClass.getName)
  implicit lazy val sslConfig = MySslConfig(mediationConfig.mediation.sslConfigPath.get, mediationConfig.gcp.project).sslConfig
  lazy val httpsSSLConnectionContext = AkkaSSLContextFromSecretManager.httpsConnectionContext

  lazy val poolClientFlow = poolClient
    .initialTimeout(FiniteDuration(akkaConfig.initialTimeout, TimeUnit.SECONDS))
    .completionTimeout(FiniteDuration(akkaConfig.completionTimeout, TimeUnit.SECONDS))
    .buffer(akkaConfig.buffer, OverflowStrategy.backpressure)
    .throttle(akkaConfig.throttleRequests, akkaConfig.throttlePerSecond.second, akkaConfig.throttleBurst, ThrottleMode.Shaping)

  lazy val poolClient: Flow[(HttpRequest, Any), (Try[HttpResponse], Any), Any] =
    if (fullUrl.contains("https")) {
      mediationConfig.mediation.endpoint.certEnabled match {
        case true => Http().cachedHostConnectionPoolHttps(url, connectionContext = httpsSSLConnectionContext, settings = connectionPoolSettings)
        case false => Http().cachedHostConnectionPoolHttps(url, settings = connectionPoolSettings)
      }
    } else Http().cachedHostConnectionPool(url, settings = connectionPoolSettings)

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  def sendPush(record: InputBer): Future[(Try[HttpResponse], Any)] =
    getResponseFuture(
      getRequest(record)
        .asInstanceOf[MyHttpRequest.HttpRequest]
        .toJson
        .toString,
      absoluteURL(mediationConfig)
    )

  def getResponseFuture(reqRawString: String, url: String): Future[(Try[HttpResponse], Any)] = {
    log.info(s"reqRawString=$reqRawString")
    val entity = HttpEntity(MediaTypes.`application/json`, ByteString(reqRawString))
    val httpRequest = HttpRequest(
      method = HttpMethods.POST,
      uri = url,
      entity = entity
    )
    log.debug(s"httpRequest=${httpRequest}")
    Source
      .single(httpRequest -> ())
      .via(AkkaHttpClient.poolClientFlow)
      .runWith(Sink.head)
  }

}