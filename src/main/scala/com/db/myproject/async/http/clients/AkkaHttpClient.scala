package com.db.myproject.async.http.clients

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import com.db.myproject.async.SCIOAsyncService.mediationConfig
import com.db.myproject.async.model.BusinessEvent.BusinessEventCirce
import com.db.myproject.async.nhub.RequestFactory.{getDeviceRequest, getPushRequest}
import com.db.myproject.async.nhub.model.HttpResponse.HttpResponse
import com.db.myproject.async.nhub.model.TargetDevice
import org.apache.beam.sdk.values.KV
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class AkkaHttpClient {
  val log: Logger = LoggerFactory getLogger getClass.getName
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  def poolClient: Flow[(HttpRequest, Any), (Try[HttpResponse], Any), Any] =
    if (mediationConfig.mediation.nhub.soapUrl.contains("https"))
      Http().cachedHostConnectionPoolHttps(mediationConfig.mediation.nhub.domain)
    else Http().cachedHostConnectionPool(mediationConfig.mediation.nhub.domain)

  val poolClientFlow = poolClient
    .initialTimeout(FiniteDuration(30L, TimeUnit.SECONDS))
    // .idleTimeout(FiniteDuration(30L, TimeUnit.SECONDS))
    .completionTimeout(FiniteDuration(60L, TimeUnit.SECONDS))

  def sendPushWithResponse(ber: BusinessEventCirce, target: TargetDevice.Value) = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    sendPush(ber, target)
      .flatMap {
        case (Success(response), _) =>
          log.info(s"Unmarshalling $response")
          log.info(s"\tentity=${response.entity}")
          Unmarshal(response.entity).to[HttpResponse]
        case (Failure(ex), _) =>
          Future.failed(new Exception(s"Request failed with exception: $ex"))
        // TODO: retry
      }
      .map(k => KV.of(ber, k))
  }

  def sendPush(ber: BusinessEventCirce, target: TargetDevice.Value) = {
    if (target == TargetDevice.LAST_DEVICES)
      getResponseFuture(
        getDeviceRequest(ber).toJson.toString,
        mediationConfig.mediation.nhub.soapUrl + mediationConfig.mediation.nhub.lastDeviceUrl
      )
    else
      getResponseFuture(
        getPushRequest(ber).toJson.toString,
        mediationConfig.mediation.nhub.soapUrl + mediationConfig.mediation.nhub.pushUrl
      )
  }

  def getResponseFuture(reqRawString: String, url: String) = {
    log.info(s"reqRawString=$reqRawString")
    val entity = HttpEntity(ContentTypes.`application/json`, ByteString(reqRawString))
    log.info(s"HttpEntity=$entity")
    val httpRequest = HttpRequest(
      method = HttpMethods.POST,
      uri = url,
      entity = entity
    )
    log.info(s"HttpRequest=${httpRequest}")
    Source
      .single(httpRequest -> ())
      .via(poolClientFlow)
      .runWith(Sink.head)
  }
}
