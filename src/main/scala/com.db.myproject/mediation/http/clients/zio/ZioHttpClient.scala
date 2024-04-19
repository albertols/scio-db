package com.db.myproject.mediation.http.clients.zio

import com.db.myproject.mediation.avro.MyEventRecordUtils.newEventRecordOK
import com.db.myproject.mediation.configs.absoluteURL
import com.db.myproject.mediation.http.StateAndTimerType.{FutureKVOutputBerAndHttpResponse, InputBer, OutputBer}
import com.db.myproject.mediation.http.clients.AbstractHttpClient
import com.db.myproject.mediation.notification.NotificationFactory.getRequest
import com.db.myproject.mediation.notification.model.MyHttpRequest
import com.db.myproject.mediation.notification.model.MyHttpResponse.NotificationResponse
import com.db.myproject.utils.time.TimeUtils.jodaNowGetMillis
import org.apache.beam.sdk.values.KV
import org.slf4j.{Logger, LoggerFactory}
import zio._
import zio.http._
import zio.http.netty.NettyConfig
import zio.http.netty.client.NettyClientDriver
import zio.json._ // or any other ExecutionContext you want to use

/**
 * WIP (not working): Suppressed: zio.Cause$FiberTrace: Exception in thread "zio-fiber-0" zio.FiberFailure: Attempting to read from a
 * closed channel, which will never finish
 *
 * make companion object like AkkaHttpClient
 */
class ZioHttpClient(implicit val runtime: Runtime[Any]) extends AbstractHttpClient {

  import ZioHttpClient._

  override def sendPushWithFutureResponse(record: OutputBer): FutureKVOutputBerAndHttpResponse = {
    // import zio.CanFail.canFailAmbiguous1
    lazy val pushes = sendPushWithRequest(record)
    lazy val futurePushes = for {
      push <- pushes
      futPush <- pushes.toFuture
    } yield futPush

    Unsafe.unsafe { implicit unsafe =>
      runtime.unsafe.run(futurePushes).getOrThrowFiberFailure()
    }
  }

  //  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] =  ZIO.unit // ??? Placeholder
}

/**
 * WIP (not working): Suppressed: java.lang.IllegalStateException: Attempting to read from a closed channel, which will never finish
 */
object ZioHttpClient {

  import com.db.myproject.mediation.MediationService.{mediationConfig}

  private val log: Logger = LoggerFactory getLogger getClass.getName

  lazy val url = URL.decode(absoluteURL(mediationConfig)).toOption.get
  implicit val lastDeviceReqEncoder: JsonEncoder[MyHttpRequest.HttpRequest] = DeriveJsonEncoder.gen[MyHttpRequest.HttpRequest]
  implicit val responseDecoder: JsonDecoder[NotificationResponse] = DeriveJsonDecoder.gen[NotificationResponse]
  // https://zio.dev/zio-http/examples/basic/https-client
  val httpsLayers = ZClient.default ++
    NettyClientDriver.live ++
    DnsResolver.default ++
    ZLayer.succeed(NettyConfig.default) ++
    Scope.default
  val httpLayers = ZClient.default ++
    Scope.default

  def sendPushWithRequest(record: InputBer) = for {
    requestDto <- ZIO.succeed(getRequest(record))
    responseDto <- sendPushWithResponse(requestDto, record)
  } yield responseDto

  def sendPushWithResponse(
                            requestDto: MyHttpRequest.HttpRequest,
                            record: InputBer
                          ) = {
    val requestJson = requestDto.toJson

    val zioRequest = Request(
      url = url,
      method = Method.POST,
      headers = Headers("Content-Type" -> "application/json"),
      body = Body.fromString(requestJson)
    )

    val beforeNhubTs = jodaNowGetMillis
    log.info(s"zioRequest=$zioRequest")
    for {
      response <- {
        ZClient
          .request(zioRequest)
          .provideLayer(httpLayers)
      }
      responseBody <- response.body.asString
      responseDto <- ZIO
        .fromEither(responseBody.fromJson[NotificationResponse])
        .mapError(new RuntimeException(_)) // Converting parsing error to Throwable
    } yield KV.of(newEventRecordOK(record, beforeNhubTs, responseDto.body), responseDto)
  }

}
