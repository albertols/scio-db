package com.db.myproject.mediation.http

import com.db.myproject.mediation.MediationService.INITIAL_LOAD_PREFIX
import com.db.myproject.mediation.avro.MyEventRecordUtils.{getIdempotentNotificationKey, newEventRecordWithRetryIncrement}
import com.db.myproject.mediation.configs.MediationConfig
import com.db.myproject.mediation.http.StateAndTimerType.{InputBer, KVInputStringAndBer, KVOutputBerAndHttpResponse}
import com.db.myproject.mediation.http.clients.AbstractHttpClient
import com.db.myproject.mediation.http.clients.akka.AkkaHttpClient
import com.db.myproject.mediation.http.clients.zio.ZioHttpClient
import com.db.myproject.mediation.http.state.StateScalaAsyncDoFn
import com.db.myproject.mediation.notification.model.MyHttpResponse.{SENT_OR_DUPLICATED, koNotificationResponse}
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import org.apache.beam.sdk.state.{BagState, Timer}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.KV
import org.joda.time.{Duration, Instant}
import org.slf4j.{Logger, LoggerFactory}
import zio.Runtime

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
 * @param mediationConfig
 * @param applyInitialLoad,
 *   interim flag for testing
 */
class StateAsyncParDoWithHttpHandler(mediationConfig: MediationConfig, applyInitialLoad: Boolean)
    extends StateScalaAsyncDoFn[
      StateAndTimerType.KVInputStringAndBer,
      StateAndTimerType.KVOutputBerAndHttpResponse,
      AbstractHttpClient
    ] {
  val log: Logger = LoggerFactory getLogger getClass.getName
  val MAX_RETRIES = 3
  val BACKOFF_SECONDS = 10
  private val BUFFER_TIME = Duration.standardSeconds(mediationConfig.mediation.ttlTime)
  implicit lazy val zioRuntime = Runtime.default

  lazy val httpClient = {
    mediationConfig.mediation.httpClientType match {
      case "akka" => new AkkaHttpClient
      case "zio"  => new ZioHttpClient
    }
  }

  override def createResource(): AbstractHttpClient = httpClient

  override def getResourceType: ResourceType = ResourceType.PER_CLASS

  override def processElement(
    input: StateAndTimerType.KVInputStringAndBer
  ): StateAndTimerType.FutureKVOutputBerAndHttpResponse =
    // STEP 4.2)
    Try(mediationConfig.mediation.retryNotifications match {
      case true  => sendPushWithRetryZio(input.getValue)
      case false => getResource.sendPushWithFutureResponse(input.getValue)
    }) match {
      case Success(s) => s
      case Failure(ex) =>
        import scala.concurrent.ExecutionContext.Implicits.global
        Future(KV.of(input.getValue, koNotificationResponse(s"NOTIFICATION_ERROR: ${ex}")))
    }

  def sendPushWithRetryZio(
    record: InputBer
  )(implicit zioRuntime: Runtime[Any]): StateAndTimerType.FutureKVOutputBerAndHttpResponse = {
    import zio._
    lazy val futureRetriableBer = ZIO
      .attempt {
        getResource.sendPushWithFutureResponse(newEventRecordWithRetryIncrement(record))
      }
      .retry(Schedule.fixed(BACKOFF_SECONDS.second) && Schedule.recurs(MAX_RETRIES))
      .onError(cause =>
        ZIO.succeed(
          log.error(s"[exhausted_notification=${record.getEvent.getTransactionId}] Retried error:${cause}", cause)
        )
      )

    Unsafe.unsafe { implicit unsafe =>
      zioRuntime.unsafe.run(futureRetriableBer).getOrThrowFiberFailure()
    }
  }

  override protected def settingElementTTLTimer(
    buffer: BagState[StateAndTimerType.KVInputStringAndBer],
    ttl: Timer,
    input: KVInputStringAndBer
  ): Unit = {
    if (buffer.read.asScala.size == 0) {
      ttl.offset(BUFFER_TIME).setRelative()
      // log.info(s"[settingElementTTLTimer] TTL=${ttl.getCurrentRelativeTime} for key=${getIdempotentBerKey(input.getValue)}")
    }
  }

  override protected def addIdempotentElementInBuffer(
    buffer: BagState[KVInputStringAndBer],
    input: KVInputStringAndBer
  ): Unit = {
    buffer.add(input)
    log.info(s"buffer_size=${buffer.read.asScala.size}, new buffer entry ${input.getKey}")
  }

  override protected def alreadySentOrLoaded(
    buffer: BagState[StateAndTimerType.KVInputStringAndBer],
    element: StateAndTimerType.KVInputStringAndBer,
    ttl: Timer
  ): Boolean = {
    val newKey = element.getKey
    if (element.getValue.getEvent.getId.toString.startsWith(s"${INITIAL_LOAD_PREFIX}_") && applyInitialLoad) {
      log.info(s"[alreadySentOrLoaded] already $newKey, as Event.getId=${element.getValue.getEvent.getId}")
      true
    } else {
      log.info(s"[alreadySentOrLoaded] Checking idempotent newKey=$newKey")
      val res = buffer.read.asScala.find { kvBer =>
        val storedBerKey = getIdempotentNotificationKey(kvBer.getValue)
        log.info(s"\t\tstoredBerKey=$storedBerKey")
        storedBerKey.equals(newKey)
      }.size
      res > 0 match {
        case true =>
          log.info(s"duplicated key=$newKey")
          true
        case false =>
          log.info(s"processing key=$newKey")
          setTTL(ttl, newKey)
          false
      }
    }
  }

  override protected def initialLoad(
    buffer: BagState[StateAndTimerType.KVInputStringAndBer],
    element: StateAndTimerType.KVInputStringAndBer,
    ttl: Timer
  ) = {
    val record = element.getValue
    if (record.getEvent.getId.toString.startsWith(s"${INITIAL_LOAD_PREFIX}_") && applyInitialLoad) {
      val newKey = getIdempotentNotificationKey(record)
      log.info(s"[initialLoad] saving with key=$newKey")
      buffer.add(KV.of(newKey, record))
      setTTL(ttl, newKey)
    }
  }

  private def setTTL(ttl: Timer, key: String): Unit = {
    val nextTtl = nextCachingTime
    log.info(s"[setTTL] new_ttl_key=$key, ttl_free=$nextTtl")
    ttl.set(nextTtl)
  }

  def nextCachingTime = Instant.now().plus(BUFFER_TIME)

  override protected def outputAlreadySentOrLoaded(
    element: KVInputStringAndBer,
    out: DoFn.OutputReceiver[KVOutputBerAndHttpResponse]
  ): Unit =
    out.output(
      KV.of(
        element.getValue,
        SENT_OR_DUPLICATED
      )
    )
}
