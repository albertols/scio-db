package com.db.myproject.async.http

import com.db.myproject.async.SCIOAsyncService.idempotentBer
import com.db.myproject.async.configs.MediationConfig
import com.db.myproject.async.http.clients.AkkaHttpClient
import com.db.myproject.async.http.state.StateScalaAsyncDoFn
import com.db.myproject.async.model.BusinessEvent.BusinessEventCirce
import com.db.myproject.async.nhub.model.HttpResponse.HttpResponse
import com.db.myproject.async.nhub.model.TargetDevice
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import org.apache.beam.sdk.state.{MapState, Timer}
import org.apache.beam.sdk.values.KV
import org.joda.time.{Duration, Instant}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.concurrent.Future

class StateAsyncParDoWithAkka(mediationConfig: MediationConfig)
  extends StateScalaAsyncDoFn[
    KV[String, BusinessEventCirce],
    KV[BusinessEventCirce, HttpResponse],
    AkkaHttpClient
  ] {
  val log: Logger = LoggerFactory getLogger getClass.getName
  private val BUFFER_TIME = Duration.standardSeconds(90)

  override def getResourceType: ResourceType = ResourceType.PER_CLASS

  override def createResource(): AkkaHttpClient = new AkkaHttpClient(mediationConfig)

  override def processElement(
                               input: KV[String, BusinessEventCirce]
                             ): Future[KV[BusinessEventCirce, HttpResponse]] =
    getResource.sendPushWithResponse(input.getValue, TargetDevice.LAST_DEVICES)

  override protected def settingElementTTLTimer(buffer: MapState[KV[String, BusinessEventCirce], KV[BusinessEventCirce, HttpResponse]], ttl: Timer): Unit = {
    if (buffer
      .entries()
      .read
      .asScala.size == 0) {
      log.info(s"Setting TTL for $BUFFER_TIME")
      ttl.offset(BUFFER_TIME).setRelative()
    }
  }

  override protected def addIdempotentElementInBuffer(
                                                       buffer: MapState[KV[String, BusinessEventCirce], KV[BusinessEventCirce, HttpResponse]],
                                                       input: KV[String, BusinessEventCirce],
                                                       output: KV[BusinessEventCirce, HttpResponse]
                                                     ): Unit = {
    buffer.put(input, output)
    log.info(s"buffer_size=${buffer.entries().read.asScala.size}, new buffer entry ${input.getKey}")
  }

  override protected def alreadySent(
                                      buffer: MapState[KV[String, BusinessEventCirce], KV[BusinessEventCirce, HttpResponse]],
                                      element: KV[String, BusinessEventCirce],
                                      timer: Timer
                                    ): Boolean = {
    val newKey = element.getKey
    log.info(s"Checking idempotent newKey=$newKey")
    val res = buffer
      .entries()
      .read
      .asScala
      .find { kvBer =>
        val storedBerKey = idempotentBer(kvBer.getKey.getValue)
        log.info(s"\t\tstoredBerKey=$storedBerKey")
        storedBerKey.equals(newKey)
      }
      .size
    res > 0 match {
      case true =>
        log.info(s"duplicated key=${newKey}")
        true
      case false =>
        log.info(s"processing key=${newKey}")
        val next_caching_time = Instant.now().plus(BUFFER_TIME)
        timer.set(next_caching_time)
        false
    }
  }

}


