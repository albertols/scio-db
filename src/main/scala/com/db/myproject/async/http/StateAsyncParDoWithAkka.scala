package com.db.pwcclakees.mediation.http

import com.db.pwcclakees.mediation.http.clients.AkkaHttpClient
import com.db.pwcclakees.mediation.http.state.StateScalaAsyncDoFn
import com.db.pwcclakees.mediation.model.BusinessEvent.BusinessEventCirce
import com.db.pwcclakees.mediation.nhub.model.HttpResponse.HttpResponse
import com.db.pwcclakees.mediation.nhub.model.TargetDevice
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import org.apache.beam.sdk.state.{MapState, Timer, ValueState}
import org.apache.beam.sdk.values.KV
import org.joda.time.Duration
import org.slf4j.{Logger, LoggerFactory}

import java.lang.{Integer => JInt}
import scala.collection.JavaConverters._
import scala.concurrent.Future

class StateAsyncParDoWithAkka()
    extends StateScalaAsyncDoFn[
      KV[String, BusinessEventCirce],
      KV[BusinessEventCirce, HttpResponse],
      AkkaHttpClient
    ] {
  private val BUFFER_TIME = Duration.standardSeconds(120)
  private val PUBSUB_TIME = Duration.standardSeconds(50)
  override def getResourceType: ResourceType = ResourceType.PER_CLASS
  override def createResource(): AkkaHttpClient = new AkkaHttpClient

  val log: Logger = LoggerFactory getLogger getClass.getName

  override def processElement(
    input: KV[String, BusinessEventCirce]
  ): Future[KV[BusinessEventCirce, HttpResponse]] =
    getResource.sendPushWithResponse(input.getValue, TargetDevice.LAST_DEVICES)

  override protected def settingElementTimer(timer: Timer, counter: ValueState[JInt]): Unit = {
    if (counter.read() == 0) {
      log.info(s"Setting timer for")
      timer.offset(BUFFER_TIME).setRelative()
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
    element: KV[String, BusinessEventCirce]
  ): Boolean = {
    val key = element.getKey
    log.info(s"Checking idempotent ${key}")
    buffer.entries().read.asScala.foreach(ber => log.info(s"stored_ber=$ber"))
    val res = buffer
      .entries()
      .read
      .asScala
      .find { ber =>
        ber.getKey.getKey.equals(key)
      }
      .size
    res > 0 match {
      case true =>
        log.info(s"duplicated key=${key}")
        true
      case false =>
        log.info(s"processing key=${key}")
        false
    }
  }

}
