package com.db.myproject.async.http

import com.db.myproject.async.http.clients.AkkaHttpClient
import com.db.myproject.async.model.BusinessEvent.BusinessEventCirce
import com.db.myproject.async.nhub.model.TargetDevice
import org.apache.beam.sdk.state._
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{OnTimer, ProcessElement, StateId, TimerId}
import org.apache.beam.sdk.values.KV
import org.joda.time.Duration
import org.slf4j.{Logger, LoggerFactory}

import java.lang.{Integer => JInt}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

object StateParDoWithAkka {

  val log: Logger = LoggerFactory getLogger getClass.getName

  type DoFnToBER = DoFn[KV[String, BusinessEventCirce], KV[String, BusinessEventCirce]]

  val akkaHttpClient = new AkkaHttpClient

  class StateParDoWithAkka() extends DoFnToBER {
    private val BUFFER_TIME = Duration.standardSeconds(120)
    private val PUBSUB_TIME = Duration.standardSeconds(50)
    @StateId("count") private val countState = StateSpecs.value[JInt]()
    @StateId("buffer") private val bagState = StateSpecs.bag[BusinessEventCirce]()
    @TimerId("timer") private val timerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME)
    @TimerId("pubSubTimer") private val pubSubTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME)

    @ProcessElement
    @throws[InterruptedException]
    def processElement(c: DoFnToBER#ProcessContext,
                       @TimerId("timer") timer: Timer,
                       @TimerId("pubSubTimer") pubSubTimer: Timer,
                       @StateId("count") countState: ValueState[JInt],
                       @StateId("buffer") bagState: BagState[BusinessEventCirce]
                      ) = {
      val key = c.element().getKey
      val ber = c.element().getValue
      val count: Int = countState.read()
      log.info(s"state_ber_size=${bagState.read().asScala.size}, state_ber=${ber}")
      if (count == 0) {
        log.info(s"Setting timer for $key")
        timer.offset(BUFFER_TIME).setRelative()
        pubSubTimer.offset(PUBSUB_TIME).setRelative()
      }

      val response = akkaHttpClient.sendPushWithResponse(ber, TargetDevice.LAST_DEVICES)
      response.onComplete {
        case Failure(t) => println(t)
          output(c, bagState, countState, ber, "KO")
        case Success(v) => log.info(s"COMPLETED HttpResponse=${v}")
          output(c, bagState, countState, ber, "OK")
      }(akkaHttpClient.ec)
    }

    def output(c: DoFnToBER#ProcessContext,
               @StateId("buffer") bagState: BagState[BusinessEventCirce],
               @StateId("count") countState: ValueState[JInt], ber: BusinessEventCirce, res: String) = {
      bagState.add(ber)
      countState.write(countState.read() + 1)
      log.info(s"complete_ber_size=${bagState.read().asScala.size}")
      c.output(KV.of(res, ber))
    }

    @OnTimer("timer")
    def onTimer(c: DoFnToBER#OnTimerContext,
                @StateId("buffer") bagState: BagState[BusinessEventCirce],
                @StateId("count") countState: ValueState[JInt]) = {
      // bagState.read().asScala.foreach { ber => log.info(s"Goodbye $ber") }
      log.info(s"****  Deleting, old_ber_size=${bagState.read().asScala.size} **** ")
      bagState.clear
      countState.clear
    }

    @OnTimer("pubSubTimer")
    def pubSubTimer(c: DoFnToBER#OnTimerContext,
                    @StateId("buffer") bagState: BagState[BusinessEventCirce],
                    @StateId("count") countState: ValueState[JInt]) = {
      log.info(s"===>  Releasing, held_ber_size=${bagState.read().asScala.size} <=== ")
      bagState.read().asScala.foreach { ber =>
        log.info(s"PubSub $ber")
        c.output(KV.of("SUCCESS", ber))
      }
      //bagState.clear
    }
  }


}
