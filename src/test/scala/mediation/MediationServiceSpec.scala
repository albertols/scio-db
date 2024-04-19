package mediation

import com.db.myproject.mediation.avro.{MyEventRecord, _}
import com.db.myproject.mediation.MediationService
import com.db.myproject.mediation.MediationService.{
  applyBerKVState,
  berKVState,
  bersAfterHttpResponse,
  mapWithIdempotentKeyAndGlobalWindow,
  windowOptions
}
import com.db.myproject.mediation.avro.MyEventRecordUtils.{getIdempotentNotificationKey, isBerValid, newBerWithInitalLoadEventId}
import com.db.myproject.mediation.http.StateAndTimerType
import com.db.myproject.mediation.notification.model.MyHttpResponse
import com.db.myproject.mediation.notification.model.MyHttpResponse.NOT_HTTP_RESPONSE
import com.db.myproject.mediation.testing.NotificationsMockData.{not_sent_debit_quique, true_sent_debit_quique}
import com.db.myproject.utils.enumeration.EnumUtils
import com.db.myproject.utils.pureconfig.{PureConfigEnvEnum, PureConfigSourceEnum}
import com.spotify.scio.coders.kryo.fallback
import com.spotify.scio.streaming.DISCARDING_FIRED_PANES
import com.spotify.scio.testing.{testStreamOf, PipelineSpec, TestStreamScioContext}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.transforms.windowing.{
  AfterPane,
  AfterProcessingTime,
  AfterWatermark,
  GlobalWindow,
  IntervalWindow,
  Repeatedly,
  TimestampCombiner
}
import org.apache.beam.sdk.values.{KV, TimestampedValue}
import org.joda.time.{Duration, Instant}
import org.scalatest.matchers.should

import scala.collection.Seq

class MediationServiceSpec extends PipelineSpec {
  MediationService.envEnum = EnumUtils.matchEnum("test", PureConfigEnvEnum).get
  MediationService.sourceConfigEnum = PureConfigSourceEnum.FILE
  MediationService.defaultConfigPath = "src/test/resources/mediation/test.conf"
  val testConfig = MediationService.defaultMediationConfig

  val invalidRecord = MyEventRecord.newBuilder
    .setNotification(new Notification())
    .setCustomer(new Customer())
    .setEvent(new Event())
    .build

  "null_nhub_debit_quique" should "be null" in {
    println(not_sent_debit_quique)
    not_sent_debit_quique.getNotification.getNhubSuccess shouldBe null
  }

  "defaultMediationConfig gcsBucket" should "be empty" in {
    testConfig.gcsBucket equals ""
  }

  val baseTime = new Instant(0)
  val teamWindowDuration = Duration.standardMinutes(20)
  private def event(
    myEventRecord: MyEventRecord,
    baseTimeOffset: Duration
  ): TimestampedValue[MyEventRecord] = {
    val t = baseTime.plus(baseTimeOffset)
    TimestampedValue.of(myEventRecord, t)
  }

  "HTTP_RESPONSE" should "exist" in {

    val stream = testStreamOf[MyEventRecord]
      .addElements(event(not_sent_debit_quique, Duration.standardSeconds(1)))
      .advanceWatermarkToInfinity()

    runWithContext { sc =>
      val streamingBer = sc.testStream(stream).withGlobalWindow()
      def winOptions = com.spotify.scio.values.WindowOptions(
        allowedLateness = Duration.ZERO,
        trigger = Repeatedly.forever(
          AfterProcessingTime
            .pastFirstElementInPane()
            .plusDelayOf(Duration.standardMinutes(1))
        ),
        accumulationMode = DISCARDING_FIRED_PANES,
        timestampCombiner = TimestampCombiner.EARLIEST
      )
      val okNotSentBers = MediationService
        .mapWithIdempotentKeyAndGlobalWindow(
          streamingBer,
          ber => getIdempotentNotificationKey(ber),
          winOptions = Some(windowOptions)
        )
        .distinctByKey
        .map { ber =>
          KV.of(ber._1, ber._2)
        }
      val appliedState: SCollection[StateAndTimerType.KVOutputBerAndHttpResponse] = applyBerKVState(berKVState, okNotSentBers)
      val bersForAnalytics = bersAfterHttpResponse(appliedState)
      val httpResponses = bersForAnalytics.keys
      // bersForAnalytics shouldNot beEmpty
      val expectedHttpResponse = MyHttpResponse.NotificationResponse(
        101,
        title = not_sent_debit_quique.getNotification.getId.toString,
        body = not_sent_debit_quique.getNotification.getMessage.toString,
        userId = not_sent_debit_quique.getCustomer.getId.toString.toInt
      )
      // val expectedOutput: StateAndTimerType.KVOutputBerAndHttpResponse = KV.of(not_sent_debit_quique, expectedHttpResponse)

      val window = new IntervalWindow(baseTime, teamWindowDuration)
      httpResponses should inEarlyGlobalWindowPanes  {
        containInAnyOrder(Seq(expectedHttpResponse))
      }

    }
  }

  "getIdempotentNotificationKey and mapBerWithIdempotentKeyAndGlobalWindow" should "validate idempotent key" in {
    runWithContext { sc =>
      val streamingBer = sc.parallelize(Seq(not_sent_debit_quique)) // also OK
      val initialBers = MediationService.mapWithIdempotentKeyAndGlobalWindow(streamingBer, ber => getIdempotentNotificationKey(ber))

      val ok_idempotent_key = s"unique_kcop-1"
      println(s"ok_idempotent_key=$ok_idempotent_key")
      initialBers should containSingleValue(
        (ok_idempotent_key, not_sent_debit_quique)
      )

      val ko_idempotent_key = s"trimmer_kcop-BAD_CONSTUMER"
      println(s"ko_idempotent_key=$ko_idempotent_key")
      initialBers shouldNot containSingleValue(
        (s"ko_idempotent_key", not_sent_debit_quique)
      )

    }
  }

  "isBerValid" should "return true for valid record" in {
    isBerValid(not_sent_debit_quique) shouldBe true
  }

  it should "return false for invalid record" in {
    isBerValid(invalidRecord) shouldBe false
  }

  "okAndKoBers" should "correctly partition valid and invalid records" in {
    runWithContext { sc =>
      val input = sc.parallelize(Seq(not_sent_debit_quique, invalidRecord))
      val (koRecords, okRecords) = MediationService.okAndKoBers(input)

      val validRecords = okRecords
      validRecords should containSingleValue(not_sent_debit_quique)

      val invalidRecords = koRecords
      invalidRecords should containSingleValue(invalidRecord)
    }
  }

  "getNonDuplicatedBerPubSubAndGcs" should "have empty okNotSentBers when historical nhubSuccess=true" in {
    val pubSubStream = testStreamOf[MyEventRecord]
      .advanceWatermarkTo(new Instant(0))
      .addElements(not_sent_debit_quique)
      .advanceWatermarkToInfinity

    runWithContext { sc =>
      val historicalGcsBers = sc.parallelize(Seq(true_sent_debit_quique))
      lazy val sideGcs = mapWithIdempotentKeyAndGlobalWindow(
        historicalGcsBers,
        getIdempotentNotificationKey,
        newBerWithInitalLoadEventId
      ).asMapSingletonSideInput
      val allDistinctBers = MediationService.getNonDuplicatedNotificationPubSubAndGcs(sc.testStream(pubSubStream), sideGcs)
      val koBers = allDistinctBers._1
      val okNotSentBers = allDistinctBers._2
      koBers should beEmpty
      okNotSentBers should beEmpty
    }
  }

  "duplicated GCS_BERs without distinctByKey" should "throw Exception" in {
    val pubSubStream = testStreamOf[MyEventRecord]
      .advanceWatermarkTo(new Instant(0))
      .addElements(not_sent_debit_quique)
      .advanceWatermarkToInfinity

    an[PipelineExecutionException] should be thrownBy {
      runWithContext { sc =>
        val historicalGcsBers = sc.parallelize(Seq(true_sent_debit_quique, true_sent_debit_quique))
        lazy val sideGcs = mapWithIdempotentKeyAndGlobalWindow(
          historicalGcsBers,
          getIdempotentNotificationKey,
          newBerWithInitalLoadEventId
        ).asMapSingletonSideInput
        val allDistinctBers = MediationService.getNonDuplicatedNotificationPubSubAndGcs(sc.testStream(pubSubStream), sideGcs)
      }
    }
  }

  "getNonDuplicatedBerPubSubAndGcs" should "have empty okNotSentBers when historical exist despite nhubSuccess=null" in {
    val pubSubStream = testStreamOf[MyEventRecord]
      .advanceWatermarkTo(new Instant(0))
      .addElements(not_sent_debit_quique)
      .advanceWatermarkToInfinity

    runWithContext { sc =>
      val historicalGcsBers = sc.parallelize(Seq(not_sent_debit_quique))
      lazy val sideGcs = mapWithIdempotentKeyAndGlobalWindow(
        historicalGcsBers,
        getIdempotentNotificationKey,
        newBerWithInitalLoadEventId
      ).asMapSingletonSideInput
      val allDistinctBers = MediationService.getNonDuplicatedNotificationPubSubAndGcs(sc.testStream(pubSubStream), sideGcs)
      val koBers = allDistinctBers._1
      val okNotSentBers = allDistinctBers._2
      koBers should beEmpty
      okNotSentBers should beEmpty
    }
  }

}
