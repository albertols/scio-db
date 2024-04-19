package mediation

import com.db.myproject.mediation.MediationService
import com.db.myproject.mediation.MediationService.{applyBerKVState, berKVState, bersAfterHttpResponse, mapWithIdempotentKeyAndGlobalWindow}
import com.db.myproject.mediation.avro.MyEventRecordUtils.{getIdempotentNotificationKey, isBerValid, newBerWithInitalLoadEventId}
import com.db.myproject.mediation.avro._
import com.db.myproject.mediation.http.StateAndTimerType
import com.db.myproject.mediation.notification.model.MyHttpResponse
import com.db.myproject.mediation.notification.model.MyHttpResponse.SENT_OR_DUPLICATED
import com.db.myproject.mediation.testing.NotificationsMockData.{not_sent_debit_quique, true_sent_debit_quique}
import com.db.myproject.utils.enumeration.EnumUtils
import com.db.myproject.utils.pureconfig.{PureConfigEnvEnum, PureConfigSourceEnum}
import com.spotify.scio.coders.kryo.fallback
import com.spotify.scio.testing.{PipelineSpec, TestStreamScioContext, testStreamOf}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.values.{KV, TimestampedValue}
import org.joda.time.{Duration, Instant}

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

  val baseTime = new Instant(0)
  val teamWindowDuration = Duration.standardMinutes(20)
  private def event(
    myEventRecord: MyEventRecord,
    baseTimeOffset: Duration
  ): TimestampedValue[MyEventRecord] = {
    val t = baseTime.plus(baseTimeOffset)
    TimestampedValue.of(myEventRecord, t)
  }

  "1 OK and 2 SENT_OR_DUPLICATED HTTP_RESPONSE" should "exist in the same stream" in {
    // simulates similar stream as PubSub
    val streamWithDuplicates = testStreamOf[MyEventRecord]
      // Start at the epoch
      .advanceWatermarkTo(baseTime)
      // add some elements ahead of the watermark
      .addElements(event(not_sent_debit_quique, Duration.standardSeconds(1)))
      // advance the watermark
      .advanceWatermarkTo(baseTime.plus(Duration.standardSeconds(10)))
      // duplicated elements
      .addElements(event(not_sent_debit_quique, Duration.standardSeconds(5)))
      .addElements(event(not_sent_debit_quique, Duration.standardSeconds(1)))
      .advanceWatermarkToInfinity

    runWithContext { sc =>
      val okNotSentBers = MediationService
        .mapWithIdempotentKeyAndGlobalWindow(
          sc.testStream(streamWithDuplicates),
          ber => getIdempotentNotificationKey(ber)
        )
        // .distinctByKey // not applied as it would get rid of the same records in GlobalWindow
        .map { ber =>
          KV.of(ber._1, ber._2)
        }
      val appliedState: SCollection[StateAndTimerType.KVOutputBerAndHttpResponse] = applyBerKVState(berKVState, okNotSentBers)
      val bersForAnalytics: SCollection[(MyHttpResponse.NotificationResponse, MyEventRecord)] = bersAfterHttpResponse(appliedState)
      val httpResponses: SCollection[MyHttpResponse.NotificationResponse] = bersForAnalytics.keys

      httpResponses should {
        val expectedOkHttpResponse = MyHttpResponse.NotificationResponse(
          101, // id automatically returned by
          title = not_sent_debit_quique.getNotification.getId.toString,
          body = not_sent_debit_quique.getNotification.getMessage.toString,
          userId = not_sent_debit_quique.getCustomer.getId.toString.toInt
        )
        containInAnyOrder(Seq(expectedOkHttpResponse, SENT_OR_DUPLICATED, SENT_OR_DUPLICATED))
      }
    }
  }

  "null_nhub_debit_quique" should "be null" in {
    println(not_sent_debit_quique)
    not_sent_debit_quique.getNotification.getNhubSuccess shouldBe null
  }

  "defaultMediationConfig gcsBucket" should "be empty" in {
    testConfig.gcsBucket equals ""
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

      val ko_idempotent_key = s"trimmer_kcop-BAD_COSTUMER"
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

  "getNonDuplicatedBerPubSubAndGcs" should "have empty okNotSentBers when historical notification exists" in {
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

  "duplicated historical notifications without distinctByKey" should "throw Exception due to SideInput lookup" in {
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
