package mediation

import com.db.myproject.mediation.avro._
import com.db.myproject.mediation.MediationService
import com.db.myproject.mediation.MediationService.{applyBerKVState, berKVState, bersAfterHttpResponse, mapWithIdempotentKeyAndGlobalWindow}
import com.db.myproject.mediation.avro.MyEventRecordUtils.{getIdempotentNotificationKey, isBerValid, newBerWithInitalLoadEventId}
import com.db.myproject.mediation.testing.NotificationsMockData.{null_nhub_debit_quique, true_nhub_debit_quique}
import com.db.myproject.utils.enumeration.EnumUtils
import com.db.myproject.utils.pureconfig.{PureConfigEnvEnum, PureConfigSourceEnum}
import com.spotify.scio.coders.kryo.fallback
import com.spotify.scio.testing.{testStreamOf, PipelineSpec, TestStreamScioContext}
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.values.KV
import org.joda.time.Instant

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
    println(null_nhub_debit_quique)
    null_nhub_debit_quique.getNotification.getNhubSuccess shouldBe null
  }

  "defaultMediationConfig gcsBucket" should "be empty" in {
    testConfig.gcsBucket equals ""
  }

  // "HTTP_RESPONSE" should "exist" in {
  //  runWithContext { sc =>
  //    val streamingBer = sc.parallelize(Seq(null_nhub_debit_quique))
  //    val okNotSentBers = MediationService.mapWithIdempotentKeyAndGlobalWindow(
  //      streamingBer,
  //      ber => getIdempotentNotificationKey(ber)).distinctByKey.map { ber =>
  //      KV.of(ber._1, ber._2)
  //    }
  //    val bersForAnalytics = bersAfterHttpResponse(applyBerKVState(berKVState, okNotSentBers))
  //    bersForAnalytics shouldNot beEmpty
  //  }
  // }

  "getIdempotentNotificationKey and mapBerWithIdempotentKeyAndGlobalWindow" should "validate idempotent key" in {
    runWithContext { sc =>
      val streamingBer = sc.parallelize(Seq(null_nhub_debit_quique)) // also OK
      val initialBers = MediationService.mapWithIdempotentKeyAndGlobalWindow(streamingBer, ber => getIdempotentNotificationKey(ber))

      val ok_idempotent_key = s"unique_kcop-80696970"
      println(s"ok_idempotent_key=$ok_idempotent_key")
      initialBers should containSingleValue(
        (ok_idempotent_key, null_nhub_debit_quique)
      )

      val ko_idempotent_key = s"trimmer_kcop-80696970"
      println(s"ko_idempotent_key=$ko_idempotent_key")
      initialBers shouldNot containSingleValue(
        (s"ko_idempotent_key", null_nhub_debit_quique)
      )

    }
  }

  "isBerValid" should "return true for valid record" in {
    isBerValid(null_nhub_debit_quique) shouldBe true
  }

  it should "return false for invalid record" in {
    isBerValid(invalidRecord) shouldBe false
  }

  "okAndKoBers" should "correctly partition valid and invalid records" in {
    runWithContext { sc =>
      val input = sc.parallelize(Seq(null_nhub_debit_quique, invalidRecord))
      val (koRecords, okRecords) = MediationService.okAndKoBers(input)

      val validRecords = okRecords
      validRecords should containSingleValue(null_nhub_debit_quique)

      val invalidRecords = koRecords
      invalidRecords should containSingleValue(invalidRecord)
    }
  }

  "getNonDuplicatedBerPubSubAndGcs" should "have empty okNotSentBers when historical nhubSuccess=true" in {
    val pubSubStream = testStreamOf[MyEventRecord]
      .advanceWatermarkTo(new Instant(0))
      .addElements(null_nhub_debit_quique)
      .advanceWatermarkToInfinity

    runWithContext { sc =>
      val historicalGcsBers = sc.parallelize(Seq(true_nhub_debit_quique))
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
      .addElements(null_nhub_debit_quique)
      .advanceWatermarkToInfinity

    an[PipelineExecutionException] should be thrownBy {
      runWithContext { sc =>
        val historicalGcsBers = sc.parallelize(Seq(true_nhub_debit_quique, true_nhub_debit_quique))
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
      .addElements(null_nhub_debit_quique)
      .advanceWatermarkToInfinity

    runWithContext { sc =>
      val historicalGcsBers = sc.parallelize(Seq(null_nhub_debit_quique))
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
