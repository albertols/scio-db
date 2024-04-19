package com.db.myproject.mediation

import com.db.myproject.mediation.avro.MyEventRecord
import com.db.myproject.mediation.avro.MyEventRecordUtils._
import com.db.myproject.mediation.configs.MediationConfig
import com.db.myproject.mediation.http.{StateAndTimerType, StateAsyncParDoWithHttpHandler}
import com.db.myproject.mediation.notification.model.MyHttpResponse.{NotificationResponse, isSuccessAndFullResultDescr}
import com.db.myproject.streaming.utils.SinkUtils
import com.db.myproject.streaming.utils.WindowUtils.toIntervalWindowAndIterable
import com.db.myproject.streaming.utils.dofn.ssl.SslConfig.SslConfigPath
import com.db.myproject.streaming.utils.dofn.ssl.SslConfigHelper
import com.db.myproject.streaming.utils.pubsub.{PubSubConsumer, PubSubProducer}
import com.db.myproject.utils.GCSCommonUtilsInterim
import com.db.myproject.utils.GCSCommonUtilsInterim.areAllFilesWithExtensionInGCS
import com.db.myproject.utils.TimeUtilsInterim.getLastDateDaysFrom
import com.db.myproject.utils.enumeration.EnumUtils
import com.db.myproject.utils.pureconfig.RootPureConfig.{readConfigFromEnv, readConfigFromGcsOrResources}
import com.db.myproject.utils.pureconfig.{PureConfigEnvEnum, PureConfigSourceEnum}
import com.db.myproject.utils.time.TimeUtils.jodaNowGetMillis
import com.google.cloud.storage.StorageOptions
import com.spotify.scio.avro.avroScioContextOps
import com.spotify.scio.coders.kryo.fallback
import com.spotify.scio.pubsub.PubsubIO
import com.spotify.scio.streaming.DISCARDING_FIRED_PANES
import com.spotify.scio.values.{SCollection, SideInput, WindowOptions}
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.windowing.{AfterProcessingTime, Repeatedly, TimestampCombiner}
import org.apache.beam.sdk.values.KV
import org.joda.time.{DateTime, Duration}
import org.slf4j.{Logger, LoggerFactory}
import pureconfig.generic.auto._

import scala.reflect.ClassTag

/**
 */
object MediationService {
  implicit val ct: ClassTag[MyEventRecord] = ClassTag(classOf[MyEventRecord])
  implicit val configReader = pureconfig.ConfigReader[MediationConfig]
  lazy val defaultMediationConfig: MediationConfig =
    readConfigFromEnv(envEnum, sourceConfigEnum, Some(defaultConfigPath))
  implicit lazy val mediationConfig: MediationConfig = envEnum match {
    case PureConfigEnvEnum.test | PureConfigEnvEnum.local => defaultMediationConfig
    case _ =>
      readConfigFromGcsOrResources(
        defaultMediationConfig.gcp.project,
        defaultMediationConfig.gcsBucket,
        defaultMediationConfig.mediation.configBlobPath,
        envEnum,
        defaultMediationConfig
      )
  }
  lazy val expiringHistoricalTime = new DateTime(jodaNowGetMillis).plusSeconds(mediationConfig.mediation.ttlTime)
  lazy val berKVState = new StateAsyncParDoWithHttpHandler(mediationConfig, true)
  val INITIAL_LOAD_PREFIX = "INITIAL_LOAD"
  val log: Logger = LoggerFactory getLogger getClass.getName

  /* vars for defaultConfig (set up also by MediationServiceSpec) */
  var envEnum = EnumUtils.matchEnum("local", PureConfigEnvEnum).get
  var sourceConfigEnum = PureConfigSourceEnum.RESOURCES
  var defaultConfigPath = "mediation/application.conf"

  /* common HTTP Client config */
  implicit lazy val fullUrl = mediationConfig.mediation.endpoint.fullUrl
  implicit lazy val url = mediationConfig.mediation.endpoint.url
  implicit lazy val domain = mediationConfig.mediation.endpoint.domain
  implicit lazy val akkaConfig = mediationConfig.mediation.akka.get

  def main(cmdlineArgs: Array[String]): Unit = {
    try {
      // 1) Init Context and config
      log.info("Init SCIO Context...")
      val (sc, args) = ContextAndArgs(cmdlineArgs)
      implicit val scioContext: ScioContext = sc
      sc.optionsAs[StreamingOptions].setStreaming(true)
      log.info(s"SCIO argsTap=$args")
      this.envEnum = EnumUtils.matchEnum(args.getOrElse("env", "local"), PureConfigEnvEnum).get
      val initialLoad: Boolean = args.getOrElse("initial-load", "false").toBoolean
      val pubsubAvroAnalytics: String =
        args.getOrElse("pubsub-avro-analytics", mediationConfig.mediation.pubsubAvroAnalytics)
      val pubSubAvro: String = args.getOrElse("pubsub-avro", mediationConfig.mediation.pubsubAvro)
      // val streamingWindow: Long = args.getOrElse("stream-window", mediationConfig.mediation.berWindow).toLong
      // val windowType: String = args.getOrElse("window-type", "global")
      log.info(s"mediationConfig=$mediationConfig")

      // 2) Loading "historical" notifications from GCS (result from YOUR_ENDPOINT, e.g: last 7 days)
      val optionalOldBers =
        if (initialLoad)
          getOldAvrosFromGCS(
            s"gs://${mediationConfig.gcsBucket}/${mediationConfig.mediation.gcsHistoricalRelativePath}/",
            mediationConfig.mediation.initialLoadBersDays,
            sc
          )
        else None

      // 3) new avro "fresh" notifications
      val avroBers = PubSubConsumer.readGenericAvroScio[MyEventRecord](pubSubAvro, PubsubIO.ReadParam(PubsubIO.Subscription))

      // 4) duplicate prevention
      val allDistinctKoAndNotSentBers: (SCollection[MyEventRecord], SCollection[(String, MyEventRecord)]) =
        if (optionalOldBers.isDefined) {
          log.info(s"expiringHistoricalTime=$expiringHistoricalTime")
          val oldGcs = optionalOldBers.get
          oldGcs.count.map(oldGcsCount => log.info(s"oldGcsCount=$oldGcsCount"))
          oldGcs.map { oldBer =>
            log.debug(s"GCS_BER=$oldBer")
          }
          val sideGcs = mapWithIdempotentKeyAndGlobalWindow(
            optionalOldBers.get,
            getIdempotentNotificationKey,
            newBerWithInitalLoadEventId,
            applyDistinctByKey = true
          ).distinctByKey.asMapSingletonSideInput
          getNonDuplicatedNotificationPubSubAndGcs(avroBers, sideGcs)
        } else {
          log.info(s"GCS_BER=EMPTY_HISTORICAL")
          val pubSub = mapWithIdempotentKeyAndGlobalWindow(avroBers, getIdempotentNotificationKey, winOptions = Some(windowOptions))
          val okKoBers = okAndKoBers(pubSub.values)
          (okKoBers._1, pubSub)
        }
      val koBers = allDistinctKoAndNotSentBers._1
      val okNotSentBers = allDistinctKoAndNotSentBers._2.distinctByKey.map { record =>
        KV.of(record._1, record._2)
      }
      // KO in GCS
      val windowedKoBers = toIntervalWindowAndIterable[MyEventRecord](30, koBers)
      windowedKoBers.count.map(koBersCount => log.info(s"sinking in GCSkoBers=$koBersCount"))
      SinkUtils.sinkAvroInGCS[MyEventRecord](windowedKoBers, s"gs://${mediationConfig.gcsBucket}/toxic/")

      // 5) Key & State => HttpResponse
      okNotSentBers.count.map(okNotSentBersCount => log.info(s"okNotSentBersCount=$okNotSentBersCount"))
      val notificationsForAnalytics = bersAfterHttpResponse(applyBerKVState(berKVState, okNotSentBers))

      // 6) Sink YOUR_ENDPOINT Notifications in PubSub: for analytics and InitialLoad
      log.info(s"Sinking YOUR_ENDPOINT NOTIFICATION topic=$pubsubAvroAnalytics")
      // PubSubProducer.writeGenericAvroScio[MyEventRecord](notificationsForAnalytics, pubsubAvroAnalytics)

      sc.run()
    } catch {
      case e: Exception => log.error("Mediation exception", e)
    }
  }

  def windowOptions = com.spotify.scio.values.WindowOptions(
    allowedLateness = Duration.ZERO,
    trigger = Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()),
    // trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
    // _.plusDelayOf(Duration.standardSeconds(2),
    accumulationMode = DISCARDING_FIRED_PANES,
    timestampCombiner = TimestampCombiner.LATEST
  )

  /**
   * This is a workaround for avoiding duplicates in fresh data (PubSub) against historical. A potential option is to load historical
   * records from Pubsub, so that KV State and Timer can be applied.
   * @param pubSubFreshData
   * @param historicalSideInputGcs
   * @return
   */
  def getNonDuplicatedNotificationPubSubAndGcs(
    pubSubFreshData: SCollection[MyEventRecord],
    historicalSideInputGcs: SideInput[Map[String, MyEventRecord]]
  ): (SCollection[MyEventRecord], SCollection[(String, MyEventRecord)]) = {
    log.info(s"loading historicalSideInputGcs for avoiding duplicates...")
    def isGcsHistoricalTimeExpired = new DateTime(jodaNowGetMillis).getMillis > expiringHistoricalTime.getMillis
    val pubSubRecords: SCollection[(String, MyEventRecord)] =
      mapWithIdempotentKeyAndGlobalWindow(pubSubFreshData, getIdempotentNotificationKey, winOptions = Some(windowOptions))
    val okKoBers: (SCollection[MyEventRecord], SCollection[MyEventRecord]) = okAndKoBers(pubSubRecords.values)
    lazy val newBers = {
      lazy val newBerAgainstGcs = pubSubRecords
        .withSideInputs(historicalSideInputGcs)
        .map { (kvBer, sideInputContext) =>
          lazy val (idempotentKey, pubSubBer) = kvBer
          if (!isGcsHistoricalTimeExpired) {
            val gcsBerMap = sideInputContext(historicalSideInputGcs)
            val duplicatedBerFromGcs = gcsBerMap.get(idempotentKey)
            duplicatedBerFromGcs.isEmpty match {
              case false => Left((idempotentKey, duplicatedBerFromGcs.get)) // duplicated
              case true  => Right((idempotentKey, pubSubBer))
            }
          } else Right((idempotentKey, pubSubBer))
        }
        .toSCollection

      lazy val duplicatedBer +: newBer +: Nil = newBerAgainstGcs.partition(
        2,
        { btrOrError =>
          btrOrError match {
            case Left(_)  => 0
            case Right(_) => 1
          }
        }
      )
      lazy val duplicatedBers = duplicatedBer.map(_.left.get).withName("duplicatedBer")
      duplicatedBers.map { old =>
        log.info(
          s"---- isExpired=$isGcsHistoricalTimeExpired? case DUPLICATE: there is ${old._2.getEvent.getId}, idempotent_key=${old._1}, nHubSuccess=${old._2.getNotification.getNhubSuccess}"
        )
      }
      newBer.map(_.right.get).withName("New BERs")
    }
    (okKoBers._1, newBers)
  }

  def mapWithIdempotentKeyAndGlobalWindow(
    windowedAvroBers: SCollection[MyEventRecord],
    transformationForBerKey: MyEventRecord => String,
    transformationForBer: MyEventRecord => MyEventRecord = identity,
    winOptions: Option[WindowOptions] = None,
    applyDistinctByKey: Boolean = false
  ): SCollection[(String, MyEventRecord)] = {
    val result = windowedAvroBers.map(record => (transformationForBerKey(record), record))

    // distinctByKey minimizes duplicates
    val distinctResult = if (applyDistinctByKey) result.distinctByKey else result

    val kvResult = distinctResult
      .map(record =>
        (record._1, transformationForBer(record._2))
      ) // distinct does not work here, due to: ParDo requires its input to use KvCoder in order to use state and timers

    // window for keeping State among elements
    winOptions match {
      case Some(opt) => kvResult.withGlobalWindow(opt)
      case None      => kvResult.withGlobalWindow()
    }
  }

  def okAndKoBers(bers: SCollection[MyEventRecord]): (SCollection[MyEventRecord], SCollection[MyEventRecord]) = {
    val ko +: ok +: Nil = bers
      .map { record =>
        isBerValid(record) match {
          case false => Left(record)
          case true  => Right(record)
        }
      }
      .partition(
        2,
        { avro =>
          avro match {
            case Left(_)  => 0
            case Right(_) => 1
          }
        }
      )
    (ko.map(_.left.get), ok.map(_.right.get))
  }

  def applyBerKVState(
    berKVState: StateAsyncParDoWithHttpHandler,
    windowedBers: SCollection[KV[String, MyEventRecord]]
  ): SCollection[StateAndTimerType.KVOutputBerAndHttpResponse] = windowedBers.applyTransform(ParDo.of(berKVState))

  def bersAfterHttpResponse(httpResponse: SCollection[StateAndTimerType.KVOutputBerAndHttpResponse]): SCollection[(NotificationResponse, MyEventRecord)] =
    httpResponse
      .map { m =>
        val nhubResponse = m.getValue
        log.debug(s"NOTIFICATION_RESPONSE=$nhubResponse")
        val successAndFullDescr = isSuccessAndFullResultDescr(nhubResponse)
        val notificationAnalytics = newEventRecordWithSuccess(m.getKey, successAndFullDescr._1, Some(successAndFullDescr._2), None)
        log.info(s"NOTIFICATION_ANALYTICS=$notificationAnalytics}")
        (nhubResponse, notificationAnalytics)
      }

  def getOldAvrosFromGCS(absoluteAvroPath: String, initialLoadBersDays: Integer, sc: ScioContext) = {
    val dates = getLastDateDaysFrom(initialLoadBersDays)
    dates.map(date => log.info(s"avro_date=$date"))

    val checkAvroPaths = dates.map { relativePath =>
      val fullAbsolutePath = s"$absoluteAvroPath$relativePath/"
      log.info(s"gcsBlobExists() fullAbsolutePath=$fullAbsolutePath")
      (
        fullAbsolutePath,
        GCSCommonUtilsInterim.fileOrDirExists(StorageOptions.getDefaultInstance.getService, fullAbsolutePath)
      )
    }

    val okAvroFullPaths = checkAvroPaths
      .map { avroPath =>
        log.info(s"filtering '${avroPath._2}' gcsBlobExists(): $avroPath")
        avroPath
      }
      .filter(exists => exists._2)

    val okAvroPathsWithAllAvros = okAvroFullPaths.map { okFullPath =>
      val okExtensions =
        areAllFilesWithExtensionInGCS(StorageOptions.getDefaultInstance.getService, okFullPath._1, ".avro")
      log.info(s"okPath, '$okExtensions' Avros in ${okFullPath._1}")
      (okFullPath._1, okExtensions)
    }

    // old avros
    if (okAvroPathsWithAllAvros.size > 0) {
      log.info(s"OK old partitioned avros")
      val indexedSeqOldAvros = okAvroFullPaths.filter(allAvrosInPath => allAvrosInPath._2).map { okPath =>
        sc.avroFile[MyEventRecord](s"${okPath._1}*")
      }
      Some(sc.unionAll(indexedSeqOldAvros))
    } else {
      log.info(s"KO old partitioned avros. Resorting to absolutePath=$absoluteAvroPath")
      val okAvros =
        areAllFilesWithExtensionInGCS(StorageOptions.getDefaultInstance.getService, absoluteAvroPath, ".avro")
      okAvros match {
        case true => Some(sc.avroFile[MyEventRecord](s"$absoluteAvroPath*"))
        case false =>
          log.warn(s"Mediation needs historical BERs (unless first execution)")
          None
      }
    }
  }

  /* SSL config */
  case class MySslConfig(sslConfigPath: SslConfigPath, gcpProject: String) extends SslConfigHelper
}
