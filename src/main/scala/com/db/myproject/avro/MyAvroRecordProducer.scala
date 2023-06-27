package com.db.myproject.avro

import com.db.myproject.avro.records.business.accounts._
import com.db.myproject.avro.BBWindows.bbFixedWindow
import com.db.myproject.avro.BTRWindows.{applyWindowToPart, bigBoardAsSideInputJoin, btrHashJoin, btrHashJoin2}
import com.db.myproject.avro.MyLookUpTable.MyLookUpTableRows
import com.db.myproject.configs.RootConfig.readConfigFromString
import com.db.myproject.configs.{ParentConfig, RootConfig}
import com.db.myproject.model.parsers.CDHJsonParserCirce
import com.db.myproject.model.tables.MyRtTable.MyRtRecords
import com.db.myproject.utils.AvroUtils
import com.db.myproject.utils.TimeUtils.windowStartAndEndFormatString
import com.db.myproject.utils.kafka.BeamKafkaConsumer.readMessages
import com.db.myproject.utils.pubsub.PubSubConsumer
import com.google.cloud.storage.Blob
import com.spotify.scio.bigquery.{CREATE_IF_NEEDED, Table, WRITE_APPEND, bigQueryScioContextOps}
import com.spotify.scio.coders.Coder
import com.spotify.scio.parquet.types._
import com.spotify.scio.streaming.{ACCUMULATING_FIRED_PANES, DISCARDING_FIRED_PANES}
import com.spotify.scio.values.{SCollection, WindowedSCollection}
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver
import org.apache.beam.sdk.transforms.{Create, DoFn, PTransform, ParDo, PeriodicImpulse}
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.util.MimeTypes
import org.apache.beam.sdk.values.PCollection
import org.slf4j.{Logger, LoggerFactory}

import java.io.OutputStream
import java.nio.channels.Channels
import java.time.Instant
import java.util.Locale
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object MyAvroRecordProducer {
  type T = MyAvroRecord
  // DONE 0: detached app.conf in GCS
  // TODO 1.1: read from GS: gs://use_case_utils.csv
  // DONE 1.2: read big_board from GS: gs://big_board.parquet
  // DONE 2: read from MyRtRecord and compose a SCollection[MyRtRecordRows]
  // DONE 2.1: composing a SCollection[MyAvroRecord]
  // TODO 2.1: finishing off a fully mapped SCollection[MyAvroRecord]
  // DONE 2.2: sinking toxic MyRtRecord input messages into GCS
  // TODO 2.3:  another or same Window, e.g (for counts): btrAvros.countByValue.map { case (btr: MyAvroRecord, c: Long) => log.info(s"count_btr= $c") }
  // DONE 3.0: (local) read the big_board using BigQueryType macros
  // WIP: read as sideInput (Beam) the big_board (.parquet from gs:// or from resource/local until is done)
  // WIP: BJH between MyRtRecord & and big_board.parquet
  // DONE 5: Produce BTRAccountAvro record in Pubsub topic
  // DONE 5.1: Sink BTRAccountAvro record in GCS
  // TODO 5.2: Sink BTRAccountAvro record in GCS (partitioned)
  // DONE 5.3: Sink BTRAccountAvro record in GCS (with PubSub does not trigger windowing and does not sink into GCS (local))
  /* TODO 6: hashjoin 2023/06/05 14:29:33.014 [main] WARN  com.spotify.scio.values.SCollection$: 'groupBy' groupBy will materialize all values for a key to a single worker,
      which is a very common cause of memory issues. Consider using aggregateByKey/reduceByKey on a keyed SCollection instead.*/

  val log: Logger = LoggerFactory getLogger getClass.getName
  implicit val coderLocale: Coder[Locale] = Coder.kryo[Locale]
  lazy val resourceConfig: ParentConfig = RootConfig.apply(env)
  var env: String = ""

  def rawStringConfigFromGcs(project: String, bucket: String, blobPath: String): Option[String] = {
    import com.google.cloud.storage.StorageOptions
    val storage = StorageOptions.newBuilder().setProjectId(project).build().getService()
    val blob: Blob = storage.get(bucket, blobPath)
    if (null == blob) None
    else Some(new String(blob.getContent()))
  }

  // application.conf from GCS or resources/
  def readConfig = rawStringConfigFromGcs(
    resourceConfig.gcp.project,
    resourceConfig.gcsBucket,
    resourceConfig.btr.configBlobPath
  ) match {
    case Some(conf) => readConfigFromString(conf, env)
    case None       => resourceConfig
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    try {
      log.info("Init SCIO Context...")
      val (sc, args) = ContextAndArgs(cmdlineArgs)
      implicit val scioContext: ScioContext = sc

      // --input args
      log.info(s"SCIO args=$args")
      this.env = args.getOrElse("env", "dev")
      val config = readConfig
      val pubSubs: String = args.getOrElse("pubsub-sub", config.btr.pubsubSub)
      val sinkTopic: String = args.getOrElse("sink-topic", config.btr.sinkTopic)
      val consumerType: String = args.getOrElse("consumer-type", "pubsub")
      val errorTable: String = args.getOrElse("error-table", config.btr.inputTopic.bqTableError)
      val bigBoardType: String = args.getOrElse("big-board-type", "parquet")
      val bigBoardParquet: String = args.getOrElse("big-board-parquet", s"gs://${config.btr.bigBoardParquet}/*")
      val joinStrategy: String = args.getOrElse("join-strategy", "bbside")
      val bbWindow: String = args.getOrElse("bb-window", "unique")
      val btrSinkPath: String = args.getOrElse("btr-sink-path", config.btr.btrSinkPath)
      log.info(config.toString)

      // reading bigboard
      val lookUpTable = bigBoardType match {
        case "parquet" =>
          sc.typedParquetFile[MyLookUpTableRows](bigBoardParquet)
        case "bigquery" =>
          sc.typedBigQuery[MyLookUpTableRows](Table.Spec(s"${config.btr.bigBoardName}")).withName("BigQuery BB")
      }

      // raw messages (with Window)
      val rawMsgs = consumerType.toLowerCase match {
        case "pubsub" => PubSubConsumer.getMessagesFromPubSubScio(pubSubs)
        case "kafka" =>
          readMessages(
            sc,
            resourceConfig,
            List(resourceConfig.btr.inputTopic.topic),
            Map("group.id" -> resourceConfig.btr.inputTopic.groupId)
          )
            .map(_.getValue.trim)
        case _ => throw new Exception(s"Unknown consumerType=$consumerType")
      }
      val winRawMsgs = applyWindowToPart("MyRtRecord Window", rawMsgs, org.joda.time.Duration.standardSeconds(1))
//      val winRawMsgs = applyWindowToPart("MyRtRecord Window", rawMsgs, org.joda.time.Duration.standardDays(1))
//      val pi = PeriodicImpulse.create()
//        .startAt(org.joda.time.Instant.now())
//        .withInterval(org.joda.time.Duration.standardSeconds(1))
//        .applyWindowing()
//      winRawMsgs
//        .internal
//        .apply("",pi)


      // parsing MyRtRecords and producing BTRAccountAvros
      val (myRtRows: SCollection[MyRtRecords], bqErrors) = new CDHJsonParserCirce[MyRtRecords].parseJSONStrings(winRawMsgs)

      // toxic MyRtRecords
      bqErrors.saveAsTypedBigQueryTable(Table.Spec(errorTable), null, WRITE_APPEND, CREATE_IF_NEEDED)

      //val bbTs = lookUpTable.timestampBy(_ => org.joda.time.Instant.now())
      val bbTs = bbFixedWindow(lookUpTable)
      val keyedBigBoard = bbWindow match {
        case "unique" => keyAndBigBoard (bbTs).distinctByKey
        case "default" => keyAndBigBoard (bbTs)
        case _ => keyAndBigBoard (bbTs)
      }
//      keyedBigBoard.take(5).map(k => log.info(s"gbk_bb=$k"))

      // (WIP) JOIN MyRtRecord & BigBoard
      val btrAvros = joinStrategy match {
        case "hash" =>btrHashJoin(myRtRows, keyedBigBoard, sc)
        case "bbside" =>bigBoardAsSideInputJoin(myRtRows, keyedBigBoard, sc)
        case "winhash" =>btrHashJoin2(myRtRows, keyedBigBoard.toWindowed, sc)
        case _ =>btrHashJoin(myRtRows, keyedBigBoard, sc)
      }

      // sinking in PubSub
      btrAvros.saveAsCustomOutput("avro-producer PubSub", sinkAvrosInPubSub(sinkTopic))

      // sinking in GCS (window for unbounded SCollection) a.k.a BTRSink
//      sinkAvroInGCS[T](transformWithWindow[T](config.btr.btrSinkWindow, btrAvros), btrSinkPath, sc)
      sc.run()
    } catch {
      case e: Exception => log.error("avro-producer exception", e)
    }
  }

  def keyAndBigBoard (bigBoard: SCollection [MyLookUpTableRows]) ={
    val keyedBigBoard = bigBoard
      .map { row =>
        val acc = row.ACCOUNT_NO_1
        val bank = row.BANK_ID
        val branch = row.BRANCH_NO_MAIN
        (acc.toString + bank.toString + branch.toString, row)
      }.withName("Keyed BB")
    keyedBigBoard
  }

  def myRtRecordToAvro(myRtRecord: MyRtRecords) = {
    log.info(s"new MyRtRecord => ts_cdc=${myRtRecord.TIMESTAMP_CDC}, acc=${myRtRecord.ACCOUNT}")
    // Event
    val e = event.newBuilder.build
    //e.setId(myRtRecord.TRADE_KEY)
    // EventType
    val et = eventType.newBuilder.build
    // Customer
    val cu = customer.newBuilder.build
    cu.setFullName("Tenerife Laznarote")
    cu.setPhoneNumber("Miming Clown")
    cu.setEmail("grannysemail@seteolvidaeltapon.com")
    cu.setCustomerDeviceId("Cartón S el Chungo")
    val gdpr = consentGDPR.newBuilder().build()

    // TNA
    val tn = tna.newBuilder.build

    // AccountMovement
    val a = accountMovement.newBuilder.build
    // MyRtRecord
    MyAvroRecord
      .newBuilder()
      .setEvent(e)
      .setEventType(et)
      .setCustomer(cu)
      .setAccountMovement(a)
      .setTna(tn)
      .build()
  }

  def sinkAvrosInPubSub(topic: String) = org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
    .writeAvros[T](classOf[T])
    .to(topic)

  /**
   * Loading Avro into GCS ,from KafkaIO or unbounded PCollection prodcues : "java.lang.IllegalArgumentException: Must
   * use windowed writes when applying WriteFiles to an unbounded PCollection"
   *
   * @param records,
   *   we should now have a collection of objects, grouped in windows. We will write each window in a separate file in
   *   GCS, encoded as Avro.
   * @param location
   * @param sc
   * @param c
   * @tparam T
   * @return
   */
  def sinkAvroInGCS[T](records: SCollection[(IntervalWindow, Iterable[T])], location: String, sc: ScioContext)(implicit
    c: Coder[T]
  ) = {
    log.info(s"Initialize GCS `FileSystem` abstraction")
    FileSystems.setDefaultPipelineOptions(sc.options)
    records
      .map { case (w: IntervalWindow, msgs: Iterable[T]) =>
        val outputShard = location + windowStartAndEndFormatString(w.start, w.end) + ".avro"
        val resourceId: ResourceId = FileSystems.matchNewResource(outputShard, false)
        val out: OutputStream = Channels.newOutputStream(FileSystems.create(resourceId, MimeTypes.BINARY))
        log.info(s"avroToGCS_count=${msgs.size}")
        AvroUtils.avroToBytes(msgs) match {
          case Success(avroBytes) => out.write(avroBytes)
          case Failure(ex)        => log.error(s"Failing converting avroToBytes for OutputStream", ex)
        }
        out.close()
      }
      .withName("Sink Avro into GCS")
  }

}
