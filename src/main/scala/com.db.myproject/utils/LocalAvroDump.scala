package com.db.myproject.utils

import com.db.myproject.mediation.avro.MyEventRecord
import com.db.myproject.mediation.avro.MyEventRecordUtils.{mockTransactionId, newBerWithLastNHubTimestamp}
import com.db.myproject.mediation.testing.NotificationsMockData.{null_nhub_debit_abuela, null_nhub_debit_quique, true_nhub_debit_quique}
import com.db.myproject.streaming.utils.SinkUtils
import com.db.myproject.utils.time.TimeUtils.jodaNowGetMillis
import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.values.SCollection

/**
 * TO BE IMPROVED: internal local tooling for quickly generating
 */
object LocalAvroDump {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    //val avros: SCollection[MyEventRecord] = sc.parallelize(Seq(null_nhub_debit_quique))
    //SinkUtils.saveAsAvroFile(avros, "src/main/resources/mock/avro/", "null_nhub_debit_quique", 1, ".avro")
    val avros: SCollection[MyEventRecord] = sc.parallelize(mockManyRecordsSeq(1, null_nhub_debit_abuela, false))
    SinkUtils.saveAsAvroFile(avros, "src/main/resources/mock/avro/", "null_nhub_debit_abuela", 1, ".avro")
    sc.run().waitUntilDone()
  }

  def mockManyRecordsSeq (number: Long, record:MyEventRecord, keepRaw: Boolean) = {
    Seq.fill(number.toInt)(keepRaw match {
      case false => newBerWithLastNHubTimestamp(mockTransactionId(record), jodaNowGetMillis)
      case true => record
    })
  }
}
