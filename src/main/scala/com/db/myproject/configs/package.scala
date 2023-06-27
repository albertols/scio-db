package com.db.myproject

import com.google.api.services.bigquery.model.TableSchema
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, Method, WriteDisposition}

package object configs {

  case class ParentConfig(gcsBucket: String, kafka: KafkaConfig, gcp: Gcp, btr: Btr)
  case class KafkaConfig(
    topics: List[String],
    brokers: String,
    consumerProps: Map[String, String],
    tables: List[TableReplicaConf]
  )

  case class TableReplicaConf(
    table: String,
    replicate: Boolean,
    topic: String,
    groupId: String,
    bqTable: String,
    sinkBqTableError: Boolean,
    bqTableError: String
  )
  case class Gcp(project: String, database: String, narId: String)
  case class Btr(
    configBlobPath: String,
    pubsubSub: String,
    sinkTopic: String,
    btrSinkWindow: Long,
    btrSinkPath: String,
    bigBoardName: String,
    bigBoardParquet: String,
    inputTopic: TableReplicaConf
  )
}
