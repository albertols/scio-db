package com.db.myproject.utils.kafka

import com.db.myproject.configs.ParentConfig
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.values.KV
import org.apache.kafka.common.serialization.StringDeserializer
import scala.collection.JavaConverters._
import org.slf4j.{Logger, LoggerFactory}

object BeamKafkaConsumer {

  val log: Logger = LoggerFactory getLogger getClass.getName

  def readMessages(sc: ScioContext, config: ParentConfig, topics: List[String], otherConfig: Map[String, Object]): SCollection[KV[String, String]] = {
    val consumerProp: Map[String, Object] = config.kafka.consumerProps
    log.info("ReadingFromKafka...")
    log.info(s"consumerProp=$consumerProp")
    log.info(s"otherConfig=$otherConfig")
    log.info(s"config.kafka.brokers=${config.kafka.brokers}")
    log.info(s"topics=$topics")
    sc.customInput(
      "ReadFromKafka",
      KafkaIO
        .read[String, String]
        .commitOffsetsInFinalize()
        .withBootstrapServers(config.kafka.brokers)
        .withTopics(topics.asJava)
        .withConsumerConfigUpdates(consumerProp.asJava)
        .withConsumerConfigUpdates(otherConfig.asJava)
        .withKeyDeserializer(classOf[StringDeserializer])
        .withValueDeserializer(classOf[StringDeserializer])
        .withoutMetadata
    )
  }

}
