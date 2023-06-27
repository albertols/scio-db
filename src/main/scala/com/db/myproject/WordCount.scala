package com.db.myproject

import com.db.myproject.model.BusinessEventRecord
import com.db.myproject.utils.ResourceReader
import com.spotify.scio._
import org.apache.avro.specific.SpecificRecord
import org.slf4j.{Logger, LoggerFactory}

/*
sbt "runMain myproject.WordCount --input=src/main/resources/dev/sweet.txt --project=pe-btr --runner=DirectRunner --output=wordcount"
 */
object WordCount {

  protected val log: Logger = LoggerFactory getLogger getClass.getName

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val exampleData = "src/main/resources/dev/sweet.txt" // gs://my_parquet
    val input = args.getOrElse("input", exampleData)
    val output = args("output")

    sc.textFile(input)
      .map(_.trim)
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map(t => t._1 + ": " + t._2)
      .saveAsTextFile(output)

    import com.spotify.scio.avro._
    def result_avro = sc.avroFile[SpecificRecord]("src/main/resources/dev/SDPAccount.avro")

    // result_avro.map(record => log.info(s"Avro Entry: $record"))
    val result = sc.run().waitUntilFinish()
  }
}
