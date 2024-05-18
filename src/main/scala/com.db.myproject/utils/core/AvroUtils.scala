package com.db.myproject.utils.core

import org.apache.avro.io._
import org.apache.avro.specific.{SpecificDatumWriter, SpecificRecordBase}
import java.io.ByteArrayOutputStream
import java.io.IOException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.dataformat.avro.{AvroFactory, AvroSchema}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.reflect.ReflectData
import scala.util.Try


object AvroUtils {

  def avroToCleanJSON[T <: GenericRecord](input: T): Try[ObjectNode] = {
    Try {
      //using jackson databind library to get clean JSON
      val mapper = new ObjectMapper(new AvroFactory())
      val avroSchema = input.getSchema
      Using.tryWithResources(new ByteArrayOutputStream()) {
        outputStream: ByteArrayOutputStream =>
          val datumWriter = new GenericDatumWriter[T](avroSchema)
          val encoder = EncoderFactory.get().binaryEncoder(outputStream, null)
          datumWriter.write(input, encoder)
          encoder.flush()

          val bytes = outputStream.toByteArray

          mapper.readerFor(classOf[ObjectNode])
            .`with`(new AvroSchema(avroSchema))
            .readValue[ObjectNode](bytes)
      }
    }
  }

  def avroToBytes[T](avroObjects: Iterable[T]): Try[Array[Byte]] = {
    Try {
      // Define the Avro schema
      val schema: Schema = ReflectData.get().getSchema(avroObjects.head.getClass)

      // Serialize the Avro object to bytes
      val datumWriter = new GenericDatumWriter[T](schema)
      val outputStream = new ByteArrayOutputStream()
      val dataFileWriter = new DataFileWriter[T](datumWriter)

      // Call create method with the schema and output stream
      dataFileWriter.create(schema, outputStream)

      // Append the Avro object to the output stream
      avroObjects.foreach(dataFileWriter.append)

      // Close the data file writer to flush any buffered data
      dataFileWriter.close()

      // Get the Avro bytes from the output stream
      outputStream.toByteArray
    }
  }


}

