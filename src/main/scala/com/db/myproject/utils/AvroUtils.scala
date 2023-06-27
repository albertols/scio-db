package com.db.myproject.utils

import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.reflect.ReflectData
import org.apache.commons.io.output.ByteArrayOutputStream

import scala.util.Try

object AvroUtils {
  def avroToBytes[T](avroObjects: Iterable[T]) = {
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
      avroObjects.foreach(dataFileWriter.append(_))

      // Close the data file writer to flush any buffered data
      dataFileWriter.close()

      // Get the Avro bytes from the output stream
      outputStream.toByteArray
    }
  }

}
