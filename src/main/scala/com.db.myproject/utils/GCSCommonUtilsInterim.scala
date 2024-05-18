package com.db.myproject.utils

/**
 * scala-utils would be its home
 */
object GCSCommonUtilsInterim {

  import com.google.cloud.storage.{Storage}
  import java.net.URI

  def areAllFilesWithExtensionInGCS(storage: Storage, path: String, extension: String): Boolean = {
    // Parse the GCS URI to get the bucket name and the prefix (path without the 'gs://' and bucket name)
    val uri = new URI(path)
    val bucketName = uri.getAuthority
    // Remove the leading '/' from the path
    val prefix = if(uri.getPath.endsWith("/")) uri.getPath.substring(1) else uri.getPath

    // Retrieve the blobs in the specified bucket with the given prefix
    val blobs = storage.list(bucketName, Storage.BlobListOption.prefix(prefix)).iterateAll()

    // Iterate through blobs to check for .extension
    import scala.collection.JavaConverters._
    blobs.asScala.forall(blob => blob.getName.endsWith(extension))
  }

  /**
   * Similar to com.db.myproject.utils.gcp..gcsBlobExists
   * But it works!
   * @param storage
   * @param fullAbsolutePath
   * @return
   */
  def fileOrDirExists(storage: Storage, fullAbsolutePath: String): Boolean = {
    val uri = new URI(fullAbsolutePath)
    val bucketName = uri.getAuthority
    val prefix = if(uri.getPath.startsWith("/")) uri.getPath.substring(1) else uri.getPath
    val blobs = storage.list(bucketName, Storage.BlobListOption.prefix(prefix)).iterateAll()
    import scala.collection.JavaConverters._
    !blobs.asScala.isEmpty
  }

}
