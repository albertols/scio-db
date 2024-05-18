package com.db.myproject.utils.gcp

import com.google.cloud.storage._

object GCSCommonUtils {
  def rawStringConfigFromGcs(project: String, bucket: String, blobPath: String): scala.Option[String] = {
    val storage = StorageOptions.newBuilder().setProjectId(project).build().getService()
    val blob: Blob = storage.get(bucket, blobPath)
    if (null == blob) None
    else Some(new String(blob.getContent()))
  }


}
