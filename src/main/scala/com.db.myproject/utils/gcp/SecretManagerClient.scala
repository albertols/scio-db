package com.db.myproject.utils.gcp

import com.google.protobuf.ByteString
import com.google.cloud.secretmanager.v1.{SecretManagerServiceClient, SecretVersionName}

import java.util.concurrent.TimeUnit

class SecretManagerClient(gcpProjectId: String,
                          awaitTerminationDuration: Long = 5,
                          awaitTerminationUnit: TimeUnit = TimeUnit.MINUTES) extends AutoCloseable {

  private val secretManager: SecretManagerServiceClient = SecretManagerServiceClient.create()

  def getSecretData(secretId: String): ByteString = {
    val secretVersionName = SecretVersionName.of(gcpProjectId, secretId, "latest")
    val secret = secretManager.accessSecretVersion(secretVersionName)
    secret.getPayload.getData
  }

  def getSecretString(secretId: String): String = {
    if (secretId.nonEmpty) {
      getSecretData(secretId).toStringUtf8.trim
    } else ""
  }

  override def close(): Unit = {
    secretManager.shutdown()
    secretManager.awaitTermination(awaitTerminationDuration, awaitTerminationUnit)
  }

}
