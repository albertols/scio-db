package com.db.myproject.mediation.http.clients.akka

import akka.http.scaladsl.{ConnectionContext, HttpsConnectionContext}
import org.slf4j.{Logger, LoggerFactory}

import java.io.ByteArrayInputStream
import java.security.cert.X509Certificate
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

object AkkaSSLContextFromSecretManager extends Serializable {

  val log: Logger = LoggerFactory getLogger getClass.getName
  import com.db.myproject.mediation.http.clients.akka.AkkaHttpClient.sslConfig

  val httpsConnectionContext: HttpsConnectionContext = {
    log.info(s"createHttpsConnectionContext, sslConfig.truststoreLocation=${sslConfig.truststoreLocation}")
    val password = sslConfig.keystorePassword.trim

    import java.util.Base64

    // ks
    val keystoreStreamDecoded = new ByteArrayInputStream(Base64.getMimeDecoder.decode(sslConfig.keystoreSecretValue))

    // ts
    val truststoreStreamDecoded = new ByteArrayInputStream(
      Base64.getMimeDecoder.decode(sslConfig.truststoreSecretValue)
    )

    // Load keystore
    val ks = KeyStore.getInstance("PKCS12")
    ks.load(keystoreStreamDecoded, password.toString.toCharArray)
    log.info("KeyStore: PKCS12")
    printKs(ks)

    // Load truststore
    val ts = KeyStore.getInstance("JKS")
    ts.load(truststoreStreamDecoded, sslConfig.truststorePassword.toCharArray)
    log.info("KeyStore: KS")
    printKs(ts)

    // Set up key manager factory
    val kmf = KeyManagerFactory.getInstance("PKIX")
    kmf.init(ks, password.toString.toCharArray)
    log.info("KeyManagerFactory: PKIX (keystore)")

    // Set up trust manager factory
    val tmf = TrustManagerFactory.getInstance("SunX509")
    tmf.init(ts)
    log.info("KeyManagerFactory: SunX509 (truststore)")

    // Initialize SSL context
    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(kmf.getKeyManagers, tmf.getTrustManagers, new SecureRandom)
    ConnectionContext.https(sslContext)
  }

  def printKs(ks: KeyStore) = {
    val aliases = ks.aliases()
    while (aliases.hasMoreElements) {
      val alias = aliases.nextElement()
      log.info(s"Alias: $alias")
      val cert = ks.getCertificate(alias).asInstanceOf[X509Certificate]
      log.info(s"  Subject: ${cert.getSubjectX500Principal}")
      log.info(s"  Issuer: ${cert.getIssuerX500Principal}")
      log.info(s"  Valid from: ${cert.getNotBefore}")
      log.info(s"  Valid until: ${cert.getNotAfter}")
      log.info(s"  Serial number: ${cert.getSerialNumber}")
      log.info(s"  Signature algorithm: ${cert.getSigAlgName}")
      log.info(s"  Public key algorithm: ${cert.getPublicKey.getAlgorithm}")
      log.info(s"  Version: ${cert.getVersion}")
    }
  }

}
