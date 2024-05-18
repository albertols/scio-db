package com.db.myproject.kryo

import com.twitter.chill._
import io.circe
import org.slf4j.{Logger, LoggerFactory}

/** https://spotify.github.io/scio/internals/Kryo.html */
//@KryoRegistrar
class MyKryoRegistrar extends IKryoRegistrar {
  val log: Logger = LoggerFactory getLogger getClass.getName

  override def apply(k: Kryo): Unit = {
    // Take care of common Scala classes; tuples, Enumerations, ...
    val reg = new AllScalaRegistrar
    reg(k)
    log.info(s"Registering Kryo classes")
    k.registerClasses(
      List(
        // All classes that might be shuffled, e.g.:
        classOf[circe.Error],

        // Class that takes type parameters:
//      classOf[_root_.java.util.ArrayList[_]],
        // But you can also explicitly do:
//      classOf[Array[Byte]],

        // Private class; cannot use classOf:
//      Class.forName("com.spotify.scio.extra.sparkey.LocalSparkeyUri"),

        // Some common Scala objects
        None.getClass,
        Nil.getClass
      )
    )
  }
}
