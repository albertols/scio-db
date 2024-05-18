package com.db.myproject.mediation.http.state

import com.spotify.scio.transforms.ScalaFutureHandlers

import scala.concurrent.Future

/**
 * A [[org.apache.beam.sdk.transforms.DoFn DoFn]] that handles asynchronous requests to an external service that returns
 * Scala [[Future]] s.
 */
abstract class StateScalaAsyncDoFn[I, O, R]
    extends StateBaseAsyncDoFn[I, O, R, Future[O]]
    with ScalaFutureHandlers[O] {}
