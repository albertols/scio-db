package com.db.myproject.mediation.http

import com.db.myproject.mediation.avro.MyEventRecord
import com.db.myproject.mediation.nhub.model.MyHttpResponse.NotificationResponse
import org.apache.beam.sdk.values.KV

import scala.concurrent.Future

object StateAndTimerType {
  /* INPUTS for State */
  type InputIdempotentString = String
  type InputBer = MyEventRecord
  type KVInputStringAndBer = KV[InputIdempotentString, InputBer]

  /* OUTPUTS with Timer */
  type OutputBer = InputBer
  type OutputHttpResponse = NotificationResponse
  type KVOutputBerAndHttpResponse = KV[OutputBer, OutputHttpResponse]
  // needed due to StateBaseAsyncDoFn
  type FutureKVOutputBerAndHttpResponse = Future[KVOutputBerAndHttpResponse]
}
