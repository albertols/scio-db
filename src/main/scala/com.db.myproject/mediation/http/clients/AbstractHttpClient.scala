package com.db.myproject.mediation.http.clients

import com.db.myproject.mediation.http.StateAndTimerType

trait AbstractHttpClient {
  def sendPushWithFutureResponse(record: StateAndTimerType.OutputBer): StateAndTimerType.FutureKVOutputBerAndHttpResponse

}
