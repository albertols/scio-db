package com.db.myproject

import io.circe.generic.JsonCodec

package object model {

  case class BusinessEventRecord(
    event: Event,
    eventType: EventType,
    customer: Customer,
    notification: Notification,
    lastTryTimestamp: String,
    userWhiteList: String,
    alertWhiteList: Seq[Int]
  )

  case class Customer(
    id: String,
    fullName: String,
    phoneNumber: String,
    email: String,
    customerDeviceId: String,
    languageCommunication: String
  )

  case class Event(
    id: String,
    transactionId: String,
    db2Timestamp: Int,
    cdcTimestamp: Int,
    btrTimestamp: Int,
    droolsTimestamp: Int,
    orchestratorTimestamp: Int,
    hzTimestamp: String,
    nhubTimestamp: String,
    timestamp: Int,
    accountMovementTimestampEntry: Int,
    schemaVersion: String
  )

  case class EventType(
    family: Int,
    useCase: Int
  )

  case class Notification(
    idNum: Int,
    id: String,
    channel: Int,
    message: String,
    retryMinutes: String,
    retries: String,
    title: String,
    reference: String,
    additionalData: String,
    isPromiscuous: Boolean,
    nhubSuccess: String,
    amount: Double,
    successDescr: String,
    serviceTarget: String,
    ccaf1: String,
    ccaf2: String,
    ccaf3: String,
    ccaf4: String
  )

  case class MyRtRecord(
    BANK_ID: Int,
    BRANCH_NO_MAIN: Int,
    ACCOUNT_NO_1: Long,
    DATE_BOOKKEEPING: String,
    TRADE_KEY: String,
    // TRANSACTION_STATUS: Long,
    ENTRY_TS: String,
    SETTL_AMT_SETL_CCY: Long,
    DEC_PLACES_AMOUNT: Long,
    // MESS_CARRIER_ID: String,
    // REQUEST_DATE: String,
    TRANS_VALUE_DATE: String,
    DIRECTION: String,
    // ADDITIONAL_REF: String,
    // CHEQUE_NO: String,
    // FST_COLLEC_AGENT: Long,
    // TEXT_1: String,
    // TEXT_2: String,
    // FLAG_ROUTING: String,
    CURRENCY_ISO_CODE: String,
    TEXT_KEY_KK: Int,
    REFERENCE_1: String,
    // REFERENCE_2: String,
    // REFERENCE_3: String,
    // REFERENCE_4: String,
    SETTL_AMT_ORIG_CCY: Long,
    DEC_PL_ORIG_CCY: Long,
    // ORIG_CCY_ISO_CODE: Long,
    // ACCOUNT_BALANCE: String,
    // CHANGE_ENTITY: Long,
    // CHANGE_BRANCH: Long,
    // ORIG_APP_ID: String,
    // EXTERNAL_KEY: String,
    FLAG_CASH: String,
    // ACCOUNT_KEY: String,
    REFERENCE_5: String,
    // REFERENCE_DT: String,
    TIMESTAMP_CDC: String,
    OPERATION_TYPE_CDC: String,
    TABLENAME_CDC: String,
    ACCOUNT_BALANCE_CDC: AccountBalanceCdc
  )

  @JsonCodec
  case class AccountBalanceCdc(
    long: Int
  )

}
