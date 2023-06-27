package com.db.myproject.avro

import com.spotify.scio.bigquery.types.BigQueryType

object MyLookUpTable {

  @BigQueryType.toTable
  case class MyLookUpTableRows(
                                BANK: Long,
                                ACC: Long,
                                BRANCH: Long,
                                FULLNAME: String,
                                LANGUAGE: String
  ){
   // def this () = this (None, 0, "", 0,0, "","", false, false, false, None, None, false)
  }

  /**
   * WIP: (DataFlow) read the big_board using BigQueryType macros:
   * https://stackoverflow.com/questions/76267480/scio-dataflow-error-message-from-worker-java-lang-classcastexception-class-ca
   * TIP: set up in your sbt compiler server (local and build.yaml) -Dbigquery.project=my-project-id
   */
  //  @BigQueryType.fromTable("CDH_dataset.big_board_payroll")
  class MyLookUpTypedRows
}
