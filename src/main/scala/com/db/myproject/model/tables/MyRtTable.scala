package com.db.myproject.model.tables

import com.db.myproject.model.tables.utils.TableUtils
import com.db.myproject.utils.TimeUtils.parseStringToInstant
import com.spotify.scio.bigquery.types.BigQueryType
import io.circe.generic.JsonCodec
import org.joda.time.Instant

/**
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
 * https://spotify.github.io/scio/api/com/spotify/scio/bigquery/types/BigQueryType$.html
 * https://spotify.github.io/scio/FAQ.html#how-to-make-intellij-idea-work-with-type-safe-bigquery-classes-
 * https://spotify.github.io/scio/io/BigQuery.html
 */
object MyRtTable {
  @JsonCodec
  @BigQueryType.toTable
  case class MyRtRecords(
                          BANK: Int,
                          BRANCH: Int,
                          ACCOUNT: Long,
                          TIMESTAMP_CDC: String,
                          OPERATION_TYPE_CDC: String,
                          TABLENAME_CDC: String
                    )

}
