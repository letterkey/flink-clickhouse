package io.clickhouse.ext.flink.sink.utils

/**
  * Created by letterkey on 19-9-11 11:48.
  */
object ClickHouseConfig {

  val CLICKHOUSE_HOST ="clickhouse.host"
  val CLICKHOUSE_PORT = "clickhouse.port"
  val USERNAME = "clickhouse.username"
  val PASSWORD = "clickhouse.password"
  val DATABASE = "clickhouse.dbname"

  val TABLENAME = "clickhouse.tablename"

  val MAX_BUFFER_SIZE = "lcickhouse.sink.buffer.max.size"
}
