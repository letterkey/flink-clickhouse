package io.clickhouse.ext.flink.sink

import io.clickhouse.ext.flink.sink.utils.{ClickHouseConfig, ClickHouseManager}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Created by letterkey on 19-9-11 09:30.
  */
class  ClickHouseSink extends RichSinkFunction[String]{
  val log = LoggerFactory.getLogger("ClickHouseSink")
  var clickHouseManager :ClickHouseManager = null
  override def open(conf: Configuration): Unit = {
    log.info("init clickhouse sink ...")
    super.open(conf)
    // get clickhouse params
    val params = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.toMap.asScala
    clickHouseManager = new ClickHouseManager(params)
  }

  override def invoke(value: String): Unit = {
    clickHouseManager.put(value)
  }

  override def close(): Unit = {
    log.info("clear buffer data to clickhouse the end of sink end")
    clickHouseManager.clearBuffer()
  }
}
