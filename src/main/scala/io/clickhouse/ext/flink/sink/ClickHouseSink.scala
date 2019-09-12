package io.clickhouse.ext.flink.sink

import io.clickhouse.ext.flink.sink.utils.ClickHouseManager
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction}

/**
  * Created by yinmuyang on 19-9-11 09:30.
  */
class  ClickHouseSink extends RichSinkFunction[String]{

  var clickHouseManager :ClickHouseManager = _
  override def open(conf: Configuration): Unit = {
    if(clickHouseManager == null){
      this.synchronized{
          clickHouseManager = new ClickHouseManager(conf)
      }
    }
  }

  override def invoke(value: String): Unit = {
    clickHouseManager.put(value)
  }

  override def close(): Unit = {

  }

}
