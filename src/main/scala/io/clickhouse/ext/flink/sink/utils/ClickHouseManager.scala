package io.clickhouse.ext.flink.sink.utils

import org.slf4j.LoggerFactory

import scala.collection.mutable._

/**
  * Created by letterkey on 19-9-11 14:54.
  */
class ClickHouseManager(conf:Map[String,String]) {
  val log = LoggerFactory.getLogger("ClickHouseManager")

  val host = conf.getOrElse(ClickHouseConfig.CLICKHOUSE_HOST,"127.0.0.1")
  val port = conf.getOrElse(ClickHouseConfig.CLICKHOUSE_PORT,"8123").toInt
  val username = conf.getOrElse(ClickHouseConfig.USERNAME,"default")
  val password = conf.getOrElse(ClickHouseConfig.PASSWORD,"")
  val dbname = conf.getOrElse(ClickHouseConfig.DATABASE,"default")
  val tablename = conf.getOrElse(ClickHouseConfig.TABLENAME,"test")
  val buffer_size = conf.getOrElse(ClickHouseConfig.MAX_BUFFER_SIZE,"100").toInt

  val ds = ClickhouseConnectionFactory.get(host,port,username,password)

  val datas = new ArrayBuffer[String]


  def put(item:String): Unit ={
    datas += item
    // condition size and time
    if(checkCondition())
      flush()
  }

  def clearBuffer(): Unit ={
    log.info(s"job end clear data size[${datas.size}]")
    flush()
  }

  def flush(): Unit ={
    log.info(s"writer data size[${datas.size}]")
    writer()
    datas.clear()
  }
  def checkCondition(): Boolean ={
    datas.size >= buffer_size
  }

  def writer(): Unit ={
    val copy_data = datas.clone()
    // get schema with last json data
    val writer = new ClickHouseWriter(ds,dbname,tablename,copy_data)
    writer.setName("Thread writer data to clickhouse"+System.nanoTime())

    writer.start()
  }

}
