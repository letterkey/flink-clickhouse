package io.clickhouse.ext.flink.sink.utils

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.parser.Feature
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ArrayBuffer

/**
  * Created by yinmuyang on 19-9-11 14:54.
  */
class ClickHouseManager(conf:Configuration) {


  val host = conf.getString(ClickHouseConfig.CLICKHOUSE_HOST,"127.0.0.1")
  val port = conf.getInteger(ClickHouseConfig.CLICKHOUSE_PORT,8123)
  val username = conf.getString(ClickHouseConfig.USERNAME,"browser-clickhouse")
  val password = conf.getString(ClickHouseConfig.PASSWORD,"eR961I+4")
  val dbname = conf.getString(ClickHouseConfig.DATABASE,"default")
  val tablename = conf.getString(ClickHouseConfig.TABLENAME,"default")

  val ds = ClickhouseConnectionFactory.get(host=host,port=port,user=username,password=password)


  var maxBufferSize:Int=1

  var datas = new ArrayBuffer[String]

  val sinkBuffer = new  SinkBuffer

  def put(data:String): Unit ={
    // condition size and time
    writer(data)
//    flush()

    datas += data
  }

  def flush(): Unit ={
    if(checkCondition()){
      // flush data to clickhouse
      writer(datas.clone())

      // clean buffer
//      datas.clear()
    }
  }


  def checkCondition(): Boolean ={
    datas.size >= maxBufferSize
  }
  def withMaxBufferSize(_maxBufferSize:Int): Unit ={
    maxBufferSize = _maxBufferSize
  }
//
//  def writer(): Unit ={
//    val copy_data = datas.clone()
//    // get schema with last json data
//    val writer = new ClickHouseWriter(ds,dbname,tablename,copy_data)
////    writer.setName("writer data to clickhouse")
//
////    writer.start()
//    writer.writer()
//  }


  def writer(item:String): Unit ={
    using(ds.getConnection){conn =>
      val columns = JSON.parseObject(item,Feature.OrderedField).keySet().toArray().map(_.toString)
      val insertStatementSql = generateInsertStatment(dbName = dbname,tableName=tablename,columns)
      val statement = conn.prepareStatement(insertStatementSql)

        val vals = JSON.parseObject(item,Feature.OrderedField).values().toArray()
        for(i <- 0 until columns.size){
          statement.setObject(i+1,vals(i))
        }
        statement.addBatch()
      statement.executeBatch()
    }
  }

  def writer(copy_data:ArrayBuffer[String]): Unit ={
    using(ds.getConnection){conn =>
      val columns = JSON.parseObject(copy_data.last,Feature.OrderedField).keySet().toArray().map(_.toString)
      val insertStatementSql = generateInsertStatment(dbName = dbname,tableName=tablename,columns)
      val statement = conn.prepareStatement(insertStatementSql)

      copy_data.foreach(item =>{
        val vals = JSON.parseObject(item,Feature.OrderedField).values().toArray()
        for(i <- 0 until columns.size){
          statement.setObject(i+1,vals(i))
        }
        statement.addBatch()
      })
      statement.executeBatch()
    }
  }
  private[utils] def using[A, B <: {def close(): Unit}] (closeable: B) (f: B => A): A =
    try {
      f(closeable)
    }
    finally {
      closeable.close()
    }

  private[utils] def generateInsertStatment( dbName: String, tableName: String,columns :Array[String]) = {
    val vals = 1 to (columns.length) map (i => "?")
    s"INSERT INTO $dbName.$tableName (${columns.mkString(",")}) VALUES (${vals.mkString(",")})"
  }

}
