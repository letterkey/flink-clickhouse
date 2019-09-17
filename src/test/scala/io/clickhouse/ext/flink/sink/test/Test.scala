package io.clickhouse.ext.flink.sink.test

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.{SerializeConfig, SerializerFeature}
import io.clickhouse.ext.flink.sink.ClickHouseSink
import io.clickhouse.ext.flink.sink.utils.ClickHouseConfig
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.collection.mutable.Map
import scala.collection.JavaConverters._
import org.apache.flink.streaming.api.scala._

/**
  * Created by letterkey on 19-9-17 15:13.
  */
case class Item(
               name:String,
               age:Int,
               sex: Int
               )
class Test extends FunSuite with Matchers with BeforeAndAfter{

  var env :StreamExecutionEnvironment = _
  before{
    env = StreamExecutionEnvironment.getExecutionEnvironment
    var params: Map[String,String] = Map()
    params += (ClickHouseConfig.CLICKHOUSE_HOST -> "127.0.0.1")
    params += (ClickHouseConfig.CLICKHOUSE_PORT -> "8123")
    params += (ClickHouseConfig.DATABASE -> "default")
    params += (ClickHouseConfig.USERNAME -> "default")
    params += (ClickHouseConfig.PASSWORD -> "")
    params += (ClickHouseConfig.TABLENAME -> "flink")

    env.getConfig.setGlobalJobParameters(ParameterTool.fromMap(params.asJava))

  }

  test("test flink clickhouse sink"){
    val data = Array[String]("lilei,10,1","jim,11,1","lucy,10,2","lily,11,2","hanmeimei,9,2")

    val ds = env.fromCollection(data)
      .map(_.split(","))
      .map(a => Item(a(0).toString,a(1).toInt,a(2).toInt))
      .map(item => {
//        SerializerFeature.QuoteFieldNames
        JSON.toJSONString(item,new SerializeConfig(true))
      })
      .setParallelism(1)
    ds.print()
    ds
      .name("test-sink")
      .addSink(new ClickHouseSink())

    env.execute("test")
  }
}
