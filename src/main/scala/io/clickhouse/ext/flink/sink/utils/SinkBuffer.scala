package io.clickhouse.ext.flink.sink.utils

import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ArrayBuffer

/**
  * Created by yinmuyang on 19-9-11 10:49.
  */
class SinkBuffer {

  var maxBufferSize:Int=0

  var datas = new ArrayBuffer[String]

  def put(data:String): Unit ={
    // condition size and time
    checkCondition()

    datas += data
  }

  def flush(): Unit ={
    if(checkCondition()){
      // flush data to clickhouse

      // clean buffer
      datas.clear()
    }
  }

  def checkCondition(): Boolean ={
    datas.size >= maxBufferSize
  }
  def withMaxBufferSize(_maxBufferSize:Int): Unit ={
    maxBufferSize = _maxBufferSize
  }

  def build(): Unit ={

  }
}
