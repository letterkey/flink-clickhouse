package io.clickhouse.ext.flink.sink.utils

import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties

/**
  * Created by yinmuyang on 19-9-11 10:01.
  */
object ClickhouseConnectionFactory extends Serializable {
  private val dataSources = scala.collection.mutable.Map[(String, Int), ClickHouseDataSource]()

  def get(host: String, port: Int = 8123,user:String="",password:String="",dbname:String="default"): ClickHouseDataSource ={
    dataSources.get((host, port)) match {
      case Some(ds) =>
        ds
      case None =>
        val ds = createDatasource(host, port=port,user =user,password = password,dbname)
        dataSources += ((host, port) -> ds)
        ds
    }
  }

  private def createDatasource(host: String, port: Int = 8123,user:String,password:String,dbname:String) = {

    val properties = new ClickHouseProperties()
    properties.setUser(user)
    properties.setPassword(password)
    properties.setDatabase(dbname)
    val url = s"jdbc:clickhouse://$host:$port"
    new ClickHouseDataSource(url,properties)
  }
}
