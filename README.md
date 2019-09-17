# flink-clickhouse
flink write/read data to clickhouse 

1. flink clickhouse sink
> support json str spec:java class or scala case class transform json str date
```scala
    env = StreamExecutionEnvironment.getExecutionEnvironment
    var params: Map[String,String] = Map()
    params += (ClickHouseConfig.CLICKHOUSE_HOST -> "127.0.0.1")
    params += (ClickHouseConfig.CLICKHOUSE_PORT -> "8123")
    params += (ClickHouseConfig.DATABASE -> "default")
    params += (ClickHouseConfig.USERNAME -> "default")
    params += (ClickHouseConfig.PASSWORD -> "")
    params += (ClickHouseConfig.TABLENAME -> "flink")
    ...
    val data = Array[String]("lilei,10,1","jim,11,1","lucy,10,2","lily,11,2","hanmeimei,9,2")
    val ds = env.fromCollection(data)
      .map(_.split(","))
      .map(a => Item(a(0).toString,a(1).toInt,a(2).toInt))
      .map(item => {
        JSON.toJSONString(item,new SerializeConfig(true))
      })
      .setParallelism(1)
    ds.print()
    ds
      .name("test-sink")
      .addSink(new ClickHouseSink())
    env.execute("test")
```
> See details: test case

2. flink clickhouse source
> Pending