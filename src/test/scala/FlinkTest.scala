import io.clickhouse.ext.flink.sink.ClickHouseSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Created by yinmuyang on 19-9-11 11:35.
  */
object FlinkTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.readTextFile("file:///home/yinmuyang/work/git/xiaomi/flink-clickhouse/data/data.json")


    data.setParallelism(2).print()

    data.name("test-sink")
      .addSink(new ClickHouseSink())

    env.execute("test")


  }


}
