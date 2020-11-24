import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//定义输入输出样例类
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class AdClickCountByProvince(windowEnd: String, province: String, count: Long)


object AdClickAnalysis {
  def main(args: Array[String]): Unit = {
    //设置运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //方便做测试，全局并行度设置为1
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //从文件读取数据
    val resource = getClass.getResource("/AdClickLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

    //转换为样例类，并提取时间戳和watermark
    val adLogStream = inputStream
      .map(data => {
        val arr = data.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 开窗聚合统计
    val adCountResultStream = adLogStream
      .keyBy(_.province)
      .timeWindow(Time.days(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountWindowResult())


    adCountResultStream.print()

    env.execute("ad count statistics job")
  }
}


class AdCountAgg() extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickLog, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}


class AdCountWindowResult() extends WindowFunction[Long, AdClickCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {
    val end = new Timestamp(window.getEnd).toString
    out.collect(AdClickCountByProvince(end, key, input.head))
  }
}


