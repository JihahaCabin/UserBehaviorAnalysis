package com.haha

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class ApacheLogEvent(ip: String, userId: String, timestamp: Long, method: String, url: String)

case class PageViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {
    //定义运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //    val inputStream = env.readTextFile("D:\\Flink\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")

    val inputStream = env.socketTextStream("localhost", 7777)

    val dataStream = inputStream
      .map(line => {
        val linearray = line.split(" ")
        //转换时间
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(linearray(3)).getTime
        //返回格式化好的数据
        ApacheLogEvent(linearray(0), linearray(2), timestamp, linearray(5), linearray(6))

      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        //因为数据是乱序的，因此使用该方法，设置waterMark
        override def extractTimestamp(t: ApacheLogEvent): Long = t.timestamp
      })

    //进行开窗聚合，以及排序输出
    val aggStream = dataStream.filter(data => {
      data.method == "GET"
    })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
      .aggregate(new PageCountAgg(), new PageViewCountWindowResult)


    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotPages(3))

    dataStream.print("data")
    aggStream.print("agg")
    aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late"))
    resultStream.print()

    env.execute("hot Page Network flow")

  }
}

class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class PageViewCountWindowResult extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
  }
}


class TopNHotPages(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {

  lazy val pageViewCountListState: ListState[PageViewCount] =
    getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageViewCount-list", classOf[PageViewCount]))

  override def processElement(i: PageViewCount, context: KeyedProcessFunction[Long, PageViewCount, String]#Context, collector: Collector[String]): Unit = {
    pageViewCountListState.add(i)

    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allPageViewCounts: ListBuffer[PageViewCount] = ListBuffer()
    val iter = pageViewCountListState.get().iterator()

    while (iter.hasNext) {
      allPageViewCounts += iter.next()
    }

    //清空状态
    pageViewCountListState.clear()

    // 按照访问量排序并输出top n
    val sortedPageViewCounts = allPageViewCounts.sortWith(_.count > _.count).take(n)


    // 将排名信息格式化为String,便于显示
    val result: StringBuilder = new StringBuilder()

    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")


    //遍历结果列表的每个ViewCount,输出到一行
    for (i <- sortedPageViewCounts.indices) {
      val currentItemViewCount = sortedPageViewCounts(i)
      result.append("NO").append(i + 1).append(": ")
        .append("URL=").append(currentItemViewCount.url).append("\t")
        .append("热门度=").append(currentItemViewCount.count).append("\n")
    }

    result.append("============================================\n\n")

    Thread.sleep(1000)

    //输出结果
    out.collect(result.toString())
  }
}