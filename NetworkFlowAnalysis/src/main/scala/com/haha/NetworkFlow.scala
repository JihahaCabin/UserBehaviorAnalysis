package com.haha

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
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

    val inputStream = env.readTextFile("D:\\Flink\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")

    //    val inputStream = env.socketTextStream("localhost", 7777)

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

  //  lazy val pageViewCountListState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageViewCount-list", classOf[PageViewCount]))
  lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long]))


  override def processElement(i: PageViewCount, context: KeyedProcessFunction[Long, PageViewCount, String]#Context, collector: Collector[String]): Unit = {
    //    pageViewCountListState.add(i)
    pageViewCountMapState.put(i.url, i.count)
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
    //另外追一个定时器，1分钟后出发，这是窗口已经关闭，不在有聚合结果输出，可以清空结果
    context.timerService().registerEventTimeTimer(i.windowEnd + 60000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //    val allPageViewCounts: ListBuffer[PageViewCount] = ListBuffer()
    //    val iter = pageViewCountListState.get().iterator()
    //
    //    while (iter.hasNext) {
    //      allPageViewCounts += iter.next()
    //    }
    //
    //    //清空状态,会有问题，会导致迟到数据来时，原来的排序数据已不见了
    //    pageViewCountListState.clear()

    //判断定时器出发时间，如果已经是窗口结束时间1分钟之后，那么直接清空状态
    if (timestamp == ctx.getCurrentKey + 60000L) {
      pageViewCountMapState.clear()
      return
    }

    val allPageViewCounts: ListBuffer[(String, Long)] = ListBuffer()

    val iter = pageViewCountMapState.entries().iterator()

    while (iter.hasNext) {
      val entry = iter.next()
      allPageViewCounts += ((entry.getKey, entry.getValue))
    }

    // 按照访问量排序并输出top n
    val sortedPageViewCounts = allPageViewCounts.sortBy(_._2)(Ordering.Long.reverse).take(n)


    // 将排名信息格式化为String,便于显示
    val result: StringBuilder = new StringBuilder()

    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")


    //遍历结果列表的每个ViewCount,输出到一行
    for (i <- sortedPageViewCounts.indices) {
      val currentItemViewCount = sortedPageViewCounts(i)
      result.append("NO").append(i + 1).append(": ")
        .append("URL=").append(currentItemViewCount._1).append("\t")
        .append("热门度=").append(currentItemViewCount._2).append("\n")
    }

    result.append("============================================\n\n")

    Thread.sleep(1000)

    //输出结果
    out.collect(result.toString())
  }
}