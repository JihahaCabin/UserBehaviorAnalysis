package com.haha

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random


//定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//定义输出pv统计的样例类
case class PvCount(windowEnd: Long, count: Long)


object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    env.setParallelism(1)

    // 从文件中读取数据
    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream = env.readTextFile(resource.getPath)

    //转换成样例类型，提取时间戳，作为watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L) //watermark用于提示在该时间后的窗口可以关闭了

    val pvStream = dataStream
      .filter(_.behavior == "pv")
      //      .map(data => ("pv", 1L)) // 定义一个pv字符串作为分组的dummy key
      .map(new MyMapper())
      .keyBy(_._1) // 当前所有数据会被分到同一个组
      .timeWindow(Time.hours(1)) //一小时的滚动窗口
      .aggregate(new PvCountAgg(), new PvCountWindowResult())

    val totalPvStream = pvStream
      .keyBy(_.windowEnd)
      .process(new TotalPvCountResult())

    totalPvStream.print()

    env.execute("pv job")

  }
}

// 自定义预聚合函数
class PvCountAgg() extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: (String, Long), acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//自定义窗口函数
class PvCountWindowResult() extends WindowFunction[Long, PvCount, String, TimeWindow] {


  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect(PvCount(window.getEnd, input.iterator.next())) //或者写成 input.head
  }
}

//自定义mapper,随机生成分组的key
class MyMapper() extends MapFunction[UserBehavior, (String, Long)] {
  override def map(t: UserBehavior): (String, Long) = {
    (Random.nextString(10), 1)
  }
}


class TotalPvCountResult extends KeyedProcessFunction[Long, PvCount, PvCount] {

  //定义一个状态，保存当前所有count总和
  lazy val totalPvCountResultState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("totlPvCountResult-state", classOf[Long]))

  override def processElement(i: PvCount, context: KeyedProcessFunction[Long, PvCount, PvCount]#Context, collector: Collector[PvCount]): Unit = {
    //每来一个数据，将count值叠加到当前的状态上
    val currentTotalCount = totalPvCountResultState.value()
    totalPvCountResultState.update(currentTotalCount + i.count)

    //注册一个windowEnd +1ms后触发的定时器
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
    val totalPvCount = totalPvCountResultState.value()

    out.collect(PvCount(ctx.getCurrentKey, totalPvCount))
    //清空状态
    totalPvCountResultState.clear()
  }
}