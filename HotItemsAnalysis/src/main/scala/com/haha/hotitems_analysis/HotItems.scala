package com.haha.hotitems_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


//定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从文件中读取文件，并转换为样例类,并提取时间戳生成watermark
    val inputStream: DataStream[String] = env.readTextFile("D:\\Flink\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L) //watermark用于提示在该时间后的窗口可以关闭了

    // 得到窗口聚合结果
    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv") //过滤pv行为，preview
      .keyBy("itemId") //根据商品ID分组
      .timeWindow(Time.hours(1), Time.minutes(5)) //设置滑动窗口进行统计
      .aggregate(new CountAgg(), new ItemViewWindowResult())

    //运行
    env.execute("hot items")
  }
}

//自定义预聚合函数AggreateFunction,
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  //需要聚合的状态，聚合的状态就是当前商品的count值
  override def createAccumulator(): Long = 0L

  //每来一条数据，调用一次add,count值加1
  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  //返回的数据
  override def getResult(acc: Long): Long = acc

  //聚合方式(没在用)
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//自定义窗口函数WindowFunction
class ItemViewWindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}