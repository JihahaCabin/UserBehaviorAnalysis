package com.haha.hotitems_analysis


import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


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

    val resultStream: DataStream[String] = aggStream
      .keyBy("windowEnd") //按照窗口分组，收集当前窗口内的count数据
      .process(new TopNHotItems(5)) // 自定义处理流程

    resultStream.print()

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

// 自定义KeyedProcessFunction
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
  // 先定义状态：ListState
  private var itemViewCountListState: ListState[ItemViewCount] = _

  // 在open生命周期初始化
  override def open(parameters: Configuration): Unit = {
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-list", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //每来一条数据，直接接入ListState
    itemViewCountListState.add(value)
    // 注册一个windowEnd +1 之后触发的定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 当定时器触发，可以认为窗口统计结果已经到齐，可以排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //为了方便排序，另外定义一个listBuffer，保存ListState里面的所有数据
    val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
    val iter = itemViewCountListState.get().iterator()
    while (iter.hasNext) {
      allItemViewCounts += iter.next()
    }

    //清空状态
    itemViewCountListState.clear()

    // 按照count从大到小进行排序,取前N个
    val sortedItemViewCounts = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 将排名信息格式化为String,便于显示
    val result: StringBuilder = new StringBuilder()

    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")


    //遍历结果列表的每个ItemViewCount,输出到一行
    for (i <- sortedItemViewCounts.indices) {
      val currentItemViewCount = sortedItemViewCounts(i)
      result.append("NO").append(i + 1).append(": ")
        .append("商品ID=").append(currentItemViewCount.itemId).append("\t")
        .append("热门度=").append(currentItemViewCount.count).append("\n")
    }

    result.append("============================================\n\n")

    Thread.sleep(1000)

    //输出结果
    out.collect(result.toString())
  }

}