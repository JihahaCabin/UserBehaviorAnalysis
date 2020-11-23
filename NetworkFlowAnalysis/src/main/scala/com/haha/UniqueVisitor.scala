package com.haha

import java.lang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//输出统计样例类
case class UvCount(windowEnd: Long, count: Long)


object UniqueVisitor {
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


    val uvStream = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1)) // 直接不分组，基于dataStream开1小时滚动窗口
      .apply(new UvCountResult())


    uvStream.print()

    env.execute("Uv test")
  }
}

//自定义实现全窗口函数,用一个Set结构保存所有的UserId，进行自动去重
class UvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(w: TimeWindow, iterable: Iterable[UserBehavior], collector: Collector[UvCount]): Unit = {
    // 定义一个Set
    var userIdSet = Set[Long]()

    //遍历窗口中的所有数据，吧userId添加到set中去重
    for (userBehavior <- iterable) {
      userIdSet += userBehavior.userId
    }

    //将set的size作为去重后的uv值输出
    collector.collect(UvCount(w.getEnd, userIdSet.size))

  }
}