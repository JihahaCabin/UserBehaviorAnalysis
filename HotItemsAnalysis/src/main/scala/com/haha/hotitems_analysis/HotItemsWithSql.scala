package com.haha.hotitems_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object HotItemsWithSql {
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


    // 定义表执行环境
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 基于dataStream创建table
    val dataTable = tableEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    // 1.table api进行开窗聚合统计
    val aggTable = dataTable
      .filter('behavior === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
      .groupBy('itemId, 'sw)
      .select('itemId, 'sw.end as 'windowEnd, 'itemId.count as 'cnt)

    // 用sql实现topN的选取
    tableEnv.createTemporaryView("aggTable", aggTable, 'itemId, 'windowEnd, 'cnt)
    val resultTable = tableEnv.sqlQuery(
      """
        |select *
        |from (
        |  select
        |   *,
        |   row_number()
        |    over (partition by windowEnd order by cnt desc)
        |    as row_num
        |  from aggTable )
        |where row_num <=5
      """.stripMargin
    )

    //
    //    //纯SQL实现
    //    tableEnv.createTemporaryView("datatable",dataStream,'itemId,'behavior,'timestamp as 'ts)
    //    val resultSqlTable = tableEnv.sqlQuery(
    //      """
    //        |select *
    //        |from (
    //        |  select
    //        |   *,
    //        |   row_number()
    //        |    over (partition by windowEnd order by cnt desc)
    //        |    as row_num
    //        |  from (
    //        |     select
    //        |       itemId,
    //        |       hop_end(ts,interval '5' minute, interval '1' hour) as windowEnd,
    //        |       count(itemId) as cnt
    //        |     from datatable
    //        |     where behavior = 'pv'
    //        |     group by
    //        |       itemId,
    //        |       hop(ts,interval '5' minute,interval '1' hour)
    //        |  )
    //        |)
    //        |where row_num <=5
    //      """.stripMargin
    //    )


    resultTable.toRetractStream[Row].print()
    //运行
    env.execute("hot items sql ")

  }
}
