
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

//输入的登录事件样例类
case class LoginEventAdvance(userId: Long, ip: String, eventType: String, timestamp: Long)

// 输出的报警信息样例类
case class LoginFailWarningAdvance(userId: Long, firstFailTime: Long, lastFailTime: Long, waringMsg: String)


object LoginFailAdvance {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

    //转换为样例类，并提取时间戳和watermark
    val loginEventStream = inputStream
      .map(data => {
        val arr = data.split(",")
        LoginEventAdvance(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEventAdvance](Time.seconds(3)) {
        override def extractTimestamp(t: LoginEventAdvance): Long = t.timestamp * 1000L
      })

    // 进行判断和检测，如果2秒内连续登录失败，输出报警信息
    val loginFailWarningStream = loginEventStream
      .keyBy(_.userId)
      .process(new LoginFailWarningAdvanceResult())

    loginFailWarningStream.print()

    env.execute("login fail job")
  }
}


class LoginFailWarningAdvanceResult() extends KeyedProcessFunction[Long, LoginEventAdvance, LoginFailWarningAdvance] {

  lazy val loginFailListState: ListState[LoginEventAdvance] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEventAdvance]("login-fail", classOf[LoginEventAdvance]))

  override def processElement(i: LoginEventAdvance, context: KeyedProcessFunction[Long, LoginEventAdvance, LoginFailWarningAdvance]#Context, collector: Collector[LoginFailWarningAdvance]): Unit = {
    // 首先判断事件类型
    if (i.eventType == "fail") {
      //如果是失败，则进一步判断
      val iter = loginFailListState.get().iterator()
      //判断之前是否有登录失败的事件
      if (iter.hasNext) {
        // 1.1 如果有，那么判断两次失败的时间差
        val firstFailEvent = iter.next()
        if (i.timestamp < firstFailEvent.timestamp + 2) {
          //如果在2s之内，输出报警
          collector.collect(LoginFailWarningAdvance(i.userId, firstFailEvent.timestamp, i.timestamp, "login fail in 2s times."))
        }

        //不管报不报警，当前都以处理完毕，将状态更新为最近一次登录失败的事件
        loginFailListState.clear()
        loginFailListState.add(i)
      } else {
        // 1.2 如果没有，直接将当前事件添加到ListState中
        loginFailListState.add(i)
      }
    } else {
      // 如果是成功，直接清空状态
      loginFailListState.clear()
    }
  }
}