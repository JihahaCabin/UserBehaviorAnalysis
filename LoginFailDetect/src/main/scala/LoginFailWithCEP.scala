import java.util

import LoginFailAdvance.getClass
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object LoginFailWithCEP {
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

    // 1. 先定义匹配的模式，要求是一个登录失败事件后，紧跟另一个登录失败事件
    val loginFailPattern = Pattern
      .begin[LoginEventAdvance]("firstFail")
      .where(_.eventType == "fail")
      .next("secondFail")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))

    // 2 将模式应用到数据流上，得到一个PatternStream
    val patternStream = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

    // 3 检出符合模式的数据流，调用select方法
    val loginFailWarningStream = patternStream.select(new LoginFailEventMatch())

    loginFailWarningStream.print()

    env.execute("login fail job")
  }
}

// 实现自定义的patternSelectFunction
class LoginFailEventMatch() extends PatternSelectFunction[LoginEventAdvance, LoginFailWarningAdvance] {
  override def select(map: util.Map[String, util.List[LoginEventAdvance]]): LoginFailWarningAdvance = {
    // 当前匹配到的时间序列，从map中提取出来
    val firstFailEvent = map.get("firstFail").get(0)
    val secondFailEvent = map.get("secondFail").get(0)
    LoginFailWarningAdvance(secondFailEvent.userId, firstFailEvent.timestamp, secondFailEvent.timestamp, "loginFail")
  }
}