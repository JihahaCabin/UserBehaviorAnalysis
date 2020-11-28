import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


//定义输入输出样例类类型
case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeoutWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val arr = data.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.orderId)

    // 定义pattern
    val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))


    // 将pattern应用到数据流上，进行模式匹配
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    // 定义侧输出流标签，用于处理超时事件
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

    // 调用select方法，提取并处理匹配的成功支付的事件以及超时事件
    val resultStream = patternStream.select(orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect()
    )

    resultStream.print()
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout job")
  }
}


class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
    val timeoutOrderId = map.get("create").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout :" + timeoutTimestamp)
  }
}


class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = map.get("pay").iterator().next().orderId
    OrderResult(payedOrderId, "already payed")
  }
}