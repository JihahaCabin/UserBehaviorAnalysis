
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object OrderTimeoutWithProccessFunction {
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


    // 自定义proccessFunction 进行复杂时间的监测
    val orderResultStream = orderEventStream
      .keyBy(_.orderId)
      .process(new OrderPayMatchResult())

    orderResultStream.print()
    orderResultStream.getSideOutput(new OutputTag[OrderResult]("timeout")).print("tiemout")


    env.execute("OrderTimeoutWithProccessFunction")
  }
}

//自定义实现KeyedProcessFunction
class OrderPayMatchResult() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

  // 定义状态，标志位，标志create、pay是否已经来过，定时器时间戳
  lazy val isCreateState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-create", classOf[Boolean]))
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-pay", classOf[Boolean]))
  lazy val timeTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  //定义侧输出流标签
  val orderTimeoutOutputTag = new OutputTag[OrderResult]("timeout")

  override def processElement(i: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {
    //先取出当前状态
    val isPayed = isPayedState.value()
    val isCreate = isCreateState.value()
    val timerTs = timeTsState.value()

    //判断当前时间类型，看是create还是pay
    // 1、来的是create,判断是否pay过
    if (i.eventType == "create") {
      //1.1 如果已经支付过，正常支付，输出匹配成功的结果
      if (isPayed) {
        collector.collect(OrderResult(i.orderId, "pay successfully"))
        //已经处理完毕，清空状态和定时器
        isCreateState.clear()
        isPayedState.clear()
        timeTsState.clear()
        context.timerService().deleteEventTimeTimer(timerTs)
      } else {
        // 1.2 如果还没pay过，注册定时器，等待15分钟
        val ts = i.timestamp * 1000L + 900 * 1000L
        context.timerService().registerEventTimeTimer(ts)

        //更新状态
        timeTsState.update(ts)
        isCreateState.update(true)
      }
      //2如果当前来的是pay,要判断是否create过
    } else if (i.eventType == "pay") {
      if (isCreate) {
        // 2.1 如果已经create过，匹配成功，还得判断一下pay的时间，是否超过定时器时间
        if (i.timestamp * 1000L < timerTs) {
          // 2.1.1 没有超时，正常输出
          collector.collect(OrderResult(i.orderId, "pay successfully"))
        } else {
          // 2.1.2 已经超时，输出超时
          context.output(orderTimeoutOutputTag, OrderResult(i.orderId, "payed but already timeout"))
        }
        // 只要输出完，当前Order已经处理完，清除状态和定时器
        isCreateState.clear()
        isPayedState.clear()
        timeTsState.clear()
        context.timerService().deleteEventTimeTimer(timerTs)
      }
    } else {
      // 2.2 如果create没来，注册一个定时器，等到pay的时间即可
      context.timerService().registerEventTimeTimer(i.timestamp * 1000L)

      //更新状态
      timeTsState.update(i.timestamp * 1000L)
      isPayedState.update(true)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    // 定时器触发
    // 1 pay来了，create没到
    if (isPayedState.value() == true) {
      ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "payed but not found create log"))
    } else {
      // 1 create来了 pay没到
      ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
    }
    //清空所有状态
    isCreateState.clear()
    isPayedState.clear()
    timeTsState.clear()
  }
}