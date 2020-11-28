
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

// 到账事件样例类
case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)

object TxMatch {


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
      .filter(_.eventType == "pay")
      .keyBy(_.txId)


    val receipt = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(receipt.getPath)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.txId)

    // 合并两条流，进行处理
    val resultSteam = orderEventStream.connect(receiptEventStream)
      .process(new TxPayMatchResult())

    resultSteam.print()
    resultSteam.getSideOutput(new OutputTag[OrderEvent]("unmatchedPays")).print("unmatched pays")
    resultSteam.getSideOutput(new OutputTag[ReceiptEvent]("unmatchedReceipts")).print("unmatched receipts")

    env.execute("Tx match job")

  }
}

class TxPayMatchResult() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  //保存当前交易对应的订单支付事件和到账事件
  lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent]))
  lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))

  override def processElement1(pay: OrderEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val receipt = receiptState.value()
    if (receipt != null) {
      // 如果已经有receipt，正常输出匹配，清空状态
      receiptState.clear()
      collector.collect((pay, receipt))
    } else {
      // 如果没来，注册定时器等待5s
      payState.update(pay)
      context.timerService().registerEventTimeTimer(pay.timestamp * 1000L + 5000L)
    }
  }

  override def processElement2(receipt: ReceiptEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val payment = payState.value()
    if (payment != null) {
      // 如果已经有pay，正常输出匹配，清空状态
      payState.clear()
      collector.collect((payment, receipt))
    } else {
      // 如果没来，注册定时器等待5s
      receiptState.update(receipt)
      context.timerService().registerEventTimeTimer(receipt.timestamp * 1000L + 5000L)
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    if (payState.value() != null) {
      ctx.output(new OutputTag[OrderEvent]("unmatchedPays"), payState.value())
    }
    if (receiptState.value() != null) {
      ctx.output(new OutputTag[ReceiptEvent]("unmatchedReceipts"), receiptState.value())
    }
    payState.clear()
    receiptState.clear()
  }
}
