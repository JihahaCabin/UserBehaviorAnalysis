package com.haha.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerUtil {

  def main(args: Array[String]): Unit = {
    val topic = "hotitems"

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](properties)

    //从文件读取数据，逐行写入kafka
    val bufferedSource = io.Source.fromFile("D:\\Flink\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val strings: Iterator[String] = bufferedSource.getLines()
    for (item <- strings) {
      val record = new ProducerRecord[String, String](topic, item)
      producer.send(record)
    }

    //关闭
    producer.close()
  }

}
