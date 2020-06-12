package com.atgui.kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 读取Kafka数据源创建DsStream 之0_10 版本
object Kafka_0_10Direct {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("kafkaDirect0_10").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    val kafkaParams = Map[String, Object](
     ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->"hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG->"bigdata",

      //key 序列化
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",

      //v 序列化
      //ClassOf[类]  返回的是全类名  和上述的情况一样
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val topics = Array("mybak")
    val dsStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      //位置策略
      LocationStrategies.PreferConsistent,
      //消费策略
      ConsumerStrategies.Subscribe(topics, kafkaParams)
    )

    dsStream.map(record => (record.key, record.value))

    ssc.start()
    ssc.awaitTermination()
  }
}
