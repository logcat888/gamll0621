package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderInfo, StartUpLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
 * @author chenhuiup
 * @create 2020-11-07 9:48
 */
/*
需求：通过canal实时监控订单表，将数据发送kafka的订单主题，使用sparkStreaming消费kafka中的数据，包装成样例类，将数据存入Phoenix中
 */
object GmvApp {
  def main(args: Array[String]): Unit = {
    // 1.创建配置文件对象和StreamingContext
    val sparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    // 2.获取kafka数据源
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_ORDER_INFO,ssc)

    // 3.将每行数据转换为样例类对象,补充时间,数据脱敏(手机号)
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(event => {
      // a.获取event的值
      val value = event.value()
      // b.将value转化为样例类
      val orderInfo = JSON.parseObject(value, classOf[OrderInfo])
      // c.添加日期和小时（以便分时统计）
      val time = orderInfo.create_time.split(" ")
      orderInfo.create_date = time(0)
      orderInfo.create_hour = time(1).split(":")(1)
      // d.数据脱敏
      orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 4) + "*******"
      // e.返回结果
      orderInfo
    })


    // 4.将明细数据写入Phoenix
    orderInfoDStream.foreachRDD(
      rdd => {
        println("aaaa")
        // a.导入org.apache.phoenix.spark._
        rdd.saveToPhoenix(
          "GMALL2020_ORDER_INFO",
          // 通过反射写出表名，且大写
          classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase()),
          HBaseConfiguration.create(),
          Some("hadoop102,hadoop103,hadoop104:2181")
        )
      }
    )
    // 5.开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
