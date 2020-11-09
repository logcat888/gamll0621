package com.atguigu.write.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.RedisHandler
import com.atguigu.utils.MyKafkaUtil
import com.atguigu.write.bean.StartUpLog
import com.atguigu.write.constants.GmallConstants
import com.atguigu.write.handler.RedisHandler
import com.atguigu.write.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
 * @author chenhuiup
 * @create 2020-11-05 15:27
 */
object DauApp {

  def main(args: Array[String]): Unit = {
    // 1. 创建配置文件对象 和 StreamingContext对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 3.获取kafka数据源
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    // 4.将Json转换成样例类对象
    val caseDStream: DStream[StartUpLog] = kafkaDStream.map(log => {
      val value = log.value()
      // 通过反射获取StartUpLog对象
      val startUpLog = JSON.parseObject(value, classOf[StartUpLog])
      // 添加时间字段
      val dtH = sdf.format(new Date(startUpLog.ts))
      val split = dtH.split(" ")
      startUpLog.logDate = split(0)
      startUpLog.logHour = split(1)
      //返回数据
      startUpLog
    })

    // 5.利用redis进行跨批次去重
    val filterMidDStream = RedisHandler.fliterByRedis(caseDStream,ssc.sparkContext)
    //设置缓存的目的：1.优化，避免任务重复执行；2.避免消费数据后，触发行动算子后，无法从kafka拉取数据。
    caseDStream.cache()
    caseDStream.count().print()

    filterMidDStream.cache()
    filterMidDStream.count().print()

    // 6.在本批次内去重
    val filterByMidDStream = RedisHandler.filterByMid(filterMidDStream)
    filterByMidDStream.cache()
    filterByMidDStream.count().print()

    // 7.将数据存入redis中
    RedisHandler.saveAsResdis(filterByMidDStream)

    // 8.将数据存入到Phoenix中
    filterByMidDStream.foreachRDD(
      rdd => {
       // 1.使用spark与Phoenix联合开发的保存到Phoenix的API,必须先导入import org.apache.phoenix.spark._
        // 2.使用saveToPhoenix算子保存到Phoenix
        // 3.Phoenix建表时如果没有把表名用双引号，则自动变成大写，因此插入时表名要大写
        // 4. 需要导入HBase的包：import org.apache.hadoop.hbase.HBaseConfiguration
        rdd.saveToPhoenix("GMALL2020_DAU",
          Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,
          //加载HBase的配置信息
          HBaseConfiguration.create(),
          Some("hadoop102,hadoop103,hadoop104:2181"))
      }
    )

    // 2.开启任务和阻塞线程
    ssc.start()
    ssc.awaitTermination()
  }
}
