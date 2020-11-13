package com.atguigu.write.app

import com.alibaba.fastjson.JSON
import com.atguigu.write.bean.UserInfo
import com.atguigu.write.constants.GmallConstants
import com.atguigu.write.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.json4s.DefaultFormats

/**
 * @author chenhuiup
 * @create 2020-11-10 20:33
 */
//将用户表新增及变化数据缓存至Redis
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    // 1.创建配置文件对象和StreamingContext
    val sparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    // 2.获取kafka数据源
    val userInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_USER_INFO,ssc)

    // 3.取出value
    val userInfoDataStream: DStream[String] = userInfoKafkaDStream.map(_.value())

    // 4.用户信息写入到redis中
    userInfoDataStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter =>{
            // a.获取redis连接
            val client = RedisUtil.getJedisClient
            iter.foreach(
              userInfoJson =>{
                // b.将Json字符串转换为样例类转换
                val userInfo = JSON.parseObject(userInfoJson,classOf[UserInfo])
                val redisKey = s"${UserInfo}:${userInfo.id}"
                client.set(redisKey,userInfoJson)
              }
            )
            // c.归还连接
            client.close()
          }
        )
      }
    )

    // 4.开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
