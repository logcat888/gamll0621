package com.atguigu.write.app

import java.sql.Date
import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.write.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.write.constants.GmallConstants
import com.atguigu.write.utils.{JdbcUtil, MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @author chenhuiup
 * @create 2020-11-10 20:33
 */

/*
1. 功能：将OrderInfo与OrderDetail数据进行双流JOIN,并根据user_id查询Redis,补全用户信息
2. 样例类转换为Json字符串

 */
object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    // 1.创建配置文件对象和StreamingContext
    val sparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    // 2.获取kafka数据源
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_ORDER_INFO,ssc)
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_ORDER_DETAIL,ssc)

    // 3.将每行数据转换为样例类对象,补充时间,数据脱敏(手机号),变换结构（id，caseClass）
    val orderInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.map(event => {
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
      (orderInfo.id, orderInfo)
    })

    val orderDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.map(
      event => {
        // a.获取event的值
        val value = event.value()
        // b.将value转化为样例类
        val orderDetail = JSON.parseObject(value, classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      }
    )


    // 4.双流join
    val orderInfoFullJoinDetailDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)

    // a.以分区为单位处理
    val saleDetailDStream: DStream[SaleDetail] = orderInfoFullJoinDetailDStream.mapPartitions(
      iter => {
        // d.定义List装能够join上的orderInfo和orderDetail
        var list = ListBuffer[SaleDetail]()
        // f.获取redis的连接
        val client: Jedis = RedisUtil.getJedisClient
        // b.遍历元素
        iter.foreach {
          case (orderId, (orderInfoOpt, orderDetailOpt)) => {
            // c.如果orderInfoOpt不为空,包含join上
            if (orderInfoOpt.isDefined) {
              // c.0 获取orderInfoOpt的值,转化为json字符串
              val orderInfo: OrderInfo = orderInfoOpt.get

              import org.json4s.native.Serialization
              implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
              val orderInfoJson: String = Serialization.write(orderInfo)

              // c.1 将orderId写入redis，String类型，redis-key为orderInfo-orderId，value为Json字符串
              client.set(s"orderInfo:${orderId}", orderInfoJson)
              // c.2 根据网络最大延迟设置过期时间
              client.expire(s"orderInfo:${orderId}",100)

              // c.2 若orderDetailOpt不为空，则join上，将结果封装成为SaleDetail,添加到List集合中
              if (orderDetailOpt.isDefined) {
                val saleDetail = new SaleDetail(orderInfo, orderDetailOpt.get)
                list.append(saleDetail)
              }

              // c.3 访问redis查看有没有对应的orderDetailOpt，有就封装成为SaleDetail,添加到List集合中
              // c.3.1 orderDetail在redis中是以set形式存的
              val res: util.Set[String] = client.smembers(s"orderDetail-${orderId}")
              // c.3.2 解析res，如果res有数据，则说明join上了
              if (res.size() > 0) {
                val iter: util.Iterator[String] = res.iterator()
                while (iter.hasNext) {
                  val orderDatailJson = iter.next()
                  val orderDetail = JSON.parseObject(orderDatailJson, classOf[OrderDetail])
                  val saleDetail = new SaleDetail(orderInfo, orderDetail)
                  list.append(saleDetail)
                }
              }
            } else {
              // orderDetail没有join上
              // 获取orderDetailOpt的值
              val orderDetail = orderDetailOpt.get
              // g.访问redis查看能否join上，如果join上将结果封装成为SaleDetail,添加到List集合中
              val res: String = client.get(s"orderInfo:$orderId")
              if (res != null) {
                val orderInfo = JSON.parseObject(res, classOf[OrderInfo])
                val saleDetail = new SaleDetail(orderInfo, orderDetail)
                list.append(saleDetail)
              } else {
                // f.没有join上将orderDetail写入到redis中，使用Set集合，redis-key为orderDetail-orderId，value为Json字符串
                // f.1 获取orderInfoOpt的值,转化为json字符串
                import org.json4s.native.Serialization
                implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
                val orderDetailJson: String = Serialization.write(orderDetail)
                client.set(s"orderDetail:$orderId", orderDetailJson)
                // f.2 根据网络最大延迟设置过期时间
                client.expire(s"orderDetail:$orderId",100)
              }
            }
          }
        }
        // 归还连接
        client.close()
        list.iterator
      }
    )
    // 5.查看redis中的用户信息，将用户数据添加到SaleDetail中
    val saleDetailWithUserInfoDStream: DStream[SaleDetail] = saleDetailDStream.mapPartitions(
      iter => {
        // a.获取redis客户端
        val client = RedisUtil.getJedisClient

        val addUserInfo: Iterator[SaleDetail] = iter.map(
          saleDetail => {
            // b.从redis中获取用户信息
            val redisKey = s"UserInfo:${saleDetail.user_id}"
            val userInfoJson: String = client.get(redisKey)
            if (userInfoJson != null) {
              // c.redis中查询到用户信息
              val userInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
              saleDetail.mergeUserInfo(userInfo)
            }else{
              // d.redis中查询不到用户信息，在mysql中查询用户信息
              // a.1 获取mysql连接，因为用户不在redis中的情况非常少，不能放在以分区为单位获取连接，而是需要用到的时候才获取连接
              val connection = JdbcUtil.getConnection
              val userInfoStrJson: String = JdbcUtil.getUserInfoFromMysql(
                connection,
                "select * from gmall2020.user_info where id=?",
                Array(saleDetail.user_id)
              )
              val userInfo = JSON.parseObject(userInfoStrJson, classOf[UserInfo])
              saleDetail.mergeUserInfo(userInfo)

              // e.将查询到的用户信息写入到redis中,设置失效时间
              client.set(redisKey,userInfoStrJson)
              // 归还连接
              connection.close()
            }

            saleDetail
          }
        )
        // c.归还连接
        client.close()
        addUserInfo
      }
    )
    // 6. 将关联的结果写入ES中
    saleDetailWithUserInfoDStream.foreachRDD(
      rdd =>{
        rdd.foreachPartition(
          iter =>{
            // a.设置索引,gmall2020_sale_detail-日期,Date为SQL的Date类
            val date: Date = new Date(System.currentTimeMillis())
            val indexName = s"${GmallConstants.ES_SALE_DETAIL_INDEX_PRE}-${date.toString}"
            // b.变换结构添加doc_id,doc_id为订单详情id
            val source: List[(String, SaleDetail)] = iter.toList.map(saleDetail => {
              (saleDetail.order_detail_id, saleDetail)
            })
            // c.批量插入ES中
            MyEsUtil.insertBulk(indexName,source)
          }
        )
      }
    )

//    orderInfoDStream.print()
//    orderDetailDStream.print()

    // 4.开启任务
    ssc.start()
    ssc.awaitTermination()


  }
}
