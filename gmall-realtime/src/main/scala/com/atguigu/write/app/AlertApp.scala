package com.atguigu.write.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.write.bean.{CouponAlertInfo, EventLog}
import com.atguigu.write.constants.GmallConstants
import com.atguigu.write.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks

/**
 * @author chenhuiup
 * @create 2020-11-10 11:25
 */
/*
1. 预警需求：同一设备，5分钟内三次及以上用不同账号领取优惠劵，并且过程中没有浏览商品。达到以上要求则产生一条预警日志。
        并且同一设备，每分钟只记录一次预警。
2. 实现思路：
1）5分钟内:开窗 union
2）同一设备:设备ID,分组
3）三次及以上用不同账号:uid
4）没有浏览商品:反向考虑,一旦有浏览商品行为,就不产生预警日志
5）每分钟只记录一次预警:去重(ES)(根据ES的doc_id,可以令mid-分钟表示唯一性，
    分钟可以是距离1970的分钟数，也可以用日志的时间戳转为年月日时分表示唯一性)

3. 预警日志格式
1）mid	设备id
2）uids	领取优惠券登录过的uid
3）itemIds	优惠券涉及的商品id
4）events	发生过的行为
5）ts	发生预警的时间戳

4. es认java的集合，不认scala的集合。

 */
object AlertApp {
  def main(args: Array[String]): Unit = {
    // 1.创建配置文件对象，创建StreamingContext对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 2.从kafka读取事件日志
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    // 3.将事件日志转换为样例类对象，变换结构为（mid，log）
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.map(
      data => {
        val log = data.value()
        val eventLog: EventLog = JSON.parseObject(log, classOf[EventLog])
        // 补充两个时间字段
        val date = sdf.format(new Date(eventLog.ts))
        eventLog.logDate = date.split(" ")(0)
        eventLog.logHour = date.split(" ")(1)
        (eventLog.mid, eventLog)
      }
    )

    // 4.开5分钟窗口
    val midToLogByWindowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))

    // 5.按照mid分组（mid，iterator(log)）
    val groupBymidDStream: DStream[(String, Iterable[EventLog])] = midToLogByWindowDStream.groupByKey()

    // 6.对分组内的数据进行分析，
    //  1）如果5分钟内浏览数据，则不产生预警日志，循环中断不继续进行判断
    //  2）否则生成预警日志，根据日志格式，将uid添加到HashSet中，将itemID添加到List集合中，将enid(事件id)添加到list集合中
    //  3）设置标记位判断是否需要生成预警日志，根据是否有evid中是否有点击日志，以及5分钟登录3次以上
    val boolDStream: DStream[(Boolean, CouponAlertInfo)] = groupBymidDStream.map {
      case (mid, iter) => {
        // a.创建java的HashSet，存放uid
        val uids = new util.HashSet[String]()
        // b.创建Set用于存放优惠券涉及的商品ID
        val itemIds = new util.HashSet[String]()
        //c.创建List用于存放反生过的所有行为
        val events = new util.ArrayList[String]()
        //d.定义标志位,用于记录是否存在浏览商品行为
        var flag = true
        Breaks.breakable {
          for (elem <- iter) {
            val evid = elem.evid
            events.add(elem.evid)
            // 如果是浏览日志，则不产生预警日志，循环中断，并标志位设置为flase
            if ("clickItem".equals(evid)) {
              flag = false
              Breaks.break()
              // 判断是否为领券行为，将uid，item，evid，加入集合
            } else if ("coupon".equals(evid)) {
              uids.add(elem.uid)
              itemIds.add(elem.itemid)
            }
          }
        }

        //产生疑似预警日志
        (uids.size() >= 3 && flag, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
    }

    // 7. 过滤预警日志
    val logDStream: DStream[CouponAlertInfo] = boolDStream.filter(_._1).map(_._2)

    // 8.批量写入es中，一个mid每分钟只写入一条预警日志，doc_id可以用mid-分钟表示唯一性，分钟可以是距离1970的分钟数
    logDStream.foreachRDD(
      rdd => {
        // a.以分区为单位获取连接批量写入es
        rdd.foreachPartition(
          iter =>{
            // b.根据创建的模板，创建索引名，按天补充日期
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
            val todayTime = sdf.format(new Date(System.currentTimeMillis()))
            val indexname = s"${GmallConstants.ES_ALERT_INDEX_PRE}-${todayTime.split(" ")(0)}"

            // c.根据当前时间设置doc_id
            val docList: List[(String, CouponAlertInfo)] = iter.toList.map(log => {
              val doc_id = s"${log.mid}-${todayTime}"
              (doc_id, log)
            })

            // d.批量写入ES中
            MyEsUtil.insertBulk(indexname,docList)
          }
        )
      }
    )

    // 9.开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
