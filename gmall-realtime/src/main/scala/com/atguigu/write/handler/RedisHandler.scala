package com.atguigu.write.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.write.bean.StartUpLog
import com.atguigu.write.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @author chenhuiup
 * @create 2020-11-05 16:01
 */
object RedisHandler {
  /**
   * 在本批次内去重
   * @param filterMidDStream 本批次内根据mid号进行去重，取mid最早的一条
   */
  def filterByMid(filterMidDStream: DStream[StartUpLog]): DStream[StartUpLog] = {
    // DStream中没有groupBy，所以需要通过transform转换为rdd进行操作
    filterMidDStream.transform(
      rdd => {
        // 变换数据结构，加上mid
        val midLogRDD: RDD[(String, StartUpLog)] = rdd.map(
          log => (log.mid, log)
        )
        // 按照mid分组，排序，取最早的数据
        val midLogListRDD: RDD[(String, List[StartUpLog])] = midLogRDD.groupByKey().mapValues(
          iter => iter.toList.sortBy(_.ts).take(1)
        )
        // 扁平化处理，不要list
        val logRDD: RDD[StartUpLog] = midLogListRDD.flatMap {
          case (mid, list) => list
        }
        logRDD
      }
    )
  }

  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  /**
   * 通过redis实现跨批次去重
   * @param caseDStream
   */
  def fliterByRedis(caseDStream: DStream[StartUpLog],sparkContext:SparkContext): DStream[StartUpLog] = {
    // 方案一：每一条数据获取一次连接，判断是否在redis中存在
//    caseDStream.filter(
//      log => {
//        val client = RedisUtil.getJedisClient
//        val key = s"DAU:${log.logDate}"
//        val bool = client.sismember(key,log.mid)
//        client.close()
//        !bool
//      }
//    )

    // 方案二：以分区为单位获取连接，判断是否存在
    // 每个分区获取一次连接，每一条数据判断时与redis访问一次
//    caseDStream.mapPartitions(
//      iter => {
//        // 1.获取连接
//        val client = RedisUtil.getJedisClient
//        val logs: Iterator[StartUpLog] = iter.filter(
//          log => {
//            // 将业务名与日期作为key
//            val redisKey = s"DAU:${log.logDate}"
//            val bool = client.sismember(redisKey, log.mid)
//            !bool
//          }
//        )
//        // 2.归还连接
//        client.close()
//        logs
//      }
//    )

    // 方案三：在Driver端获取一次连接，查询redis中存储数据，以广播变量的形式分发到各个Executor，
    // 这样只获取一次连接，且每批次只与redis访问一次
    caseDStream.transform(
      rdd => {
        // 1.在Driver端获取连接，获取redis存储的所有数据，以广播变量的形式分发到Executor端
        val client = RedisUtil.getJedisClient
        // 1.2获取系统时间，通过simpleDateFormat类获取日期
        val redisKey = s"DAU:${sdf.format(new Date(System.currentTimeMillis()))}"
        val set: util.Set[String] = client.smembers(redisKey)
        // 1.3 查询到的结果以广播变量的形式分发出去,需要SparkContext对象
        val redisSet: Broadcast[util.Set[String]] = sparkContext.broadcast(set)

        // 2.Executor进行过滤操作
        val filterRDD: RDD[StartUpLog] = rdd.filter(
          log => {
            //判断log.mid在set集合中是否存在
            !redisSet.value.contains(log.mid)
          }
        )
        // 3.归还连接
        client.close()
        // 4.返回结果
        filterRDD
      }
    )
  }


  /**
   *  将去重后的流式数据中mid保存到redis中（为了后续批次去重）
   *
   * @param caseDStream 经过跨批次去重和同批次去重的数据
   */
  def saveAsResdis(caseDStream: DStream[StartUpLog]): Unit = {

    // 3.将数据保存到redis的set集合中，key为天，value为mid
    // 使用分区操作，减少连接的获取与释放
    caseDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            // 1.获取Jedis连接
            val client: Jedis = RedisUtil.getJedisClient
            // 3.1 遍历写库
            iter.foreach(
              log =>{
                // 将业务名与日期作为key
                val redisKey = s"DAU:${log.logDate}"
                client.sadd(redisKey,log.mid)
              }
            )
            // 2.归还连接
            client.close()
          }
        )
      }
    )
  }
}
