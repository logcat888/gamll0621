package com.atguigu.write.bean

/**
 * @author chenhuiup
 * @create 2020-11-10 11:21
 */
/*
时间日志：补充logDate，logHour两个字段以便如果有做分时统计的需求，可以将数据存入HBase中。
 */
case class EventLog(mid:String,
                    uid:String,
                    appid:String,
                    area:String,
                    os:String,
                    `type`:String,
                    evid:String,
                    pgid:String,
                    npgid:String,
                    itemid:String,
                    var logDate:String,
                    var logHour:String,
                    var ts:Long)

