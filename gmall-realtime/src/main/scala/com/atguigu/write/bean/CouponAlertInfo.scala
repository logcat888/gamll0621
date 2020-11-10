package com.atguigu.write.bean

/**
 * @author chenhuiup
 * @create 2020-11-10 11:22
 */
case class CouponAlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)

