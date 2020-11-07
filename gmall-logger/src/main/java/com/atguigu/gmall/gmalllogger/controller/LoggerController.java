package com.atguigu.gmall.gmalllogger.controller;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.constants.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author chenhuiup
 * @create 2020-11-03 19:56
 */

//@RestController = @Controller+@ResponseBody
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;
    // 1.创建kafka生产者对象

    @RequestMapping("test1")
    //    @ResponseBody
    public String test01() {
        System.out.println("11111");
        return "success";
    }

    @RequestMapping("test2")
    public String test02(@RequestParam("name") String nn,
                         @RequestParam("age") int age) {
        System.out.println(nn + ":" + age);
        return "success";
    }

    @RequestMapping("log")
    public String getLogger(@RequestParam("logString") String logString) {
//        System.out.println(logString);
        // 0.为日志添加时间戳
        // 0.1解析为json对象
        JSONObject jsonObject = JSONObject.parseObject(logString);
        // 0.2为json对象添加时间戳
        jsonObject.put("ts",System.currentTimeMillis());

        // 1.落盘 file
        // 将添加时间戳的日志文件落盘，通过log4j框架落盘，将json对象转换为字符串
        String logsAndTime = jsonObject.toString();
        log.info(logsAndTime);

        // 2 推送到kafka
        // 将启动日志写入到kafka启动日志主题
        if ("startup".equals(jsonObject.get("type"))){
            // 使用common模块定义的常量（需要引入common模块的依赖），指定kafka生产者发送的主题，
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,logsAndTime);
        }else {
            // 将事件日志写入到kafka事件日志主题
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,logsAndTime);
        }

        return "success";
    }

}
