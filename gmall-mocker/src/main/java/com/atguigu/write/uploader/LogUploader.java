package com.atguigu.write.uploader;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author chenhuiup
 * @create 2020-11-03 19:06
 */
public class LogUploader {

    public static void sendLogStream(String log){
        try{
            //不同的日志类型对应不同的URL，将生成的日志发送到Nginx服务器做负载均衡
            // 1. 如果再idea中启动SpringBoot程序，mocker生成数据，就需要设置为localhost:8080
            // 2. 如果在服务器上，仅在hadoop102启动SpringBoot程序，mocker生成数据，就需要设置为hadoop102:Tomcat端口号
            // 3. 如果在服务器上，在所有服务器都开启日志服务器，且在hadoop102配置Nginx，
            //    就需要设置为hadoop102:Nginx端口号，如果是80端口号可以省略
            URL url  =new URL("http://hadoop102:7777/log");

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            //设置请求方式为post
            conn.setRequestMethod("POST");

            //时间头用来供server进行时钟校对的
            conn.setRequestProperty("clientTime",System.currentTimeMillis() + "");

            //允许上传数据
            conn.setDoOutput(true);

            //设置请求的头信息,设置内容类型为JSON
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            System.out.println("upload" + log);

            //输出流
            OutputStream out = conn.getOutputStream();
            out.write(("logString="+log).getBytes());
            out.flush();
            out.close();
            int code = conn.getResponseCode();
            System.out.println(code);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}

