package com.atguigu.write;

import com.atgugui.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * @author chenhuiup
 * @create 2020-11-09 17:46
 */
public class Write {
    public static void main(String[] args) throws IOException {
        //1.创建ES客户端构建器
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.创建ES客户端连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        //3.设置ES连接地址
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //4.获取ES客户端连接
        JestClient jestClient = jestClientFactory.getObject();

        //5.构建ES插入数据对象

        Movie movie = new Movie("1004", "王义大战金刚");

        Index index = new Index.Builder(movie)
                .index("movie_test2")
                .type("_doc")
                .id("1004")
                .build();

        //6.执行插入数据操作
        jestClient.execute(index);
        //7.关闭连接
        jestClient.shutdownClient();

    }

}
