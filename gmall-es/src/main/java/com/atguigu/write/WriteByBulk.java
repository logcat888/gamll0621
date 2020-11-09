package com.atguigu.write;

import com.atgugui.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * @author chenhuiup
 * @create 2020-11-09 18:10
 */
public class WriteByBulk {
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

        Movie movie = new Movie("1005", "王义大战金刚");

        Index index = new Index.Builder(movie)
                .index("movie_test2")
                .type("_doc")
                .id("1005")
                .build();

        Movie movie1 = new Movie("1006", "王义大战白骨精");

        Index index1 = new Index.Builder(movie1)
                .id("1006")
                .build();

        Bulk bulk = new Bulk.Builder()
                .addAction(index)
                .addAction(index1)
                .defaultIndex("movie_test2")
                .defaultType("_doc")
                .build();

        //6.执行插入数据操作
        jestClient.execute(bulk);
        //7.关闭连接
        jestClient.shutdownClient();

    }
}
