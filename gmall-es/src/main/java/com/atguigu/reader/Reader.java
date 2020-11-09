package com.atguigu.reader;

import com.atgugui.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author chenhuiup
 * @create 2020-11-09 18:17
 */
public class Reader {
    public static void main(String[] args) throws IOException {
        //1.创建ES客户端构建器
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.创建ES客户端连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        //3.设置ES连接地址
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //4.获取ES客户端连接
        JestClient jestClient = jestClientFactory.getObject();

        //9.查询数据
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // 9.1添加查询条件

        // b.创建bool对象
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        // d.创建term对象
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("class_id", "0621");
        // c.调用filter方法，添加term对象
        boolQueryBuilder.filter(termQueryBuilder);

        // d.创建match对象
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo2", "球");

        // c.调用must方法，关联match对象
        boolQueryBuilder.must(matchQueryBuilder);

        // a.添加查询条件
        sourceBuilder.query(boolQueryBuilder);

        // 9.2添加聚合组条件

        // a.添加最大值聚合组
        MaxAggregationBuilder maxAge1 = AggregationBuilders.max("maxAge").field("age");
        sourceBuilder.aggregation(maxAge1);

        // b.添加年龄分组
        TermsAggregationBuilder genderCount1 = AggregationBuilders.terms("genderCount").field("gender").size(10);
        sourceBuilder.aggregation(genderCount1);

        // 9.3设置分页
        sourceBuilder.from(0);
        sourceBuilder.size(2);

        // 9.4设置排序
        sourceBuilder.sort("stu_id", SortOrder.ASC);

        //5.构建ES插入数据对象

        Search search = new Search.Builder(sourceBuilder.toString())
                .addIndex("student2")
                .addType("_doc")
                .build();

//        Search search = new Search.Builder("{\n" +
//                "  \"query\": {\n" +
//                "    \"bool\": {\n" +
//                "      \"filter\": {\n" +
//                "        \"term\": {\n" +
//                "          \"class_id\": \"0621\"\n" +
//                "        }\n" +
//                "      },\n" +
//                "      \"must\": [\n" +
//                "        {\n" +
//                "          \"match\": {\n" +
//                "            \"favo2\": \"球\"\n" +
//                "          }\n" +
//                "        }\n" +
//                "      ]\n" +
//                "    }\n" +
//                "  },\n" +
//                "  \"aggs\": {\n" +
//                "    \"genderCount\": {\n" +
//                "      \"terms\": {\n" +
//                "        \"field\": \"gender\",\n" +
//                "        \"size\": 10\n" +
//                "      }\n" +
//                "    },\n" +
//                "    \"maxAge\":{\n" +
//                "      \"max\": {\n" +
//                "        \"field\": \"age\"\n" +
//                "      }\n" +
//                "    }\n" +
//                "  },\n" +
//                "  \"from\": 0,\n" +
//                "  \"size\": 2,\n" +
//                "  \"sort\": [\n" +
//                "    {\n" +
//                "      \"stu_id\": {\n" +
//                "        \"order\": \"asc\"\n" +
//                "      }\n" +
//                "    }\n" +
//                "  ]\n" +
//                "}")
//                .addIndex("student2")
//                .addType("_doc")
//                .build();

        //6.执行插入数据操作
        SearchResult result = jestClient.execute(search);

        //7.解析结果（先在web页面查询结果，再根据结果调用各个方法）
        // 7.1 解析hits中的source和sort
        Long total = result.getTotal();
        System.out.println("总共命中：" + total + "条数据");
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println("************************");
            // 获取source
            Map source = hit.source;
            for (Object o : source.keySet()) {
                System.out.println("key: " + o + ",value: " +source.get(o));
            }
            System.out.println("排序：" + hit.sort);
        }
        // 7.2解析聚合组
        MetricAggregation aggregations = result.getAggregations();
        // a.获取分组Term的聚合组
        TermsAggregation genderCount = aggregations.getTermsAggregation("genderCount");
        // b.获取term中的bucket
        for (TermsAggregation.Entry bucket : genderCount.getBuckets()) {
            System.out.println("--------------------------");
            System.out.println("getKeyAsString: "+bucket.getKeyAsString());
            System.out.println("doc_count: " + bucket.getCount());
        }
        // c.获取最大值的聚合组
        MaxAggregation maxAge = aggregations.getMaxAggregation("maxAge");
        System.out.println("最大年龄为：" + maxAge.getMax());

        //8.关闭连接
        jestClient.shutdownClient();

    }
}
/*
查询条件：
GET student2/_search
{
  "query": {
    "bool": {
      "filter": {
        "term": {
          "class_id": "0621"
        }
      },
      "must": [
        {
          "match": {
            "favo2": "球"
          }
        }
      ]
    }
  },
  "aggs": {
    "genderCount": {
      "terms": {
        "field": "gender",
        "size": 10
      }
    },
    "maxAge":{
      "max": {
        "field": "age"
      }
    }
  },
  "from": 0,
  "size": 2,
  "sort": [
    {
      "stu_id": {
        "order": "asc"
      }
    }
  ]
}
 */
/*
查询结果：
{
  "took" : 3,
  "timed_out" : false,
  "_shards" : {
    "total" : 5,
    "successful" : 5,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : 2,
    "max_score" : null,
    "hits" : [
      {
        "_index" : "student2",
        "_type" : "_doc",
        "_id" : "1003",
        "_score" : null,
        "_source" : {
          "stu_id" : 1003,
          "name" : "石磊",
          "class_id" : "0621",
          "gender" : "false",
          "age" : 10000,
          "favo1" : "乒乓球篮球击剑",
          "favo2" : "乒乓球篮球击剑"
        },
        "sort" : [
          1003
        ]
      },
      {
        "_index" : "student2",
        "_type" : "_doc",
        "_id" : "1006",
        "_score" : null,
        "_source" : {
          "stu_id" : 1006,
          "name" : "晓磊",
          "class_id" : "0621",
          "gender" : "true",
          "age" : 99999,
          "favo1" : "橄榄球降维打击",
          "favo2" : "橄榄球降维打击"
        },
        "sort" : [
          1006
        ]
      }
    ]
  },
  "aggregations" : {
    "genderCount" : {
      "doc_count_error_upper_bound" : 0,
      "sum_other_doc_count" : 0,
      "buckets" : [
        {
          "key" : 0,
          "key_as_string" : "false",
          "doc_count" : 1
        },
        {
          "key" : 1,
          "key_as_string" : "true",
          "doc_count" : 1
        }
      ]
    },
    "maxAge" : {
      "value" : 99999.0
    }
  }
}
 */