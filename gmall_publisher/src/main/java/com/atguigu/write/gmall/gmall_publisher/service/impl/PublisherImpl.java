package com.atguigu.write.gmall.gmall_publisher.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.Options;
import com.atguigu.bean.Stat;
import com.atguigu.write.gmall.gmall_publisher.mapper.DauMapper;
import com.atguigu.write.gmall.gmall_publisher.mapper.GmvMapper;
import com.atguigu.write.gmall.gmall_publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenhuiup
 * @create 2020-11-06 21:12
 */
@Service
public class PublisherImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private GmvMapper gmvMapper;

    @Autowired
    private JestClient jestClient;

    public Integer getDauTotal(String date){
        return dauMapper.selectDauTotal(date);
    }

    public Map getDauTotalHourMap(String date){
        //1. 查询Phoenix，获取数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2. 创建HashMap保存数据，将多行数据保存在一个Map中
        HashMap<String, Long> result = new HashMap<>();

        //3. 遍历list集合,取出小时LH，新增日活CT,在Phoenix中默认转换为大写
        for (Map map : list) {
            result.put((String)map.get("LH"),(Long)map.get("CT"));
        }

        //4.返回结果
        return result;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return gmvMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHourMap(String date) {
        //1. 查询Phoenix，获取数据
        List<Map> list = gmvMapper.selectOrderAmountHourMap(date);

        //2. 创建HashMap保存数据，将多行数据保存在一个Map中
        HashMap<String, Double> result = new HashMap<>();

        //3. 遍历list集合,取出小时create_hour，小时的GMV sum_amount,在Phoenix中默认转换为大写
        for (Map map : list) {
            result.put((String)map.get("CREATE_HOUR"),(Double)map.get("SUM_AMOUNT"));
        }
        //4.返回结果
        return result;
    }

    // 从ES中获取数据
    @Override
    public String getSaleDetail(String date, int startpage, int size, String keyword) throws IOException {

        //1.查询数据--过滤匹配
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // b.设置bool对象
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        // e.创建term对象，设置查询条件
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("dt", date);
        // g.创建match对象，设置查询条件
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("sku_name", keyword);
        matchQueryBuilder.operator(MatchQueryBuilder.Operator.AND);
        // c.调用filter方法，全值匹配
        boolQueryBuilder.filter(termQueryBuilder);
        // f.调用must方法，进行分词匹配match
        boolQueryBuilder.must(matchQueryBuilder);
        // a.查询数据
        sourceBuilder.query(boolQueryBuilder);

        // 2.创建聚合组
        // 2.1按照年龄聚合
        TermsBuilder groupby_age = AggregationBuilders.terms("groupby_age").field("user_age");
        sourceBuilder.aggregation(groupby_age);
        // 2.2按照性别聚合
        TermsBuilder groupby_gender = AggregationBuilders.terms("groupby_gender").field("user_gender");
        sourceBuilder.aggregation(groupby_gender);

        // 3.设置起始位置和每页条数
        sourceBuilder.from((startpage -1)*size);
        sourceBuilder.size(size);

        //5.构建ES插入数据对象
        Search search = new Search.Builder(sourceBuilder.toString())
                .addIndex("gmall2020_sale_detail-query")
                .addType("_doc")
                .build();


        //6.执行查询
        SearchResult searchResult = jestClient.execute(search);


        //7.解析查询结果封装为Json对象
        JSONObject jsonObject = new JSONObject();
        //7.1获取总查询数
        Long total = searchResult.getTotal();

        //7.2获取聚合组
        MetricAggregation aggregations = searchResult.getAggregations();
        //7.2.3创建ArrayList<Stat>
        ArrayList<Stat> statList = new ArrayList<>();

        //7.2.1获取性别聚合组
        TermsAggregation groupbyGenderResult = aggregations.getTermsAggregation("groupby_gender");
        // 定义一个变量接收男性的人数
        Long maleCount = 0L;
        for (TermsAggregation.Entry bucket : groupbyGenderResult.getBuckets()) {
            if ("M".equals(bucket.getKey())){
                //获取男性人数
                maleCount=  bucket.getCount();
            }
        }
        // 计算男性的比例，保留1位小数
        String maleRatio = String.format("%.1f", maleCount * 100D / total);
        String femaleRatio = String.format("%.1f", 100-maleCount * 100D / total);
        ArrayList<Options> genderList = new ArrayList<>();
        genderList.add(new Options("男",maleRatio));
        genderList.add(new Options("女",femaleRatio));

        statList.add(new Stat(genderList,"用户性别占比"));

        //7.2.2获取年龄聚合组
        TermsAggregation groupbyAgeResult = aggregations.getTermsAggregation("groupby_age");
        //年龄小于20岁的人数
        Long lower20 = 0L;
        //年龄大于30岁的人数
        Long upper30 = 0L;

        for (TermsAggregation.Entry bucket : groupbyAgeResult.getBuckets()) {
            if (Integer.parseInt(bucket.getKey()) < 20){
                lower20 += bucket.getCount();
            }else if (Integer.parseInt(bucket.getKey()) >= 30){
                upper30 += bucket.getCount();
            }
        }
        //计算各年龄段的比例，保留1个小数
        String lower20Ratio = String.format("%.1f", lower20 * 100D / total);
        String upper3020Ratio = String.format("%.1f", upper30 * 100D / total);
        String range20To30Ratio = String.format("%.1f", 100-(lower20+upper30) * 100D / total);
        ArrayList<Options> ageList = new ArrayList<>();
        ageList.add(new Options("20岁以下",lower20Ratio));
        ageList.add(new Options("20岁到30岁",range20To30Ratio));
        ageList.add(new Options("30岁及30岁以上",upper3020Ratio));

        //添加到数组中
        statList.add(new Stat(ageList,"用户年龄占比"));

        //7.3获取Hits
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        ArrayList<Map> sourceList = new ArrayList<>();
        for (SearchResult.Hit<Map, Void> hit : hits) {
            //获取source
            Map source = hit.source;
            //添加到集合中
            sourceList.add(source);
        }

        //为JsonObject添加元素
        jsonObject.put("total",total);
        jsonObject.put("stat",statList);
        jsonObject.put("detail",sourceList);

        //返回Json字符串
        return jsonObject.toJSONString();
    }
}
