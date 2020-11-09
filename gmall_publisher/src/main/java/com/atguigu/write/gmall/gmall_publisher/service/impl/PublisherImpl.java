package com.atguigu.write.gmall.gmall_publisher.service.impl;

import com.atguigu.write.gmall.gmall_publisher.mapper.DauMapper;
import com.atguigu.write.gmall.gmall_publisher.mapper.GmvMapper;
import com.atguigu.write.gmall.gmall_publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
}
