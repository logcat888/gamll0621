package com.atguigu.write.gmall.gmall_publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author chenhuiup
 * @create 2020-11-07 11:40
 */
public interface GmvMapper {

    public Double selectOrderAmountTotal(String date);

    public List<Map> selectOrderAmountHourMap(String date);
}
