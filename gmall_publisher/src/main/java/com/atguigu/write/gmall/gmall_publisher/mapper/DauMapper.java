package com.atguigu.write.gmall.gmall_publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author chenhuiup
 * @create 2020-11-06 21:07
 */
public interface DauMapper {
    public Integer selectDauTotal(String date);

    public List<Map> selectDauTotalHourMap(String date);
}
