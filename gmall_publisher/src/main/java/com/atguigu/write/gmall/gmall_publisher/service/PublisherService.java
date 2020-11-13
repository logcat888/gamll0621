package com.atguigu.write.gmall.gmall_publisher.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author chenhuiup
 * @create 2020-11-06 21:12
 */
public interface PublisherService {
    public Integer getDauTotal(String date);
    public Map getDauTotalHourMap(String date);

    public Double getOrderAmountTotal(String date);

    public Map getOrderAmountHourMap(String date);

    public String getSaleDetail(String date, int startpage, int size, String keyword) throws IOException;
}
