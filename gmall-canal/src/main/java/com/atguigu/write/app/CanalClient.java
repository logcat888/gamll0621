package com.atguigu.write.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.write.constants.GmallConstants;
import com.atguigu.write.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author chenhuiup
 * @create 2020-11-06 23:00
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1.获取Canal连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                "");

        //2.抓取数据并解析
        while (true) {
            //1.连接
            canalConnector.connect();
            //2.指定消费的数据表,库下的表
            canalConnector.subscribe("gmall2020.*");
            //3.抓取数据，每次抓取100条消息
            Message message = canalConnector.get(100);
            // 4.判断是否为空
            if (message.getEntries().size() <= 0) {
                System.out.println("当前没有数据！休息一会！");
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                // 5.获取到Message中对应的多个Entry
                List<CanalEntry.Entry> entries = message.getEntries();
                // 6.遍历entries
                for (CanalEntry.Entry entry : entries) {
                    // 7.获取entry的类型,只要RowData类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        // 8.获取表名
                        String tableName = entry.getHeader().getTableName();
                        // 9.获取entry对应的序列化值
                        ByteString storeValue = entry.getStoreValue();
                        // 10.反序列化：使用RowChange解析序列化的值
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        // 11.获取EventType类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        // 12.获取数据集合
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        // 13.根据表名以及事件类型处理数据rowDatasList
                        handler(tableName, eventType, rowDatasList);
                    }
                }
            }

        }

    }

    /**
     * 根据表名以及事件类型处理数据rowDatasList
     *
     * @param tableName    表名
     * @param eventType    事件类型
     * @param rowDatasList 数据集合
     */
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //1.对于订单表而言,只需要新增数据
        if (CanalEntry.EventType.INSERT.equals(eventType) && "order_info".equals(tableName)){
            //2.将数据发送到kafka集群
            //3.遍历多行数据
            for (CanalEntry.RowData rowData : rowDatasList) {
                // 4.创建Json对象，用于存放多个列的数据
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    jsonObject.put(column.getName(),column.getValue());
                }
                //5.将数据转换为Json字符串
                String s = jsonObject.toString();
                System.out.println(s);
                //6.将数据发送到kafka
                MyKafkaSender.send(GmallConstants.GMALL_ORDER_INFO, s);
            }
        }
    }
}
