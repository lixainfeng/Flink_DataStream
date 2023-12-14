package com.lxf.canal;

import com.alibaba.fastjson.JSON;
import com.alibaba.google.common.base.CaseFormat;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;

public class CanalClientApp {
    public static void main(String[] args) throws Exception{
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("localhost", 11111),
                "example", null, null);
        //设置死循环，一直访问connector中数据
        while (true){
            connector.connect(); //进行链接
            connector.subscribe("test.*");//设置需要监控的库表
            Message message = connector.get(100);//设置获取一批数据量大小

            List<CanalEntry.Entry> entries = message.getEntries(); //获取一批消息的list集合
            if(entries.size() > 0){ //如果list中有数据就遍历取出
                for (CanalEntry.Entry entry : entries) {
                    String tableName = entry.getHeader().getTableName(); //获取header中请求到的表名

                    CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());//value值转换成string类型
                    List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                    //insert update delete ...
                    CanalEntry.EventType eventType = rowChange.getEventType();

                    if(eventType == CanalEntry.EventType.INSERT){
                        for (CanalEntry.RowData rowData : rowDatasList) {
                            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

                            HashMap<String, String> map = new HashMap<>();
                            for (CanalEntry.Column column : afterColumnsList) {
                                String key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                                map.put(key, column.getValue());
                            }

                            /**
                             * TODO...
                             * 到这一步，下面要做的事情就是把map的数据发送到想要的地方去...
                             */
                            System.out.println("tableName:"+ tableName + " , " + JSON.toJSONString(map));
                        }
                    }
                }
            }

        }
    }
}
