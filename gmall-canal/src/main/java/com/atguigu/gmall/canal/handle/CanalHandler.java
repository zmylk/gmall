package com.atguigu.gmall.canal.handle;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall.canal.utill.MyKafkaSender;
import com.atguigu.gmall.constant.GmallConstants;
import com.google.common.base.CaseFormat;

import java.util.List;

public class CanalHandler {

    public static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList)
    {
        if ("order_info".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType))
        {
            System.out.println("============================一行数据==============================");
            for (CanalEntry.RowData rowData : rowDataList) {
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {
                    System.out.println(column.getName()+":::"+column.getValue());
                    String propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                    jsonObject.put(propertyName,column.getValue());
                }
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER,jsonObject.toJSONString());
            }
        }
    }
}
