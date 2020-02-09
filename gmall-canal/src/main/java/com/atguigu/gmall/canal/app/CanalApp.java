package com.atguigu.gmall.canal.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.gmall.canal.handle.CanalHandler;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalApp {
    public static void main(String[] args) {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop104", 11111), "example", "", "");
        while (true)
        {
            //连接、订阅、抓取数据
            canalConnector.connect();
            canalConnector.subscribe("gmalldb.order_info");
            //获得message
            Message message = canalConnector.get(100);
            int size = message.getEntries().size();
            if (size==0)
            {

                try {
                    System.out.println("没有数据，休息一会");
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                //获得Entries
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //判断事件类型 只处理 行变化业务
                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA))
                    {
                        //序列化
                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange = null;

                        try {
                            //得到反序列化rowChange
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }

                        //获得行集rowData
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        CanalEntry.EventType eventType = rowChange.getEventType(); //操作类型
                        String tableName = entry.getHeader().getTableName();//表明
                        CanalHandler.handle(tableName,eventType,rowDatasList);

                    }
                }
            }
        }


    }


}
