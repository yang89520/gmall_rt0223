package com.yiran.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.yiran.gmall.Constansts;
import com.yiran.gmall.realtime.util.MyKafkaSink;
import com.yiran.gmall.realtime.util.MyKafkaUtil;
import org.apache.spark.sql.catalog.Column;

import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.util.List;

/**
 * @program: gmall_rt0223
 * @description: Canal客户端
 * @author: Mr.Yang
 * @create: 2021-07-08 18:13
 **/

public class CanalClient {
    /**
     *  *      步骤：
     *  *              ①创建一个客户端
     *  *                      CanalConnector
     *  *                          SimpleCanalConnector:  单节点canal集群
     *  *                          ClusterCanalConnector： HA canal集群
     *  *
     *  *
     *  *              ②使用客户端连接 canal server
     *  *              ③指定客户端订阅 canal server中的binlog信息
     *  *              ④解析binlog信息
     *  *              ⑤写入kafka
     *  *
     * @param args
     */

    public static void main(String[] args) {
        //创建canal客户端
        /*单机模式*/
//        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop103", 11111),
//                "mirror1", "root", "Yang89520..");
        /*集群模式*/
        CanalConnector canalConnector = CanalConnectors.newClusterConnector("hadoop102:2181,hadoop103:2181,hadoop104:2181","mirror1", "canal", "canal");
        canalConnector.connect();
        int batchSize = 1000;
        int emptyCount = 0;
        try {
            //创建连接对象
            canalConnector.connect();
            //指定客户端订阅 canal server中的binlog信息，也可只统计特定的表
            canalConnector.subscribe(Constansts.GMALL_DB_CANAL);
            //canalConnector.subscribe("gmall0223_db_c.order_info");

            //回滚到未进行 ack 的地方，下次fetch的时候，可以从最后一个没有 ack 的地方开始拿
            canalConnector.rollback();
            //不停地拉取数据
            int totalEmptyCount = 120;
            //Message[id=-1,entries=[],raw=false,rawEntries=[]] 代表当前这批没有拉取到数据
            while (emptyCount< totalEmptyCount) {

                Message msg = canalConnector.getWithoutAck(batchSize); //获取指定数量的数据

                long batchId = msg.getId();
                int size = msg.getEntries().size();
                //id=-1批次数量=0  表示本次没有拉取到数据
                if (batchId == -1 || size==0) {
                    emptyCount++;
                    String datetime = LocalDateTime.now().toString();
                    System.out.println("拉取失败:::" + datetime);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }else{
                    emptyCount=0;
                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);

                    //对数据进行处理
                    //获取msgs的entry对象，用于遍历拿到msg处理
                    List<CanalEntry.Entry> msgEntries = msg.getEntries();
                    for (CanalEntry.Entry msgEntry : msgEntries) {
                        //事件开始或结束跳过本次
                        if (msgEntry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || msgEntry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                            continue;
                        }
                        //传输的数据 格式为:二进制数组
                        ByteString storeValue = msgEntry.getStoreValue();
                        //变更数据的tablename
                        String tableName = msgEntry.getHeader().getTableName();

                        handleStoreValue(storeValue, tableName);
                    }
                }
                canalConnector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }
            System.out.println("empty too many times, exit");
        }finally {
            //释放链接
            canalConnector.disconnect();
        }
    }

    /**
     * 处理msg的方法
     *     入参：
     *     storeValue传输的数据 格式为:二进制数组
     *     tableName变更数据的tablename
     * @param storeValue
     * @param tableName
     */
    private static void handleStoreValue(ByteString storeValue, String tableName) {
        CanalEntry.RowChange rowChange = null;
        try {
            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
        } catch (Exception e) {
            throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + storeValue.toStringUtf8(), e);
        }

        /*
                一个RowChange代表多行数据

                order_info :  可能会执行的写操作类型. 统计GMV    total_amount
                                    insert :  会
                                                更新后的值
                                    update ：  不会。 只允许修改 order_status

                                            更新前 a列是 jack ,更新后变成tom
                                    delete :  不会。数据是不允许删除

                  判断当前这批写操作产生的数据是不是insert语句产生的
         */
        //binlog状态是Insert则运行以下逻辑
        //DELETE & INSERT & UPDATE...需要捕捉哪种状态都可以在这里处理
        if (rowChange.getEventType().equals(CanalEntry.EventType.INSERT)){
            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : rowDatasList) {
                JSONObject jsonObject = new JSONObject();
                //获取insert操作后每一行的每个字段  该对象可以拿到数据的字段和字段值
                List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();
                for (CanalEntry.Column column : columnsList) {
                    jsonObject.put(column.getName(),column.getValue());
                }
                MyKafkaSink.send(Constansts.GMALL_DB_CANAL,jsonObject.toJSONString());
            }
        }
    }
}
