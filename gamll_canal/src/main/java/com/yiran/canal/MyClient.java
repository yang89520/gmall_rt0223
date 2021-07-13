package com.yiran.canal;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.yiran.gmall.Constansts;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.yiran.gmall.realtime.util.MyKafkaSink;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by Smexy on 2021/7/7
 *
 *      步骤：
 *              ①创建一个客户端
 *                      CanalConnector
 *                          SimpleCanalConnector:  单节点canal集群
 *                          ClusterCanalConnector： HA canal集群
 *
 *
 *              ②使用客户端连接 canal server
 *              ③指定客户端订阅 canal server中的binlog信息
 *              ④解析binlog信息
 *              ⑤写入kafka
 *
 *
 *
 *
 *        消费到的数据的结构：
 *              Message:  代表拉取的一批数据！这一批数据可能是多个SQL执行，造成的写操作变化
 *                  List<Entry> entries ： 每个Entry代表一个SQL造成的写操作变化
 *                  id ： -1 说明没有拉取到数据
 *
 *
 *              Entry:
 *                       CanalEntry.Header header_ :  头信息，其中包含了对这条sql的一些说明
 *                              private Object tableName_: sql操作的表名
 *                              EntryType; Entry操作的类型
 *                                      开启事务： 写操作  begin
 *                                      提交事务： 写操作  commit
 *                                      对原始数据进行影响的写操作： rowdata
 *                                              update
 *                                              delete
 *                                              insert
 *
 *
 *
 *                       ByteString storeValue_：   数据
 *                          序列化数据，需要使用工具类RowChange，进行转换,转换之后获取到一个RowChange对象
 *
 *
 *
 */
public class MyClient {
    //todo canal 集群模式java连接超时
    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException {

        /*
            创建一个客户端
                newSingleConnector(SocketAddress address,   指定canal server的主机名和端口号
                                 String destination,  参考 canal.properties 中的canal.destinations
                                                            destination 可以选择其中的一个或多个destination
                                 String username,  不是instance.properties中的canal.instance.dbUsername，
                                                            参考AdminGuide
                                                            是从canal 1.1.4 之后才提供的
                                 String password)
         */
//        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop103",
//                11111), "example", "", "");

        //CanalConnector connector = CanalConnectors.newClusterConnector("hadoop102:2181,hadoop103:2181,hadoop104:2181","mirror1", "canal", "canal");

        CanalConnector connector = CanalConnectors.newClusterConnector("hadoop102:2181,hadoop103:2181,hadoop104:2181", "mirror1", "", "");


        //使用客户端连接 canal server
        connector.connect();

        //指定客户端订阅 canal server中的binlog信息  只统计Order_info表
        connector.subscribe("gmall_realtime.order_info");

        //不停地拉取数据   Message[id=-1,entries=[],raw=false,rawEntries=[]] 代表当前这批没有拉取到数据
        while (true){

            Message message = connector.get(100);

            //判断是否拉取到了数据，如果没有拉取到，歇一会再去拉取
            if (message.getId() == -1){

                System.out.println("歇一会，再去拉取!");

                Thread.sleep(5000);

                continue;
            }

            // 数据的处理逻辑
            // System.out.println(message);

            List<CanalEntry.Entry> entries = message.getEntries();

            for (CanalEntry.Entry entry : entries) {

                //判断这个entry的类型是不是rowdata类型，只处理rowdata类型
                if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)){

                    ByteString storeValue = entry.getStoreValue();
                    String tableName = entry.getHeader().getTableName();

                    handleStoreValue(storeValue,tableName);

                }

            }
        }
    }

    private static void handleStoreValue(ByteString storeValue, String tableName) throws InvalidProtocolBufferException {

        //将storeValue 转化为 RowChange
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

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
        if (rowChange.getEventType().equals(CanalEntry.EventType.INSERT)){

            //获取 行的集合
            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

            for (CanalEntry.RowData rowData : rowDatasList) {

                JSONObject jsonObject = new JSONObject();

                //获取insert后每一行的每一列
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

                for (CanalEntry.Column column : afterColumnsList) {

                    jsonObject.put(column.getName(),column.getValue());

                }

                //获取列名和列值
                System.out.println(jsonObject);

                //MyKafkaSink.send(Constansts.GMALL_DB_CANAL,jsonObject.toJSONString());
            }

        }


    }
}

