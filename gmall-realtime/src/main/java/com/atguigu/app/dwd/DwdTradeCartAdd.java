package com.atguigu.app.dwd;

import com.atguigu.util.MyKafkaUtil;
import com.atguigu.util.MysqlUtil;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 *  * 数据流： web/app -> nginx -> 业务服务器(mysql) -> maxwell -> kafka(ods) -> flinkApp -> kafka(dwd)
 *  * 程序：Mock -> mysql -> maxwell -> kafka(zk) -> DwdTradeCartAdd -> kafka(zk)
 */
public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 生产环境中设置为kafka主题的分区数
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.1 开启checkpoint
//        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));

        // 1.2 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://10.20.1.231:8020/flink/ck");
//        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // TODO 2.使用DDL方式读取topic_db主题的数据创建表[Kafka-Source-DDL]
        tableEnv.executeSql(MyKafkaUtil.getTopicDb("cart_add_230417"));

        // TODO 3.过滤出加购数据
        Table cardAddTable = tableEnv.sqlQuery("" +
                "select " +
                "    `data`['id'] AS id, " +
                "    `data`['user_id'] AS user_id, " +
                "    `data`['sku_id'] AS sku_id, " +
                "    `data`['cart_price'] AS cart_price, " +
                "    if(`type` = 'insert', `data`['sku_num'], cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) AS sku_num, " +
                "    `data`['img_url'] AS img_url, " +
                "    `data`['sku_name'] AS sku_name, " +
                "    `data`['is_checked'] AS is_checked, " +
                "    `data`['create_time'] AS create_time, " +
                "    `data`['operate_time'] AS operate_time, " +
                "    `data`['is_ordered'] AS is_ordered, " +
                "    `data`['order_time'] AS order_time, " +
                "    `data`['source_type'] AS source_type, " +
                "    `data`['source_id'] AS source_id, " +
                "    pt " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'cart_info' " +
                "and (`type` = 'insert' " +
                "or (`type` = 'update' " +
                "    and `old`['sku_num'] is not null " +
                "    and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)))");

        // 将加购表转换为流并打印测试
        //tableEnv.toAppendStream(cardAddTable, Row.class).print(">>>>>");
        tableEnv.createTemporaryView("cart_info_table", cardAddTable);

        // TODO 4.读取MySQL的base_dic表作为LookUp表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 5.关联2张表
        Table cartAddWithDicTable = tableEnv.sqlQuery("" +
                "SELECT " +
                "  ci.id, " +
                "  ci.user_id, " +
                "  ci.sku_id, " +
                "  ci.cart_price, " +
                "  ci.sku_num, " +
                "  ci.img_url, " +
                "  ci.sku_name, " +
                "  ci.is_checked, " +
                "  ci.create_time, " +
                "  ci.operate_time, " +
                "  ci.is_ordered, " +
                "  ci.order_time, " +
                "  ci.source_type source_type_id, " +
                "  dic.dic_name source_type_name, " +
                "  ci.source_id " +
                "FROM cart_info_table ci " +
                "JOIN base_dic FOR SYSTEM_TIME AS OF ci.pt AS dic " +
                "ON ci.source_type = dic.dic_code");


        // TODO 6.使用DDL方式创建加购事实表[Kafka-SINK-DDL]
        tableEnv.executeSql("" +
                "create table dwd_cart_add( " +
                "    `id` STRING, " +
                "    `user_id` STRING, " +
                "    `sku_id` STRING, " +
                "    `cart_price` STRING, " +
                "    `sku_num` STRING, " +
                "    `img_url` STRING, " +
                "    `sku_name` STRING, " +
                "    `is_checked` STRING, " +
                "    `create_time` STRING, " +
                "    `operate_time` STRING, " +
                "    `is_ordered` STRING, " +
                "    `order_time` STRING, " +
                "    `source_type_id` STRING, " +
                "    `source_type_name` STRING,  " +
                "    `source_id` STRING " +
                ") " + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add"));

        // TODO 7.将数据写出到kakfa
        tableEnv.executeSql("insert into dwd_cart_add select * from " + cartAddWithDicTable).print();
        //tableEnv.createTemporaryView("cart_add_dic_table", cartAddWithDicTable);
        //tableEnv.executeSql("insert into dwd_cart_add select * from cart_add_dic_table");

        // TODO 8.启动任务
         env.execute();
    }
}
