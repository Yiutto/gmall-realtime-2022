package com.atguigu.app.dwd;

import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *  * 数据流： web/app -> nginx -> 业务服务器(mysql) -> maxwell -> kafka(ods) -> flinkApp -> kafka(dwd) -> flinkApp -> Kafka(dwd)
 *  * 程序：Mock -> mysql -> maxwell -> kafka(zk) -> DwdTradeOrderPreProcess -> kafka(zk) -> DwdTradeOrderDetail -> kafka(zk)
 */
public class DwdTradeOrderDetail {
    public static void main(String[] args) {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
        // TODO 2.读取kafka订单预处理主题数据创建表
        tableEnv.executeSql("" +
                "create table dwd_order_pre( " +
                "   `id`  string, " +
                "   `order_id`  string, " +
                "   `sku_id`  string, " +
                "   `sku_name`  string, " +
                "   `order_price`  string, " +
                "   `sku_num`  string, " +
                "   `create_time`  string, " +
                "   `source_type_id`  string, " +
                "   `source_type_name`  string, " +
                "   `source_id`  string, " +
                "   `split_total_amount`  string, " +
                "   `split_activity_amount`  string, " +
                "   `split_coupon_amount`  string, " +
                "   `consignee`  string, " +
                "   `consignee_tel`  string, " +
                "   `total_amount`  string, " +
                "   `order_status`  string, " +
                "   `user_id`  string, " +
                "   `payment_way`  string, " +
                "   `delivery_address`  string, " +
                "   `order_comment`  string, " +
                "   `out_trade_no`  string, " +
                "   `trade_body`  string, " +
                "   `operate_time`  string, " +
                "   `expire_time`  string, " +
                "   `process_status`  string, " +
                "   `tracking_no`  string, " +
                "   `parent_order_id`  string, " +
                "   `province_id`  string, " +
                "   `activity_reduce_amount`  string, " +
                "   `coupon_reduce_amount`  string, " +
                "   `original_total_amount`  string, " +
                "   `feight_fee`  string, " +
                "   `feight_fee_reduce`  string, " +
                "   `refundable_time`  string, " +
                "   `order_detail_activity_id`  string, " +
                "   `activity_id`  string, " +
                "   `activity_rule_id`  string, " +
                "   `order_detail_coupon_id`  string, " +
                "   `coupon_id`  string, " +
                "   `coupon_use_id`  string, " +
                "   `type`  string, " +
                "   `old`   map<string,string> " +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_order_pre_process", "Order_Detail"));

        // TODO 3.过滤出下单数据，即新增数据
        Table filteredTable = tableEnv.sqlQuery("" +
                "select " +
                "    id, " +
                "    order_id, " +
                "    user_id, " +
                "    sku_id, " +
                "    sku_name, " +
                "    sku_num, " +
                "    order_price, " +  // +++
                "    province_id, " +
                "    activity_id, " +
                "    activity_rule_id, " +
                "    coupon_id, " +
                // "    date_id, " +
                "    create_time, " +
                "    source_id, " +
                "    source_type_id, " + //"    source_type source_type_code, " +
                "    source_type_name, " +

                //"    split_original_amount, " +
                "    split_activity_amount, " +
                "    split_coupon_amount, " +
                "    split_total_amount " +
                //"    od_ts ts, " +
                //"    row_op_ts " +
                "from dwd_order_pre " +
                "where `type`='insert'");
        tableEnv.createTemporaryView("filtered_table", filteredTable);

        // TODO 4.创建DWD层下单数据表
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "sku_num string, " +   // ++
                "order_price string, " +   // ++
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
//                "date_id string, " +
                "create_time string, " +
                "source_id string, " +
                "source_type_id string, " +    // "source_type_code string, " +
                "source_type_name string, " +
//                "sku_num string, " +
//                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string " +  // 删掉,
//                "ts string, " +
//                "row_op_ts timestamp_ltz(3) " +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_order_detail"));
        // TODO 5.将数据写出到kafka

        tableEnv.executeSql("insert into dwd_trade_order_detail select * from filtered_table");

        // TODO 6.启动任务


    }
}
