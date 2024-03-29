package com.atguigu.app.dwd;

import com.atguigu.util.MyKafkaUtil;
import com.atguigu.util.MysqlUtil;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 *  * 数据流： web/app -> nginx -> 业务服务器(mysql) -> maxwell -> kafka(ods) -> flinkApp -> kafka(dwd)
 *  * 程序：Mock -> mysql -> maxwell -> kafka(zk) -> DwdTradeOrderRefund -> kafka(zk)
 */
public class DwdTradeOrderRefund {
    public static void main(String[] args) throws Exception{
        // TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        // 1.1 开启checkpoint
//        env.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://10.20.1.231:8020/flink/ck");
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        // 1.2 状态后端设置
//        System.setProperty("HADOOP_USER_NAME", "hadoop");
//        env.setStateBackend(new HashMapStateBackend());

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));   // 乱序时间5s

        // TODO 2.flinksql从kafka读取topic_db数据
        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`old` map<string, string>, " +
                "`proc_time` as PROCTIME(), " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "dwd_trade_order_refund"));

        // TODO 3. 读取退单表数据
        Table orderRefundInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "data['sku_id'] sku_id, " +
                "data['refund_type'] refund_type, " +
                "data['refund_num'] refund_num, " +
                "data['refund_amount'] refund_amount, " +
                "data['refund_reason_type'] refund_reason_type, " +
                "data['refund_reason_txt'] refund_reason_txt, " +
                "data['create_time'] create_time, " +
                "proc_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'order_refund_info' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // TODO 4. 读取订单表数据，筛选退单数据
        Table orderInfoRefund = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['province_id'] province_id, " +
                "`old` " +
                "from topic_db " +
                "where `table` = 'order_info' " +
                "and `type` = 'update' " +
                "and data['order_status']='1005' " +
                "and `old`['order_status'] is not null");

        tableEnv.createTemporaryView("order_info_refund", orderInfoRefund);

        // TODO 5. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 6. 关联三张表获得退单宽表
        Table resultTable = tableEnv.sqlQuery("select  " +
                "ri.id, " +
                "ri.user_id, " +
                "ri.order_id, " +
                "ri.sku_id, " +
                "oi.province_id, " +
                "date_format(ri.create_time,'yyyy-MM-dd') date_id, " +
                "ri.create_time, " +
                "ri.refund_type, " +
                "type_dic.dic_name refund_type_name, " +
                "ri.refund_reason_type, " +
                "reason_dic.dic_name refund_reason_type_name, " +
                "ri.refund_reason_txt, " +
                "ri.refund_num, " +
                "ri.refund_amount, " +
                "ri.ts, " +
                "current_row_timestamp() row_op_ts " +
                "from order_refund_info ri " +
                "join  " +
                "order_info_refund oi " +
                "on ri.order_id = oi.id " +
                "join  " +
                "base_dic for system_time as of ri.proc_time as type_dic " +
                "on ri.refund_type = type_dic.dic_code " +
                "join " +
                "base_dic for system_time as of ri.proc_time as reason_dic " +
                "on ri.refund_reason_type=reason_dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 8. 建立 Kafka-Connector dwd_trade_order_refund 表
        tableEnv.executeSql("create table dwd_trade_order_refund( " +
                "id string, " +
                "user_id string, " +
                "order_id string, " +
                "sku_id string, " +
                "province_id string, " +
                "date_id string, " +
                "create_time string, " +
                "refund_type_code string, " +
                "refund_type_name string, " +
                "refund_reason_type_code string, " +
                "refund_reason_type_name string, " +
                "refund_reason_txt string, " +
                "refund_num string, " +
                "refund_amount string, " +
                "ts string, " +
                "row_op_ts timestamp_ltz(3) " +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_order_refund"));

        // TODO 9. 将关联结果写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_trade_order_refund select * from result_table");
    }


}
