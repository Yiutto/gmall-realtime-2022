package com.atguigu.app.dwd;

import com.atguigu.util.MyKafkaUtil;
import com.atguigu.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

/**
 * 数据流： web/app -> nginx -> 业务服务器(mysql) -> maxwell -> kafka(ods) -> flinkApp -> kafka(dwd)
 * 程序：Mock -> mysql -> maxwell -> kafka(zk) -> DwdTradeOrderPreProcess -> kafka(zk)
 */
public class DwdTradeOrderPreProcess {
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

        // 1.3 设置状态TTL 生成环境设置最大乱序时间为5s
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // TODO 2.创建topic_db表
        tableEnv.executeSql(MyKafkaUtil.getTopicDb("Order_Pre_Process"));

        // TODO 3.过滤出订单明细数据
        Table orderDetailTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] AS id, " +
                "    data['order_id'] AS order_id, " +
                "    data['sku_id'] AS sku_id, " +
                "    data['sku_name'] AS sku_name, " +
                "    data['order_price'] AS order_price, " +
                "    data['sku_num'] AS sku_num, " +
                "    data['create_time'] AS create_time, " +
                "    data['source_type'] AS source_type, " +
                "    data['source_id'] AS source_id, " +
                "    data['split_total_amount'] AS split_total_amount, " +
                "    data['split_activity_amount'] AS split_activity_amount, " +
                "    data['split_coupon_amount'] AS split_coupon_amount, " +
                "    pt " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail' " +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail_table", orderDetailTable);
        // 转换为流并打印测试
        //tableEnv.toAppendStream(orderDetailTable, Row.class).print("1>>>>>>>>");

        // TODO 4.过滤出订单数据
        Table orderInfoTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] AS id, " +
                "    data['consignee'] AS consignee, " +
                "    data['consignee_tel'] AS consignee_tel, " +
                "    data['total_amount'] AS total_amount, " +
                "    data['order_status'] AS order_status, " +
                "    data['user_id'] AS user_id, " +
                "    data['payment_way'] AS payment_way, " +
                "    data['delivery_address'] AS delivery_address, " +
                "    data['order_comment'] AS order_comment, " +
                "    data['out_trade_no'] AS out_trade_no, " +
                "    data['trade_body'] AS trade_body, " +
                "    data['create_time'] AS create_time, " +
                "    data['operate_time'] AS operate_time, " +
                "    data['expire_time'] AS expire_time, " +
                "    data['process_status'] AS process_status, " +
                "    data['tracking_no'] AS tracking_no, " +
                "    data['parent_order_id'] AS parent_order_id, " +
                "    data['province_id'] AS province_id, " +
                "    data['activity_reduce_amount'] AS activity_reduce_amount, " +
                "    data['coupon_reduce_amount'] AS coupon_reduce_amount, " +
                "    data['original_total_amount'] AS original_total_amount, " +
                "    data['feight_fee'] AS feight_fee, " +
                "    data['feight_fee_reduce'] AS feight_fee_reduce, " +
                "    data[']refundable_time'] AS refundable_time, " +
                "    `type`, " +
                "    `old` " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_info' "+
                "and (`type` = 'insert' OR `type` = 'update')");
        tableEnv.createTemporaryView("order_info_table", orderInfoTable);
        // 转换为流并打印测试
        //tableEnv.toAppendStream(orderInfoTable, Row.class).print("2>>>>>>>>>>>");

        // TODO 5.过滤出订单明细活动关联数据
        Table orderActivityTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] AS id, " +
                "    data['order_id'] AS order_id, " +
                "    data['order_detail_id'] AS order_detail_id, " +
                "    data['activity_id'] AS activity_id, " +
                "    data['activity_rule_id'] AS activity_rule_id, " +
                "    data['sku_id'] AS sku_id,            " +
                "    data['create_time'] AS create_time " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail_activity' "+
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_activity_table", orderActivityTable);

        // 转换为流并打印输出测试
        //tableEnv.toAppendStream(orderActivityTable, Row.class).print("3>>>>>");

        // TODO 6.过滤出订单明细购物券关联数据
        Table orderCouponTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] AS id, " +
                "    data['order_id'] AS order_id, " +
                "    data['order_detail_id'] AS order_detail_id, " +
                "    data['coupon_id'] AS coupon_id, " +
                "    data['coupon_use_id'] AS coupon_use_id, " +
                "    data['sku_id'] AS sku_id, " +
                "    data['create_time'] AS create_time " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail_coupon' "+
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_coupon_table", orderCouponTable);
        // 转换为流并打印输出测试
        //tableEnv.toAppendStream(orderCouponTable, Row.class).print("4>>>>>");

        // TODO 7.创建base_dic LookUp表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 8.关联5张表
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    od.id, " +
                "    od.order_id, " +
                "    od.sku_id, " +
                "    od.sku_name, " +
                "    od.order_price, " +
                "    od.sku_num, " +
                "    od.create_time, " +
                "    od.source_type as source_type_id, " +
                "    dic.dic_name as source_type_name, " +
                "    od.source_id, " +
                "    od.split_total_amount, " +
                "    od.split_activity_amount, " +
                "    od.split_coupon_amount, " +
                "    oi.consignee, " +
                "    oi.consignee_tel, " +
                "    oi.total_amount, " +
                "    oi.order_status, " +
                "    oi.user_id, " +
                "    oi.payment_way, " +
                "    oi.delivery_address, " +
                "    oi.order_comment, " +
                "    oi.out_trade_no, " +
                "    oi.trade_body, " +
                "    oi.operate_time, " +
                "    oi.expire_time, " +
                "    oi.process_status, " +
                "    oi.tracking_no, " +
                "    oi.parent_order_id, " +
                "    oi.province_id, " +
                "    oi.activity_reduce_amount, " +
                "    oi.coupon_reduce_amount, " +
                "    oi.original_total_amount, " +
                "    oi.feight_fee, " +
                "    oi.feight_fee_reduce, " +
                "    oi.refundable_time, " +
                "    oa.id as order_detail_activity_id, " +
                "    oa.activity_id, " +
                "    oa.activity_rule_id, " +
                "    oc.id as order_detail_coupon_id, " +
                "    oc.coupon_id, " +
                "    oc.coupon_use_id, " +
                "    oi.`type`, " +
                "    oi.`old` " +
                "from order_detail_table od " +
                "join order_info_table oi on od.order_id=oi.id " +
                "left join order_activity_table oa on od.id=oa.order_detail_id " +
                "left join order_coupon_table oc on od.id=oc.order_detail_id " +
                "JOIN base_dic FOR SYSTEM_TIME AS OF od.pt as dic on od.source_type=dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);
        // 转换为流并打印输出测试【里面有left join,用撤回流】
        tableEnv.toRetractStream(resultTable, Row.class).print("3>>>>>");

        // TODO 9.创建upsert-kafka表
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
                "   `old`   map<string,string>, " +
                "   primary key(id) not enforced  " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_pre_process"));

        // TODO 10.将数据写出
        tableEnv.executeSql("insert into dwd_order_pre select * from result_table");

        // TODO 11.启动任务
        env.execute("DwdTradeOrderPreProcess");
    }
}
