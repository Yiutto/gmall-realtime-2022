package com.atguigu.app.dws;

import com.atguigu.app.func.SplitFunction;
import com.atguigu.bean.KeywordBean;
import com.atguigu.util.MyClickHouseUtil;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;
/**
 * 数据流: web/app -> Nginx -> 日志服务器（.log）[node01/node02] -> Flume[node01/node02]  -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
 * 程序： Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwsTrafficSourceKeywordPageViewWindow -> ClickHouse(ZK)
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        // TODO 1.1  环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

//        // TODO 1.2 启用状态后端
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.setRestartStrategy(
//                RestartStrategies.failureRateRestart(3, Time.days(1L), Time.minutes(3L))
//        );
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 2.使用DDL方式读取kafka page_log 主题的数据创建表并且提取时间戳生成watermark
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_source_keyword_page_view_window";
        tableEnv.executeSql("" +
                "create table page_log( " +
                "  `page` map<string,string>, " +
                "  `ts`  bigint, " +
                "  `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "   WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
                ")" + MyKafkaUtil.getKafkaDDL(topic, groupId));

        // TODO 3.过滤出搜索数据
        // "item":"小米","item_type":"keyword","last_page_id":"search"
        Table filterTable = tableEnv.sqlQuery("" +
                "select  " +
                "    page['item'] item, " +
                "    rt " +
                "from page_log " +
                "where page['last_page_id'] = 'search' " +
                "and page['item_type'] = 'keyword' " +
                "and page['item'] is not null");
        tableEnv.createTemporaryView("filter_table", filterTable);

        // TODO 4.注册UDTF & 切词
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        Table splitTable = tableEnv.sqlQuery("" +
                "SELECT " +
                "    word, " +
                "    rt " +
                "FROM filter_table, LATERAL TABLE(SplitFunction(item))");
        tableEnv.createTemporaryView("splitTable", splitTable);

        // TODO 5.分组、开窗、聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "    'search' source, " +
                "    word keyword, " +
                "    count(*) keyword_count, " +
                "    UNIX_TIMESTAMP()*1000 ts " +
                "from splitTable " +
                "group by word,TUMBLE(rt, INTERVAL '10' SECOND)");

        // TODO 6.将动态表转换为流
        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);// 通过名字关联
        keywordBeanDataStream.print(">>>>>>>");
        // TODO 7.将数据写出到clickhouse
        keywordBeanDataStream.addSink(MyClickHouseUtil.getSinkFunction(
                "insert into dws_traffic_source_keyword_page_view_window values (?,?,?,?,?,?)"));  // 只看字段顺序

        // TODO 8.启动任务
        env.execute();
    }
}
