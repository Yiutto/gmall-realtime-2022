package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DimApp {
    public static void main(String[] args) {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 生产环境中设置为kafka主题的分区数

        /**  生产环境一定要写，这里注释为了方便测试
        // 1.1 开启CheckPoint
        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE); // 5min一次
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L); // 10min超时
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2); // 共存的checkpoint为2个
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L)); // 总共尝试3次重启，每隔5s一次

        // 1.2 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://node01:8020/flink_2022/ck");
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        **/

        // TODO 2.读取Kafka topic_db主题数据创建主流
        String topic = "topic_db";
        String groupId = "dim_app_2022";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // TODO 3.过滤非json数据，保留新增、变化以及初始化数据，并将数据转换为json格式
        // 新增 {"database":"gmall","table":"base_trademark","type":"insert","ts":1678331684,"xid":144025,"commit":true,"data":{"id":12,"tm_name":"yiutto","logo_url":"/static/yt.jpg"}}
        // 修改 {"database":"gmall","table":"base_trademark","type":"update","ts":1678331738,"xid":144149,"commit":true,"data":{"id":12,"tm_name":"yiutto","logo_url":"/static/lle.jpg"},"old":{"logo_url":"/static/yt.jpg"}}
        // 删除 {"database":"gmall","table":"base_trademark","type":"delete","ts":1678331805,"xid":144301,"commit":true,"data":{"id":12,"tm_name":"yiutto","logo_url":"/static/lle.jpg"}}

        // maxwell初始化  base_trademark
        // {"database":"gmall","table":"base_trademark","type":"bootstrap-start","ts":1678332023,"data":{}}
        // {"database":"gmall","table":"base_trademark","type":"bootstrap-insert","ts":1678332024,"data":{"id":1,"tm_name":"三星","logo_url":"/static/default.jpg"}}
        // ..........
        // {"database":"gmall","table":"base_trademark","type":"bootstrap-complete","ts":1678332024,"data":{}}

        SingleOutputStreamOperator<JSONObject> filterJsonDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);

                    // 获取数据中的操作类型字段
                    String type = jsonObject.getString("type");
                    if ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type)) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    //throw new RuntimeException(e);
                    // 可以打印，如果用process也可以写到侧输出流
                    System.out.println("发现脏数据：" + value);
                }

            }
        });


        // TODO 4.使用FlinkCDC读取MySQL配置信息表创建配置流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.20.1.231")
                .port(3306)
                .username("bigdata")
                .password("bigdata123")
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> mysqlSourceDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");

        // TODO 5.将配置流处理为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcast = mysqlSourceDS.broadcast(mapStateDescriptor);

        // TODO 6.连接主流和广播流
        // TODO 7.处理连接流，根据配置信息处理主流数据
        // TODO 8.将数据写到Phoenix
        // TODO 9.启动任务
    }
}
