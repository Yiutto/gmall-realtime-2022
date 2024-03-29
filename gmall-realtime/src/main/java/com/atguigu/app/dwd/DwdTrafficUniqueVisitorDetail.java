package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DateFormatUtil;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
/**
 * 数据流: web/app -> Nginx -> 日志服务器（.log）[node01/node02] -> Flume[node01/node02]  -> Kafka(ODS) -> FlinkApp[BaseLogApp] -> Kafka(DWD) -> FlinkApp[] -> Kafka(DWD)
 * 程序： Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwdTrafficUniqueVisitorDetail -> Kafka(zk)
 * 需求：过滤页面数据中的独立访客访问记录【独立访客的页面必定是会话起始页面，last_page_id为null】
 **/
public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
        // 1.1 开启checkpoint
        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));

        // 1.2 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://10.20.1.231:8020/flink_2022/ck");
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        **/

        // TODO 2.读取kafka，页面日志主题创建流
        String topic = "dwd_traffic_page_log";
        String groupId = "unique_Visitor_Detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // TODO 3.过滤掉上一跳页面不为nulll的数据，并将每行数据转换为json对象 (又要过滤又要转换，用flatMap)
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    // 获取上一跳页面id
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (lastPageId == null) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(value);
                }
            }
        });

        // TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        // TODO 5.使用状态编程实现按照Mid的去重
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> lastVisitState; // 状态记录mid本次登录日期

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("last-visit", String.class);

                // 设置状态的TTL
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  // 更新状态的时候也更新过期时间
                        .build();
                stringValueStateDescriptor.enableTimeToLive(ttlConfig);

                lastVisitState = getRuntimeContext().getState(stringValueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 获取状态数据 and 当前数据中的时间戳并转换为日期
                String lastDate = lastVisitState.value();
                String curDate = DateFormatUtil.toDate(value.getLong("ts"));

                // 如果末次登录日期为null或者不是今日，则本次访问是该mid当日首次访问，保留数据，将末次登录日期更新为当日
                // 否则不是当日首次访问，丢弃数据。
                if (lastDate == null || !lastDate.equals(curDate)) {
                    lastVisitState.update(curDate);
                    return true;
                } else {
                    return false;
                }
            }
        });

        // TODO 6.将数据写到kafka
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        filterDS.print(">>>>>");
        filterDS.map(json -> json.toJSONString()).addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));

        // TODO 7.启动程序
        env.execute("DwdTrafficUniqueVisitorDetail");

    }
}
