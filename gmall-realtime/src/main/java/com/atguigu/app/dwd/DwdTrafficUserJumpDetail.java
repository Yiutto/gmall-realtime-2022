package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cep.*;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import scala.io.StdIn;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 数据流: web/app -> Nginx -> 日志服务器（.log）[node01/node02] -> Flume[node01/node02]  -> Kafka(ODS) -> FlinkApp[BaseLogApp] -> Kafka(DWD) -> FlinkApp[] -> Kafka(DWD)
 * 程序： Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwdTrafficUserJumpDetail -> Kafka(zk)
 * 需求：过滤用户跳出明细数据【跳出是指会话中只有一个页面的访问行为，如果能获取会话的所有页面，只要筛选页面数为 1 的会话即可获取跳出明细数据】
 **/

public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取环境变量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
        // 1.1 开启checkpoint
        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        env.getCheckpointConfig().setCheckpointStorage("hdfs://10.20.1.231:8020/flink_2022/ck");

        // 1.2 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        System.setProperty("HADOOP_USER_NAME", "hadoop");
         **/


        //TODO 2.读取kafka页面日志主题创建流
        String topic = "dwd_traffic_page_log";
        String groupId = "user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.将每行数据转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.map(str -> JSON.parseObject(str));

        //TODO 4.按照Mid分组 [提取事件时间作为watermark](为啥不在keyBy后面写)
        KeyedStream<JSONObject, String> keyedStream= jsonObjectDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }))
                .keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 5.定义CEP模式序列
        /**
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10));;
         **/

        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                }).times(2)  // 默认是宽松紧邻 followedBy
                .consecutive() // 严格紧邻 next
                .within(Time.seconds(10));

        //TODO 6.将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);
    
        //TODO 7.提取事件（匹配上的事件以及超时事件）
        OutputTag<String> timeOutTag = new OutputTag<String>("timeOut"){};

        SingleOutputStreamOperator<String> selectDS = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, String>() {
            @Override
            public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                // 获取key为start的数据，但是是一个list【考虑到 循环模式time(2)】
                return map.get("start").get(0).toJSONString();
            }
        }, new PatternSelectFunction<JSONObject, String>() {
            @Override
            public String select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0).toJSONString();
            }
        });
        DataStream<String> timeOutDS = selectDS.getSideOutput(timeOutTag);

        //TODO 8.合并2种事件【超时事件放在侧输出流】
        DataStream<String> unionDS = selectDS.union(timeOutDS);

        //TODO 9.将数据写出到kafka
        selectDS.print("Select>>>>>>");
        timeOutDS.print("Timeout>>>>>>");

        String targetTopic = "dwd_traffic_user_jump_detail";

        unionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));

        //TODO 10.启动任务
        env.execute("DwdTrafficUserJumpDetail");
    }
}
