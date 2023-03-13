package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DateFormatUtil;
import com.atguigu.util.MyKafkaUtil;
import io.debezium.data.Json;
import jdk.nashorn.internal.runtime.RecompilableScriptFunctionData;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 数据流: web/app -> Nginx -> 日志服务器（.log）[node01/node02] -> Flume[node01/node02]  -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
 * 程序： Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK)
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 生成环境设置成kafka主题的分区数

        // 1.1 开区checkpoint
//        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
//
//        // 1.2 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://10.20.1.231:8020/flink_2022/ck");
//        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // TODO 2.消费Kafka topic_log 主题的数据 创建流
        String topic = "topic_log";
        String groupId = "base_log_app_2022";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // TODO 3.过滤掉非JSON格式的数据&将每行数据转换为json对象【侧输出流】
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });

        // 获取侧输出流脏数据并打印
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("Dirty>>>>>>");

        // TODO 4.按照Mid分组
        // {"common":{"ar":"500000","ba":"Xiaomi","ch":"oppo","is_new":"1","md":"Xiaomi Mix2 ","mid":"mid_458291","os":"Android 8.1","uid":"198","vc":"v2.1.134"},
        //    "start":{"entry":"icon","loading_time":15424,"open_ad_id":18,"open_ad_ms":7757,"open_ad_skip_ms":0},"ts":1592100700000}

        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        // TODO 5.使用状态编程作 新老访客标记校验
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last_visit", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                // 获取is_new标记 和 ts
                String isNew = value.getJSONObject("common").getString("is_new");
                Long ts = value.getLong("ts");
                String curDate = DateFormatUtil.toDate(ts);

                // 获取状态中的日期
                String lastDate = lastVisitState.value();

                // 判断is_new标记是否为“1”
                if ("1".equals(isNew)){
                    if (lastDate == null) {
                        // 如果键控状态为null，则将日志中ts对应的日期更新到状态中，不对is_new作处理
                        lastVisitState.update(curDate);
                    } else if (! lastDate.equals(curDate)) {
                        // 如果键控状态不为null，且首次访问日期不是当日，则将is_now字段值为0
                        value.getJSONObject("common").put("is_new", "0");
                    }

                } else{
                    if (lastDate == null){
                        // 如果键控状态为null，则将状态中的首次访问日期更新为昨日。这样做可以保证同一mid的其他日志到来时，依然会被判定为老访客；
                        lastVisitState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));
                    }

                }

                return value;
            }
        });
        // TODO 6.使用侧输出流进行分流处理 【页面日志放主流】【启动、曝光、动作、错误放到侧输出流】
        // 错误（启动错误、页面错误）
        // 启动
        // 页面-（曝光、动作）
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};
        OutputTag<String> actionTag = new OutputTag<String>("action"){};
        OutputTag<String> errorTag = new OutputTag<String>("error"){};
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {

                // TODO a.尝试获取错误信息
                String err = value.getString("err");
                if (err != null) {
                    // 将数据写到error侧输出流
                    ctx.output(errorTag, value.toJSONString());
                }

                // 移除错误信息
                value.remove("err");

                // TODO b.尝试获取启动信息
                String start = value.getString("start");
                if (start != null) {
                    // 将数据写到start侧输出流
                    ctx.output(startTag, value.toJSONString());
                } else {
                    // 获取公共信息&页面id&时间戳，【ts，common，page_id】
                    String common = value.getString("common");
                    String pageId = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");

                    // TODO c.尝试获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        // 遍历曝光数据，写到display侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);

                            display.put("common", common);
                            display.put("page_id", pageId);
                            display.put("ts", ts);

                            ctx.output(displayTag, display.toJSONString());
                        }
                    }

                    // TODO d.尝试获取动作数据
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        // 遍历动作数据，写到action侧输出流
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);

                            action.put("common", common);
                            action.put("page_id", pageId);
                            // display.put("ts", ts);

                            ctx.output(actionTag, action.toJSONString());
                        }
                    }

                    // 移除曝光和动作信息，保存页面日志写到主流
                    value.remove("displays");
                    value.remove("actions");

                    // TODO e.剩下就是页面数据
                    out.collect(value.toJSONString());
                }

            }
        });
        // TODO 7.提取各个侧输出流数据
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);

        // TODO 8.将数据打印并写入对应的主题topic
        pageDS.print("Page>>>>");
        startDS.print("Start>>>>");
        displayDS.print("Display>>>>");
        actionDS.print("Action>>>>");
        errorDS.print("error>>>>");

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(start_topic));
        displayDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(action_topic));
        errorDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(error_topic));

        // TODO 9.启动任务
        env.execute("BaseLogApp");
    }
}
