package com.atguigu.app.dws;

import akka.japi.tuple.Tuple4;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DateFormatUtil;
import com.atguigu.bean.TrafficPageViewBean;
import com.atguigu.util.MyClickHouseUtil;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 数据流1: web/app -> Nginx -> 日志服务器（.log）[node01/node02] -> Flume[node01/node02]  -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
 *  * 数据流2【UJ】: web/app -> Nginx -> 日志服务器（.log）[node01/node02] -> Flume[node01/node02]  -> Kafka(ODS) -> FlinkApp[BaseLogApp] -> Kafka(DWD) -> FlinkApp[DwdTrafficUserJumpDetail] -> Kafka(DWD)
 *  * 数据流3【UV】: web/app -> Nginx -> 日志服务器（.log）[node01/node02] -> Flume[node01/node02]  -> Kafka(ODS) -> FlinkApp[BaseLogApp] -> Kafka(DWD) -> FlinkApp[DwdTrafficUniqueVisitorDetail] -> Kafka(DWD)
 ******[最终]====> FlinkApp -> ClickHouse(DWS)

 * 程序1： Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK)
 *  * 程序2： Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwdTrafficUserJumpDetail -> Kafka(zk)
 *  * 程序3： Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwdTrafficUniqueVisitorDetail -> Kafka(zk)
 *****【最终】====> DwsTrafficVcChArIsNewPageViewWindow -> ClickHouse(zk)
 *
 *
 * 需求：流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 根据kafka分区来设定

//        // 1.1 状态后端设置1
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.minutes(1)));
//
//        // 1.2 状态后端设置2
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://10.20.1.231:8020/ck");
//        System.setProperty("HADOOP_USER_HOME", "hadoop");

        // TODO 2.读取3个主题kafka并创建流
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        String ujTopic = "dwd_traffic_user_jump_detail";
        String pageTopic = "dwd_traffic_page_log";
        String groupId = "vccharisnew_pageview_window";
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(uvTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(ujTopic, groupId));
        DataStreamSource<String> pageDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageTopic, groupId));

        // TODO 3.统一数据格式
        /**
        {"common":{"ar":"420000","uid":"578","os":"Android 11.0","ch":"huawei","is_new":"1","md":"Xiaomi 9","mid":"mid_380821","vc":"v2.1.132","ba":"Xiaomi"},
            "page":{"page_id":"good_list","item":"电视","during_time":2552,"item_type":"keyword","last_page_id":"search"},"ts":1684998153000}
        **/
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUvDS = uvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts")
            );
        });
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUjDS = ujDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 0L, 1L,
                    jsonObject.getLong("ts")
            );
        });

        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithPageDS = pageDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");
            String lastPageId = page.getString("last_page_id");
            long sv = 0L;
            if (lastPageId == null) {
                sv = 1L;
            }
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, sv, 0L, page.getLong("during_time"), 0L,
                    jsonObject.getLong("ts")
            );
        });

        // TODO 4.将3个流进行Union
        DataStream<TrafficPageViewBean> unionDS = trafficPageViewWithUvDS.union(
                trafficPageViewWithUjDS,
                trafficPageViewWithPageDS);

        // TODO 5.提取事件时间生成WaterMark
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithWmDS = unionDS.assignTimestampsAndWatermarks(
                //WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(14))  // 结合DwdTrafficUserJumpDetail, 用了CEP【10s超时+2s乱序】 +2s(本身任务自身的乱序)
                .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                    @Override
                    public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        // TODO 6.分组开窗聚合
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowStream = trafficPageViewWithWmDS.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {
                return new Tuple4<>(value.getAr(),
                        value.getCh(),
                        value.getIsNew(),
                        value.getVc());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));

        /**
         开窗操作：
              OpWindow: windowAll()
              KeyedWindow: window
                    时间：滚动、滑动、会话
                    计数：滚动、滑动
              窗口聚合函数：
                    增量聚合函数：来一条计算一条
                        效率高、存储数据量小
                    全量聚合函数：攒到一个集合汇总，最后统一计算 【可以获取窗口信息】
                        可以求前百分比

        // 增量聚合
        windowStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                return null;
            }
        });
        // 全量聚合
        windowStream.apply(new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {

            }
        });
         **/
        SingleOutputStreamOperator<TrafficPageViewBean> resultDS = windowStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                return value1;
            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window,
                              Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                // 获取数据
                TrafficPageViewBean next = input.iterator().next();

                // 补充信息
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                // 修改ts
                next.setTs(System.currentTimeMillis());

                // 输出数据
                out.collect(next);
            }
        });

        // TODO 7.将数据写出到clickhouse
        resultDS.print(">>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_vc_ch_ar_is_new_page_view_window " +
                "values (?,?,?,?,?,?,?,?,?,?,?,?)"));

        // TODO 8.启动任务
        env.execute("DwsTrafficVcChArIsNewPageViewWindow");

    }


}
