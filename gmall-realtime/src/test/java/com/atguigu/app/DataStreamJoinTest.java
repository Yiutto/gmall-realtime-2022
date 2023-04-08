package com.atguigu.app;

import com.atguigu.bean.WaterSensor1;
import com.atguigu.bean.WaterSensor2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class DataStreamJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 这个版本里面只能用事件时间，不能用处理时间，用处理时间得用FlinkSql
        SingleOutputStreamOperator<WaterSensor1> waterSensorDS1 = env.socketTextStream("10.20.1.231", 8888).assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] split = element.split(",");
                        return new Long(split[2]) * 1000L;
                    }
                })).map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor1(
                            split[0],
                            Double.parseDouble(split[1]),
                            Long.parseLong(split[2])
            );
        });

        SingleOutputStreamOperator<WaterSensor2> waterSensorDS2 = env.socketTextStream("10.20.1.231", 9999).assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] split = element.split(",");
                        return new Long(split[2]) * 1000L;
                    }
                })).map(line -> {
            String[] split = line.split(",");
            return new WaterSensor2(
                    split[0],
                    split[1],
                    Long.parseLong(split[2])
            );
        });

        SingleOutputStreamOperator<Tuple2<WaterSensor1, WaterSensor2>> result = waterSensorDS1.keyBy(WaterSensor1::getId)
                .intervalJoin(waterSensorDS2.keyBy(WaterSensor2::getId))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<WaterSensor1, WaterSensor2, Tuple2<WaterSensor1, WaterSensor2>>() {
                    @Override
                    public void processElement(WaterSensor1 left, WaterSensor2 right, Context ctx,
                                               Collector<Tuple2<WaterSensor1, WaterSensor2>> out) throws Exception {
                        out.collect(new Tuple2<>(left, right));

                    }
                });
        result.print(">>>>>>>>>>>>>>>>>>>>>>>>>>");
        /** 测试数据
         * 1001,23.5,8     1001,sensor_1,8
         * 1001,23.5,9     1001,sensor_2,9
         * 1002,89.5,15    1001,sensor_3,10
         *                 1002,sensor_1,15
         *                 1001,sensor_3,10
         * 1003,23.5,8     1003,s_1,8
         */
        env.execute();
    }
}
