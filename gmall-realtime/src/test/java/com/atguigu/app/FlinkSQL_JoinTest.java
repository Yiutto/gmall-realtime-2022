package com.atguigu.app;

import com.atguigu.bean.WaterSensor1;
import com.atguigu.bean.WaterSensor2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class FlinkSQL_JoinTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 默认状态不过期
        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        // 重设过期时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // FlinkSQL可以用事件时间和处理时间
        SingleOutputStreamOperator<WaterSensor1> waterSensor1DS= env.socketTextStream("10.20.1.231", 8888).map(line -> {
            String[] split = line.split(",");
            return new WaterSensor1(
                    split[0],
                    Double.parseDouble(split[1]),
                    Long.parseLong(split[2]));
        });

        SingleOutputStreamOperator<WaterSensor2> waterSensor2DS = env.socketTextStream("10.20.1.231", 9999).map(line -> {
            String[] split = line.split(",");
            return new WaterSensor2(
                    split[0],
                    split[2],
                    Long.parseLong(split[2]));
        });

        // 将流转换为动态表
        tableEnv.createTemporaryView("t1", waterSensor1DS);
        tableEnv.createTemporaryView("t2", waterSensor2DS);

        /**
         * 1001,23.5,8   1001,sensor_1,8
         * 1001,50.5,9   1001,s_2,9
         * 1001,23,10
         */
        // FlinkSql JOIN 【这里默认是处理时间，状态默认 不过期】

        /** inner join
        tableEnv.sqlQuery("select t1.id, t2.id, t1.vc, t2.name from t1 join t2 on t1.id=t2.id")
                .execute()
                .print();
         */

        tableEnv.sqlQuery("select t1.id, t2.id, t1.vc, t2.name from t1 left join t2 on t1.id=t2.id")
                .execute()
                .print();

    }
}
