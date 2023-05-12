package com.atguigu.app;

import com.atguigu.bean.WaterSensor1;
import com.atguigu.bean.WaterSensor2;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

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
                    split[1],
                    Long.parseLong(split[2]));
        });

        // 将流转换为动态表
        tableEnv.createTemporaryView("t1", waterSensor1DS);
        tableEnv.createTemporaryView("t2", waterSensor2DS);

        /**
         * 1001,23.5,8   1001,sensor_1,8
         * 1002,50.5,9   1002,s_2,9
         * 1001,23,10
         */
        // FlinkSql JOIN 【这里默认是处理时间，状态默认 不过期】

        /** inner join  左表：OnCreateAndWrite  右表：OnCreateAndWrite
        tableEnv.sqlQuery("select t1.id, t2.id, t1.vc, t2.name from t1 join t2 on t1.id=t2.id")
                .execute()
                .print();
         */

        /** left join 左表：OnReadAndWrite   右表：OnCreateAndWrite   【左表被读了就会更新状态过期时间， 间隔时间不能超过状态的TTL时间。比如右边数据在10s内输入，状态就不会过期】
        tableEnv.sqlQuery("select t1.id, t2.id, t1.vc, t2.name from t1 left join t2 on t1.id=t2.id")
                .execute()
                .print();
         */

        /** right join 左表：OnCreateAndWrite   右表：OnReadAndWrite   【右表被读了就会更新状态过期时间】
        tableEnv.sqlQuery("select t1.id, t2.id, t1.vc, t2.name from t1 right join t2 on t1.id=t2.id")
                .execute()
                .print();

         */

        /**
        // full join 左右表都是：OnReadAndWrite
        tableEnv.sqlQuery("select t1.id, t2.id, t1.vc, t2.name from t1 full join t2 on t1.id=t2.id")
                .execute()
                .print();
        */

        Table resultTable = tableEnv.sqlQuery("select t1.id t1_id, t2.id t2_id, t2.name from t1 full join t2 on t1.id=t2.id");
        tableEnv.createTemporaryView("result_table", resultTable);



        // 创建upsert kafka表
        tableEnv.executeSql("" +
                "create table upsert_test(" +
                "    t1_id string, " +
                "    t2_id string, " +
                "    name string, " +
                "    PRIMARY KEY (t1_id) NOT ENFORCED" +
                ") " + MyKafkaUtil.getUpsertKafkaDDL("test")
                );
        // 将数据写入Kafka
        tableEnv.executeSql("insert into upsert_test select * from result_table");
//         bin/kafka-console-consumer.sh --bootstrap-server node01:9092 --topic test

    }
}
