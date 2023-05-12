package com.atguigu.app;

import com.atguigu.bean.WaterSensor1;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import scala.reflect.internal.Trees;

import static org.apache.flink.table.api.Expressions.$;

public class Flink_LookUp_JoinTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 查询mysql构建LookUp表
        tableEnv.executeSql("" +
                "CREATE TEMPORARY TABLE base_dic( " +
                "   `dic_code` string, " +
                "   `dic_name` string, " +
                "   `parent_code` string, " +
                "   `create_time` string, " +
                "   `operate_time` string " +
                ") WITH ( " +
                "  'connector' = 'jdbc', " +
                "  'url' = 'jdbc:mysql://10.20.1.231:3306/gmall', " +
                "  'table-name' = 'base_dic', " +
                "  'driver' = 'com.mysql.cj.jdbc.Driver', " +
                "  'lookup.cache.max-rows' = '10', " +
                "  'lookup.cache.ttl' = '1 hour', " +
                "  'username' = 'bigdata', " +
                "  'password' = 'bigdata123' " +
                ")"
                );

//        tableEnv.sqlQuery("select * from base_dic")
//                .execute().print();
        // 构建事实表
        SingleOutputStreamOperator<WaterSensor1> waterSensorDS1 = env.socketTextStream("10.20.1.231", 8888).map(line -> {
            String[] split = line.split(",");
            return new WaterSensor1(split[0],
                    Double.parseDouble(split[1]),
                    Long.parseLong(split[2]));

        });

        Table table = tableEnv.fromDataStream(waterSensorDS1,
                $("id"),
                $("vc"),
                $("ts"),
                $("pt").proctime());

        tableEnv.createTemporaryView("t1", table);

        // join

        tableEnv.sqlQuery("                " +
                "SELECT " +
                "    t1.id, " +
                "    t1.vc, " +
                "    dic.dic_name " +
                "FROM t1 JOIN base_dic FOR SYSTEM_TIME AS OF t1.pt AS dic " +
                "ON t1.id=dic.dic_code"
        ).execute().print();


    }
}
