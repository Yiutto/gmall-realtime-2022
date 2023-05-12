package com.atguigu.util;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

public class MyClickHouseUtil {
    // 泛型需前面申明
    public static <T> SinkFunction<T> getSinkFunction(String sql){
        return JdbcSink.<T>sink(sql, new JdbcStatementBuilder<T>() {
            @SneakyThrows
            @Override
            public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                // 使用反射的方式获取t对象中的数据
                Class<?> tClz = t.getClass();

                // 反射说明：aaa属性
                     // 正常调用：Object value = obj.getAaa()
                     // 反射调用：Object value = aaa.get(obj)
                // 反射说明: ccc(int a, String b)
                     // 正常调用：Object value = obj.ccc(a, b)
                     // 反射调用：Object value = ccc.invoke(obj, a, b)

//                Method[] methods = tClz.getMethods();
//                for (int i = 0; i < methods.length; i++) {
//                    Method method = methods[i];
//                    method.invoke(t);
//
//                }

                // 获取并遍历属性
                Field[] declaredFields = tClz.getDeclaredFields();
                int offset = 0;
                for (int i = 0; i < declaredFields.length; i++) {
                    // 读取单个属性
                    Field field = declaredFields[i];
                    field.setAccessible(true);  // 私有拿不到，需要设置

                    // 尝试获取字段上的自定义注解
                    TransientSink transientSink = field.getAnnotation(TransientSink.class);
                    if (transientSink != null) {
                        offset++;
                        continue;
                    }
                    // 获取属性值
                    Object value = field.get(t);

                    // 给占位符设置
                    preparedStatement.setObject(i + 1 - offset, value);

                }


            }
        },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL).build());

    }
}
