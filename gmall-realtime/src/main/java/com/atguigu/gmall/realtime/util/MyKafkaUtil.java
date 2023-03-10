package com.atguigu.gmall.realtime.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

public class MyKafkaUtil {
    private static final String KAFKA_SERVER = "10.20.1.231:9092";
    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic, String groupId){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        return new FlinkKafkaConsumer<String>(
                topic,
                new KafkaDeserializationSchema<String>(){

                    @Override
                    public TypeInformation<String> getProducedType() {
                        //return null;
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                       // return null;
                        if (record == null || record.value() == null){
                            return null;
                        } else {
                            return new String(record.value());
                        }
                    }
                },  // 这里不用SimpleStringSchema, deserialize里面的String传参可能为null。
                properties);

    }

}