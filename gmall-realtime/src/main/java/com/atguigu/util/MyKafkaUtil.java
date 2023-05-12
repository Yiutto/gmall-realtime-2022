package com.atguigu.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class MyKafkaUtil {
    private static final String KAFKA_SERVER = "10.20.1.231:9092";

    /**
     * 获取flink需要的kafka消费者相关信息
     * @param topic
     * @param groupId
     * @return
     */
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

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(KAFKA_SERVER, topic, new SimpleStringSchema());
    }

    /**
     * 获取flink需要的kafka生产者相关信息
     * @param topic
     * @param defaultTopic
     * @return
     */
    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic, String defaultTopic) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);

        return new FlinkKafkaProducer<String>(defaultTopic, new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                if (element == null){
                    return new ProducerRecord<>(topic, "".getBytes());
                }
                return new ProducerRecord<>(topic, element.getBytes());
            }
        }, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    /**
     * kafka-Source DDL相关信息
     * @param topic  数据源主题
     * @param groupId 消费者组
     * @return 拼接好的kafka数据源DDL语句
     */
    public static String getKafkaDDL(String topic, String groupId) {
        return "with ('connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                "  'properties.group.id' = '" + groupId + "', " +
                "  'scan.startup.mode' = 'group-offsets', " +
                "  'format' = 'json' " +
                ")";
    }

    /**
     * Kafka-Sink DDL 语句
     * @param topic 输出到kafka的目标主题
     * @return 拼接好的kafkaSinkDDL语句
     */
    public static String getKafkaSinkDDL(String topic) {
        return "with ('connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                "  'format' = 'json' " +
                ")";
    }

    /**
     * 获取topic_db 主题的 flink-sql连接kafka相关建表语句
     * @param groupId
     * @return 拼接好的kafka数据源DDL语句
     */
    public static String getTopicDb(String groupId) {
        return "        create table topic_db (  " +
                "    `database` string,  " +
                "    `table` string,  " +
                "    `type` string,  " +
                "    `data` map<string, string>,  " +
                "    `old` map<string, string>,  " +
                "    `pt` as proctime()  " +
                " ) " + getKafkaDDL("topic_db", groupId);
    }

    /**
     * Upsert Kafka Sink DDL 语句
     * @param topic 输出到kafka的目标主题
     * @return 拼接好的Upsert Kafka Sink DDL
     */
    public static String getUpsertKafkaDDL(String topic) {
        return "with ('connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }

}
