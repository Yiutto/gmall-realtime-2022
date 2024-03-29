package com.atguigu.common;

public class GmallConfig {
    // phoenix 库名  create schema GMALL_REALTIME;
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";

    // phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:node01,node02,node03:2181";

    // ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://10.20.1.231:8123/gmall";
}
