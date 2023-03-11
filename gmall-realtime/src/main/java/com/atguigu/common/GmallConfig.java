package com.atguigu.common;

public class GmallConfig {
    // phoenix 库名  create schema GMALL_REALTIME;
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";

    // phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:node01,node02,node03:2181";
}
