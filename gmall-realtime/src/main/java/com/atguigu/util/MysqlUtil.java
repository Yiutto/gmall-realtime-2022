package com.atguigu.util;

public class MysqlUtil {
    public static String getBaseDicLookUpDDL() {

        return "create table `base_dic`( " +
                "`dic_code` string, " +
                "`dic_name` string, " +
                "`parent_code` string, " +
                "`create_time` timestamp, " +
                "`operate_time` timestamp, " +
                "primary key(`dic_code`) not enforced " +
                ")" + MysqlUtil.mysqlLookUpTableDDL("base_dic");
    }

    public static String mysqlLookUpTableDDL(String tableName) {

        String ddl = "WITH ( " +
                "'connector' = 'jdbc', " +
                "'url' = 'jdbc:mysql://10.20.1.231:3306/gmall', " +
                "'table-name' = '" + tableName + "', " +
                "'lookup.cache.max-rows' = '10', " +
                "'lookup.cache.ttl' = '1 hour', " +
                "'username' = 'bigdata', " +
                "'password' = 'bigdata123', " +
                "'driver' = 'com.mysql.cj.jdbc.Driver' " +
                ")";
        return ddl;
    }
}
