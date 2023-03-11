package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;

import com.atguigu.util.DruidDSUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private DruidDataSource druidDataSource;
    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    /**
     * value: {"database":"gmall","table":"base_trademark","type":"insert","ts":1678331684,"xid":144025,"commit":true
     *          ,"data":{"id":12,"tm_name":"yiutto","logo_url":"/static/yt.jpg"}
     *          ,"sinkTable": "dim_xxx"}
     * @param value The input record.
     * @param context Additional context about the input record.
     * @throws Exception
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        // 获取连接
        DruidPooledConnection connection = druidDataSource.getConnection();

        // 写出数据
        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");
        PhoenixUtil.upsertValues(connection, sinkTable, data);  // 这里不用try catch 报错直接挂了
        // 归还连接
        connection.close();

    }
}
