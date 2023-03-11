package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import com.alibaba.fastjson.JSON;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection connection;

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        // {"before":null,"after":{"source_table":"base_trademark","sink_table":"dim_base_trademark","sink_columns":"id,tm_name","sink_pk":"id","sink_extend":null},
        // "source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1678501416709,"snapshot":"false","db":"gmall_config",
        // "sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},
        // "op":"r","ts_ms":1678501416709,"transaction":null}

        // TODO 1.获取并解析数据，方便主流操作
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        // TODO 2.校验表是否存在，如果不存在需要在phoenix中建表 checkTable
        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkExtend());
        // TODO 3.写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(), tableProcess);

    }

    /**
     * 校验并建表 create table if not exists db.tb( aa varchar primary key, bb varchar) xxx
     * @param sinkTable     Phoenix表名
     * @param sinkColumns   Phoenix表字段
     * @param sinkPk        Phoenix表主键
     * @param sinkExtend    Phoenix表拓展字段
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;
        try {
            // 处理特殊字段
            if (sinkPk == null || "".equals(sinkPk)){
                sinkPk = "id";
            }
            if (sinkExtend == null){
                sinkExtend = "";
            }
            // 拼接sql: create table if not exists db.tb(id varchar primary key, bb varchar) xxx
            StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++){
                // 取出字段
                String column = columns[i];

                // 判断是否为主键
                if (sinkPk.equals(column)){
                    createTableSql.append(column).append(" varchar primary key");
                } else {
                    createTableSql.append(column).append(" varchar");
                }
                // 判断是否为最后一个字段
                if (i < columns.length - 1){
                    createTableSql.append(",");
                }

            }
            createTableSql.append(")").append(sinkExtend);

            // 编译sql
            System.out.println("建表语句为：" + createTableSql);
            preparedStatement = connection.prepareStatement(createTableSql.toString());

            // 执行SQL，建表
            preparedStatement.execute();

        } catch (SQLException e) {
            // 希望建表失败直接抛出异常报错
            throw new RuntimeException("建表失败：" + sinkTable);
        } finally {
            if (preparedStatement != null) {
                try {
                    // 释放资源
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        // TODO 1.获取广播的配置数据
        // TODO 2.过滤字段filterColumn
        // TODO 3.补充SinkTable字段

    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
