package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import com.alibaba.fastjson.JSON;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

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

    /**
     * value: {"database":"gmall","table":"base_trademark","type":"insert","ts":1678331684,"xid":144025,"commit":true,"data":{"id":12,"tm_name":"yiutto","logo_url":"/static/yt.jpg"}}
     * @param value The stream element.
     * @param ctx A {@link ReadOnlyContext} that allows querying the timestamp of the element,
     *     querying the current processing/event time and updating the broadcast state. The context
     *     is only valid during the invocation of this method, do not store it.
     * @param out The collector to emit resulting elements to
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        // TODO 1.获取广播的配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String table = value.getString("table");
        TableProcess tableProcess = broadcastState.get(table);  // null?

        if (tableProcess != null) {
            // TODO 2.过滤字段filterColumn
            filterColumn(value.getJSONObject("data"), tableProcess.getSinkColumns());

            // TODO 3.补充SinkTable字段，并写出到流中
            value.put("sinkTable", tableProcess.getSinkTable());
            out.collect(value);

        } else { // 关联的不是自己需要的维度表
            System.out.println("找不到对应的key：" + table);
        }

    }

    /**
     * 过滤字段
     * @param data ：{"id":12,"tm_name":"yiutto","logo_url":"/static/yt.jpg"}
     * @param sinkColumns "id,name"
     */
    private void filterColumn(JSONObject data, String sinkColumns) {

        // 切分sinkColumns
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);
        /**
        // 这里用entrySet而不用keySet是为了方便删除键值对
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while(iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            //  data: {"id":xx, "name":xxx, "tm_name":xxx}
            // sinkColumns: "id,tm_name"
            if (!columnList.contains(next.getKey())){
                iterator.remove();

            }
        }
        **/

        // 这里用entrySet而不用keySet是为了方便删除键值对
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        //  data: {"id":xx, "name":xxx, "tm_name":xxx}
        // sinkColumns: "id,tm_name"
        entries.removeIf(next -> !columnList.contains(next.getKey()));


    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
