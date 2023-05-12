package com.atguigu.app.func;

import com.atguigu.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String str) {
//        for (String s : str.split(" ")) {
//            // use collect(...) to emit a row
//            collect(Row.of(s, s.length()));
//        }
        List<String> list = null;
        try {
            list = KeywordUtil.splitKeyword(str);
            for (String s : list) {
                collect(Row.of(s));
            }

        } catch (IOException e) {
//            throw new RuntimeException(e);
            // 切词切不出来
            collect(Row.of(str));
        }

    }
}