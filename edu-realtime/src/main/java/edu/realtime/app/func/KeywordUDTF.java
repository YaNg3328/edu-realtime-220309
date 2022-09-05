package edu.realtime.app.func;


import edu.realtime.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * 官网地址左侧找到table Functions
 * 1.使用注解，可以用别名以及确定数据类型
 * 2.最后写出的时候用collect方法
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public  class KeywordUDTF extends TableFunction<Row> {

    public void eval(String str) {
        List<String> analyze = KeywordUtil.analyze(str);
        for (String s : analyze) {
            collect(Row.of(s));
        }
    }
}
