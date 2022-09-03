package edu.realtime.util;


import edu.realtime.bean.TransientSink;
import edu.realtime.common.EduConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {
    public static  <T> SinkFunction<T> getClickHouseSinkFunc(String sql) {

        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        //todo 1.通过反射获取类模板
                        Class<?> tClass = t.getClass();
                        // todo 2.通过类模板获取对象的所有字段名，注意私有字段需要用getDeclaredFields()
                        Field[] declaredFields = tClass.getDeclaredFields();
                        // TODO  7.自定义注解之后，定义一个标签，用来跳过不写入的字段
                        int offset = 0;
                        //遍历循环取出所有字段
                        for (int i = 0; i < declaredFields.length; i++) {
                            // todo 3.先获取每个字段名称
                            Field declaredField = declaredFields[i];

                            // TODO 8.使用注解跳过判断
                            TransientSink transientSink = declaredField.getAnnotation(TransientSink.class);
                            //使用了注解之后，对应字段获取注解的时候就不为null，则需要跳过，且 标签自增1
                            if (transientSink != null){
                                offset++;
                                continue;
                            }

                            // todo 4.必须设置参数，突破限制 ,下面才能对着对应的值
                            declaredField.setAccessible(true);
                            // todo 5.获取每个字段对应的值  语法：字段名.对象  会报异常，异常的解决方法就是上面设置true，然后直接抛出异常
                            Object value = declaredField.get(t);
                            preparedStatement.setObject(i+1-offset,value);
                        }
                    }
                }
                ,
                JdbcExecutionOptions.builder()
                        .withBatchSize(10)
                        .withBatchIntervalMs(100L)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(EduConfig.CLICKHOUSE_URL)
                        .withDriverName(EduConfig.CLICKHOUSE_DRIVER)
                        .build()
        );
    }
}
