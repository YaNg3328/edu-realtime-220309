package edu.realtime.app.dim;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import edu.realtime.app.func.MyBroadcastFunction;
import edu.realtime.app.func.MyPhoenixSink;
import edu.realtime.bean.TableProcess;
import edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author: Abner
 * @Date: Created in 11:35 2022/9/3
 */
public class DimSinkApp {
    public static void main(String[] args) throws Exception {
        // TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2 设置状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */

        // TODO 3 读取kafka的topic_db主题数据
        String topicName = "topic_db";
        String groupID = "dim_sink_app";
        DataStreamSource<String> topicDBStream = env.addSource(KafkaUtil.getKafkaConsumer(topicName, groupID));

        /* TODO 4 改变数据结构并将脏数据写出到侧输出流 */
        OutputTag<String> dirtyOutputTag = new OutputTag<String>("Dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonObjStream = topicDBStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String type = jsonObject.getString("type");
                    if (!"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type)) {
                        out.collect(jsonObject);
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                    ctx.output(dirtyOutputTag, value);
                }
            }
        });

        // 获取脏数据的流
        DataStream<String> dirtyStream = jsonObjStream.getSideOutput(dirtyOutputTag);

        //TODO 5 使用flinkCDC读取配置表数据
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("edu_config")
                .tableList("edu_config.table_process")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        // TODO 6 将配置流转换为广播流和主流进行连接
        // K String(表名)  判断当前表是否为维度表
        // V (后面的数据)   能够完成在phoenix中创建表格的工作
        DataStreamSource<String> tableConfigStream = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(),
                "table_config");

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table_process",
                String.class, TableProcess.class);

        BroadcastStream<String> broadcastStream = tableConfigStream.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjStream.connect(broadcastStream);
        // TODO 7 处理连接流 根据配置流的信息  过滤出主流的维度表内容
        SingleOutputStreamOperator<JSONObject> filterTableStream = connectedStream.process(new MyBroadcastFunction(mapStateDescriptor));

//        filterTableStream.print("filterTable>>>>>>>>");

        // TODO 8 将数据写入到phoenix中
        filterTableStream.addSink(new MyPhoenixSink());

        // TODO 9 执行任务
        env.execute(groupID);
    }
}
