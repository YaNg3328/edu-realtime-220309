package edu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.设置检查点和状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(3 * 60 * 1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME","atguigu");
        env.setStateBackend(new HashMapStateBackend());
         */

        // TODO 3.读取page_log主题的数据
        String topicName = "dwd_traffic_page_log";
        String groupID = "dwd_traffic_user_jump_detail";
        DataStreamSource<String> pageStream = env.addSource(KafkaUtil.getKafkaConsumer(topicName, groupID));
        // TODO 4.转化数据格式
        SingleOutputStreamOperator<JSONObject> jsonOnjStream = pageStream.map(JSON::parseObject);
        // TODO 5.添加水位线 + 6.按照设备mid进行分组
        KeyedStream<JSONObject, String> keyedStream =
                jsonOnjStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                })).keyBy(s -> s.getJSONObject("common").getString("mid"));

        // TODO 7.定义规则
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("begin")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value, Context<JSONObject> ctx) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null;
                    }
                })
                .<JSONObject>next("next")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value, Context<JSONObject> ctx) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null;
                    }
                })
                .within(Time.seconds(15L));

        // TODO 8.CEP使用规则
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // TODO 9.拆分出超时流
        OutputTag<JSONObject> timeoutOutputTag = new OutputTag<JSONObject>("timeout") {
        };
        // todo 9.1获取侧输出超时流数据
        //flatSelect与select的区别可以对比map与flatmap的区别
        //方法传三个参数  侧输出 + 处理超时数据 + 处理主流数据
        SingleOutputStreamOperator<JSONObject> flatSelectStream =
                patternStream.flatSelect(timeoutOutputTag, new PatternFlatTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp,
                                Collector<JSONObject> out) throws Exception {
                //获取超时数据写出到侧输出流中
                List<JSONObject> begin = pattern.get("begin");
                JSONObject jsonObject = begin.get(0);
                out.collect(jsonObject);
            }
        }, new PatternFlatSelectFunction<JSONObject, JSONObject>() {
            @Override
            public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<JSONObject> out) throws Exception {
                //获取正常跳出数据
                List<JSONObject> begin = pattern.get("begin");
                JSONObject jsonObject = begin.get(0);
                out.collect(jsonObject);

            }
        });

        DataStream<JSONObject> timeoutStream = flatSelectStream.getSideOutput(timeoutOutputTag);

        timeoutStream.print("timeout>>>");
        flatSelectStream.print("jump>>>");
        // TODO 10.合并两个流
        DataStream<JSONObject> jumpStream = flatSelectStream.union(timeoutStream);

        // TODO 11.写出到新的kafka主题
        String targetTopic = "dwd_traffic_user_jump_detail";
        //注意先转string，再写出
        jumpStream.map(JSONAware::toJSONString)
                .addSink(KafkaUtil.getKafkaProducer(targetTopic));

        // TODO 12.执行任务
        env.execute(groupID);
    }
}
