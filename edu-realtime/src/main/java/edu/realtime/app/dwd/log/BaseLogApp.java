package edu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import edu.realtime.util.DateFormatUtil;
import edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseLogApp {
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

        // TODO 3.读取topic_log主题数据
        String topicName = "topic_log";
        String groupID = "base_log_app";
        DataStreamSource<String> logStream = env.addSource(KafkaUtil.getKafkaConsumer(topicName, groupID));

        // TODO 4.过滤加转化数据格式
        //只是为了增加代码的健壮性进行判断，f1已经过滤了json格式
        OutputTag<String> dirtyOutputTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjStream = logStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                    ctx.output(dirtyOutputTag, value);
                }
            }
        });

        // TODO 5.按照mid进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.keyBy(s -> s.getJSONObject(
                "common").getString("mid"));

        // TODO 6.状态编程，新老访客修复
        //格式转换掉map，需要用到状态加上Rich
        SingleOutputStreamOperator<JSONObject> isNewStream = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            ValueState<String> state = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<String>(
                        "first_visit_dt", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String firstVisitDt = state.value();
                String visitDt = DateFormatUtil.toDate(value.getLong("ts"));
                String isNew = value.getJSONObject("common").getString("is_new");
                if ("1".equals(isNew)) {
                    //如果标记为1
                    if (firstVisitDt == null) {
                        //且状态非空，不需要修复，更新状态即可
                        state.update(visitDt);
                    } else if (!firstVisitDt.equals(visitDt)) { // 新老访客每天0点更新，所以当天的不需要更改
                        //状态不为空，则不是新访客，需要修复,不需要更新状态
                        value.getJSONObject("common").put("is_new", "0");
                    }
                } else {
                    //如果标记为0 判断状态是否为空
                    if (firstVisitDt == null) {
                        //将状态更新至前一天即可
                        String yesterday = DateFormatUtil.toDate(value.getLong("ts") - 1000L * 60 * 60 * 24);
                        state.update(yesterday);
                        //输出昨天测试看效果
//                        System.out.println(yesterday);
                    }
                }
                return value;
            }
        });
        // TODO 6.1测试新老访客代码
//        isNewStream.print(">>>>>");

        // TODO 7.1动态分流
        OutputTag<String> startOutputTag = new OutputTag<String>("start") {
        };
        OutputTag<String> errOutputTag = new OutputTag<String>("err") {
        };
        OutputTag<String> actionOutputTag = new OutputTag<String>("action") {
        };
        OutputTag<String> displayOutputTag = new OutputTag<String>("display") {
        };
        OutputTag<String> videoOutputTag = new OutputTag<String>("video") {
        };

        // TODO 7.2获取侧输出流中的数据
        SingleOutputStreamOperator<String> pageStream = isNewStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                //1.先获取错误日志
                String err = value.getString("err");
                if (err != null) {
                    ctx.output(errOutputTag, err);
                }
                value.remove("err");

                JSONObject start = value.getJSONObject("start");
                JSONObject appVideo = value.getJSONObject("appVideo");
                if (start != null) {
                    //2.获取启动日志
                    ctx.output(startOutputTag, value.toJSONString());
                } else if (appVideo != null) {
                    //3.获取播放日志
                    ctx.output(videoOutputTag, value.toJSONString());
                } else {
                    //4.剩下的一定是页面日志
                    JSONObject common = value.getJSONObject("common");
                    JSONObject page = value.getJSONObject("page");
                    Long ts = value.getLong("ts");
                    //4.1打散动作数据，类似于sql的炸裂 + 侧写，最后记得删除
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("page", page);
                            action.put("ts", ts);
                            ctx.output(actionOutputTag, action.toJSONString());
                        }
                    }
                    value.remove("actions");
                    //4.2同理打散曝光
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("page", page);
                            display.put("ts", ts);
                            ctx.output(displayOutputTag, display.toJSONString());
                        }
                    }
                    value.remove("displays");
                    //5.最终将剩余的页面输出
                    out.collect(value.toJSONString());
                }
            }
        });

        DataStream<String> errStream = pageStream.getSideOutput(errOutputTag);
        DataStream<String> startStream = pageStream.getSideOutput(startOutputTag);
        DataStream<String> videoStream = pageStream.getSideOutput(videoOutputTag);
        DataStream<String> actionStream = pageStream.getSideOutput(actionOutputTag);
        DataStream<String> displayStream = pageStream.getSideOutput(displayOutputTag);
        //测试输出
        errStream.print("err>>>>>>>");
        startStream.print("start>>>>>>>");
        videoStream.print("video>>>>>>>");
        actionStream.print("action>>>>>>>");
        displayStream.print("display>>>>>>>");
        pageStream.print("page>>>>>");

        // TODO 8.写入对应的kafka主题
        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String err_topic = "dwd_traffic_err_log";
        String video_topic = "dwd_traffic_video_log";
        String action_topic = "dwd_traffic_action_log";
        String display_topic = "dwd_traffic_display_log";
        pageStream.addSink(KafkaUtil.getKafkaProducer(page_topic));
        pageStream.addSink(KafkaUtil.getKafkaProducer(start_topic));
        pageStream.addSink(KafkaUtil.getKafkaProducer(err_topic));
        pageStream.addSink(KafkaUtil.getKafkaProducer(video_topic));
        pageStream.addSink(KafkaUtil.getKafkaProducer(action_topic));
        pageStream.addSink(KafkaUtil.getKafkaProducer(display_topic));


        // TODO 9.执行任务
        env.execute(groupID);
    }
}
