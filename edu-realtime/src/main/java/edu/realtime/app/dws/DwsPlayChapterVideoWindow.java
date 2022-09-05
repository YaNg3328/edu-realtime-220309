package edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import edu.realtime.app.func.DimAsyncFunction;
import edu.realtime.bean.PlayChepterVideoBean;
import edu.realtime.util.ClickHouseUtil;
import edu.realtime.util.DateFormatUtil;
import edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DwsPlayChapterVideoWindow {
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

        // 获取原始数据
//        {"uid":"816","play_sec":30,"position_sec":150,"video_id":"4928","sid":"543c2bc8-7b6a-4871-8e37-25941fae2d04"}

        DataStreamSource<String> kafkaStream = env.fromSource(KafkaUtil.getKafkaSource("dwd_play_chapter_video", "dws_play_chapter_window", OffsetsInitializer.earliest()), WatermarkStrategy.noWatermarks(), "dws_play_chapter_window");

        //转换为json
        SingleOutputStreamOperator<JSONObject> jsonStream = kafkaStream.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> watermarks = jsonStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //聚合，统计每个sid下，每个视频播放的总时长
        KeyedStream<JSONObject, String> keyedStream = watermarks.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("uid") + value.getString("video_id") + value.getString("sid");

            }
        });
        //聚合统计播放时长
        SingleOutputStreamOperator<JSONObject> reduceStream = keyedStream.reduce(new ReduceFunction<JSONObject>() {
            @Override
            public JSONObject reduce(JSONObject value1, JSONObject value2) throws Exception {
                int sec = value1.getInteger("play_sec") + value2.getIntValue("play_sec");
                value1.replace("play_sec", sec);
                return value1;
            }
        });
        //将播放总时长最大的数据作为最终数据
        SingleOutputStreamOperator<JSONObject> processStream = reduceStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("uid") + value.getString("video_id") + value.getString("sid");

            }
        }).process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            ValueState<JSONObject> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> lastTs = new ValueStateDescriptor<JSONObject>("lastTs", JSONObject.class);
                this.state = getRuntimeContext().getState(lastTs);

            }


            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject obj = state.value();
                if (obj == null) {
                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
                    state.update(value);
                } else {
                    if (obj.getString("play_sec").compareTo(value.getString("play_sec")) < 0) {
                        state.update(value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                if (state.value() != null) {
                    out.collect(state.value());
                }
                state.clear();
            }
        });


        //转换为javabean
        SingleOutputStreamOperator<PlayChepterVideoBean> beanStream = processStream.map(new MapFunction<JSONObject, PlayChepterVideoBean>() {
            @Override
            public PlayChepterVideoBean map(JSONObject value) throws Exception {
                return PlayChepterVideoBean.builder()
                        .videoId(value.getString("video_id"))
                        .totalSec(value.getLong("play_sec"))
                        .playTimesCt(1L)
                        .userSet(new HashSet<>(Collections.singleton(value.getString("uid"))))
                        .ts(value.getLong("ts"))
                        .build();
            }
        });



        //分组开窗聚合
        SingleOutputStreamOperator<PlayChepterVideoBean> reduce = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<PlayChepterVideoBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<PlayChepterVideoBean>() {
            @Override
            public long extractTimestamp(PlayChepterVideoBean element, long recordTimestamp) {
                return element.getTs();
            }
        })).keyBy(PlayChepterVideoBean::getVideoId).window(TumblingEventTimeWindows.of(Time.seconds(10L))).reduce(new ReduceFunction<PlayChepterVideoBean>() {
            @Override
            public PlayChepterVideoBean reduce(PlayChepterVideoBean value1, PlayChepterVideoBean value2) throws Exception {
                value1.setTotalSec(value1.getTotalSec() + value2.getTotalSec());
                value1.setPlayTimesCt(value1.getPlayTimesCt() + value2.getPlayTimesCt());
                value1.getUserSet().addAll(value2.getUserSet());
                return value1;
            }
        }, new WindowFunction<PlayChepterVideoBean, PlayChepterVideoBean, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<PlayChepterVideoBean> input, Collector<PlayChepterVideoBean> out) throws Exception {
                PlayChepterVideoBean bean = input.iterator().next();
                bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                bean.setWatchUidCt((long) bean.getUserSet().size());
                bean.setAvgPlaySec(bean.getTotalSec()/bean.getWatchUidCt());
                bean.setTs(System.currentTimeMillis());
                out.collect(bean);
            }
        });

        //关联维度字段
        SingleOutputStreamOperator<PlayChepterVideoBean> joinVideoInfo = AsyncDataStream.unorderedWait(reduce, new DimAsyncFunction<PlayChepterVideoBean>("DIM_VIDEO_INFO") {
            @Override
            public String getKey(PlayChepterVideoBean input) {
                return input.getVideoId();
            }

            @Override
            public void join(PlayChepterVideoBean input, JSONObject jsonObject) {
                input.setChapterId(jsonObject.getString("chapterId"));
            }
        }, 5 * 60L, TimeUnit.SECONDS);
//        joinVideoInfo.print();

        SingleOutputStreamOperator<PlayChepterVideoBean> resultStream = AsyncDataStream.unorderedWait(joinVideoInfo, new DimAsyncFunction<PlayChepterVideoBean>("DIM_CHAPTER_INFO") {
            @Override
            public String getKey(PlayChepterVideoBean input) {
                return input.getChapterId();
            }

            @Override
            public void join(PlayChepterVideoBean input, JSONObject jsonObject) {
                input.setChapterName(jsonObject.getString("chapterName"));
            }
        }, 5 * 60L, TimeUnit.SECONDS);
        resultStream.print();
resultStream.addSink(ClickHouseUtil.getClickHouseSinkFunc("insert into dws_play_chapter_window values(?,?,?,?,?,?,?,?,?,?)"));



        //写出到clickhouse


        env.execute();
    }
}
