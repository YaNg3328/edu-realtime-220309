package edu.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DwdPlayChapterVideo {
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
        //获取topic_db的数据视频播放的数据
        DataStreamSource<String> kafkaDBStream = env.fromSource(KafkaUtil.getKafkaSource("topic_db", "dwd_play_chapter_video", OffsetsInitializer.earliest()), WatermarkStrategy.noWatermarks(), "dwd_play_chapter_video");
        DataStreamSource<String> kafkaLogStream = env.fromSource(KafkaUtil.getKafkaSource("topic_log", "dwd_play_chapter_video", OffsetsInitializer.earliest()), WatermarkStrategy.noWatermarks(), "dwd_play_chapter_video");
//        {"appVideo":{"play_sec":19,"position_sec":210,"video_id":"4552"},"
//        common":{"ar":"20","ba":"Huawei","ch":"oppo","is_new":"0","md":"Huawei P30","mid":"mid_180","os":"Android 11.0","sc":"4","sid":"df5ad64b-95c8-41e8-a4b3-bba994eb74df","uid":"39","vc":"v2.1.132"}
//        ,"ts":1662280870964}
        SingleOutputStreamOperator<JSONObject> playVideoStream = kafkaLogStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject appVideo = jsonObject.getJSONObject("appVideo");
                if (appVideo != null) {

                    JSONObject common = jsonObject.getJSONObject("common");
                    String sid = common.getString("sid");
                    String uid = common.getString("uid");

                    appVideo.put("uid", uid);
                    appVideo.put("sid", sid);
                    appVideo.put("ts",jsonObject.getString("ts"));
                    out.collect(appVideo);
                }
            }
        });
        //写出到kafka的主题中
        playVideoStream.map(JSONAware::toJSONString).addSink(KafkaUtil.getKafkaProducer("dwd_play_chapter_video"));


        env.execute();

    }
}
