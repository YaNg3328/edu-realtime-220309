package edu.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 用户注册
 */
public class DwdUserRegister {
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

        // TODO 3.读取topic_db主题的数据
        String topicName = "topic_db";
        String groupID = "dwd_user_register";
        DataStreamSource<String> dbStream = env.addSource(KafkaUtil.getKafkaConsumer(topicName, groupID));
        // TODO 4.过滤加转换数据格式
        /*SingleOutputStreamOperator<JSONObject> jsonObjstream =
                dbStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String table = jsonObject.getString("table");
                String type = jsonObject.getString("type");
                if ("user_info".equals(table) && "insert".equals(type)) {
                    out.collect(jsonObject);
                }
            }
        });*/
        //直接过滤，最后输出string，可直接写到kafka
        SingleOutputStreamOperator<String> filterStream =
                dbStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String table = jsonObject.getString("table");
                String type = jsonObject.getString("type");
                Long ts = jsonObject.getLong("ts");
                if ("user_info".equals(table) && "insert".equals(type)) {
                    JSONObject data = jsonObject.getJSONObject("data");
                    data.remove("birthday");
                    data.remove("login_name");
                    data.remove("user_level");
                    data.remove("phone_num");
                    data.remove("email");
                    data.remove("email");
                    data.put("user_id",data.getString("id"));
                    data.remove("id");
                    data.put("ts",ts);
                    out.collect(data.toJSONString());
                }
            }
        });

        //测试
        filterStream.print(">>>>>>>>");

        // TODO 5.写入到kafka中
        String targetTopic = "dwd_user_register";
        filterStream.addSink(KafkaUtil.getKafkaProducer(targetTopic));

        // TODO 6.执行任务
        env.execute(groupID);
    }
}
