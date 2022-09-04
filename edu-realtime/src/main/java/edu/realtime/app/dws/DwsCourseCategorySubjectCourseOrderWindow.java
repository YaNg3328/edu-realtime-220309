package edu.realtime.app.dws;

import edu.realtime.util.KafkaUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsCourseCategorySubjectCourseOrderWindow {
    public static void main(String[] args) {
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
        //获取dwd层订单表的数据
        DataStreamSource<String> kafkaStream = env.addSource(KafkaUtil.getKafkaConsumer("dwd_trade_order_detail",
                "dws_course_category_subject_course_order_window"));
        //转换数据结构


    }

}
