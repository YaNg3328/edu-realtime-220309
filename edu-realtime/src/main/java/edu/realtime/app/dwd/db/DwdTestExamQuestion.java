package edu.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdTestExamQuestion {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // TODO 2.设置检查点和状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(3 * 60 * 1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME","atguigu");
        env.setStateBackend(new HashMapStateBackend());
         */

        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "5 s");
        //TODO 3.获取kafka对应主题的数据,转换为flink表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`type` string,\n" +
                "`data` map<string, string>,\n" +
                "`old` map<string, string>,\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_test_exam_question"));

        //过滤出test_exam_question表
        Table testExamQuestion = tableEnv.sqlQuery("select\n" +
                "`data`['score'] score,\n" +
                "`data`['deleted'] deleted,\n" +
                "`data`['answer'] answer,\n" +
                "`data`['create_time'] create_time,\n" +
                "`data`['user_id'] user_id,\n" +
                "`data`['id'] id,\n" +
                "`data`['paper_id'] paper_id,\n" +
                "`data`['question_id'] question_id,\n" +
                "`data`['is_correct'] is_correct,\n" +
                "`data`['exam_id'] exam_id,\n" +
                "ts\n" +
                "from\n" +
                "topic_db\n" +
                "where\n" +
                "\t`table` = 'test_exam_question' and `type` = 'insert'");
//        testExamQuestion.execute().print();
        tableEnv.createTemporaryView("test_exam_question", testExamQuestion);

        //过滤出test_exam表的数据
        Table testExam = tableEnv.sqlQuery("select\n" +
                "`data`['id'] id,\n" +
                "`data`['paper_id'] paper_id,\n" +
                "`data`['user_id'] user_id,\n" +
                "`data`['score'] score,\n" +
                "`data`['duration_sec'] duration_sec,\n" +
                "`data`['create_time'] create_time,\n" +
                "`data`['submit_time'] submit_time,\n" +
                "`data`['update_time'] update_time,\n" +
                "`data`['deleted'] deleted,\n" +
                "ts\n" +
                "from\n" +
                "topic_db\n" +
                "where\n" +
                "\t`table` = 'test_exam' and `type` = 'insert'");
        tableEnv.createTemporaryView("test_exam",testExam);
//        testExam.execute().print();
        //关联两张表作为考试试题明细表
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "teq.id,\n" +
                "teq.score,\n" +
                "teq.answer,\n" +
                "teq.user_id,\n" +
                "teq.paper_id,\n" +
                "teq.question_id,\n" +
                "teq.is_correct,\n" +
                "teq.exam_id,\n" +
                "te.duration_sec,\n" +
                "te.ts\n" +
                "from\n" +
                "test_exam_question teq\n" +
                "join\n" +
                "test_exam te\n" +
                "on\n" +
                "teq.exam_id = te.id");
//        resultTable.execute().print();
        tableEnv.createTemporaryView("result_table",resultTable);
        tableEnv.executeSql("create table dwd_test_exam_question(\n" +
                "id string,\n" +
                "score string,\n" +
                "answer string,\n" +
                "user_id string,\n" +
                "paper_id string,\n" +
                "question_id string,\n" +
                "is_correct string,\n" +
                "exam_id string,\n" +
                "duration_sec string,\n" +
                "ts string\n" +
                ")"+KafkaUtil.getKafkaSinkDDL("dwd_test_exam_question"));

        tableEnv.executeSql("insert into dwd_test_exam_question select * from result_table");

    }
}
