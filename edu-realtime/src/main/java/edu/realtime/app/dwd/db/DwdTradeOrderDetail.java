package edu.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import edu.realtime.util.KafkaUtil;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdTradeOrderDetail {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "300 s");
        // TODO 2.设置检查点和状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(3 * 60 * 1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME","atguigu");
        env.setStateBackend(new HashMapStateBackend());
         */
        // TODO 3.读取kafka的数据topic_db，topic_log的数据
        tableEnv.executeSql("create table page_log(\n" +
                "  `common`  MAP<String,String>,\n" +
                "  `page`  MAP<String,String>,\n" +
                "  `ts`   bigint,\n" +
                "  rt AS TO_TIMESTAMP_LTZ(ts, 3)\n" +
//                "  WATERMARK FOR rt AS rt - INTERVAL '2' SECOND \n" +
                ")" + KafkaUtil.getKafkaDDL("dwd_traffic_page_log", "dwd_trade_order_detail"));
        tableEnv.executeSql("create table topic_db(" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`type` string,\n" +
                "`data` map<string, string>,\n" +
                "`old` map<string, string>,\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_test_exam_question"));
        //TODO 4.分别从topic_log表和topic_db表过滤出order_detail,order_info和用户下单页面日志
        Table orderDetail = tableEnv.sqlQuery("select \n" +
                "`data`['id'] id,\n" +
                "`data`['course_id'] course_id,\n" +
                "`data`['course_name'] course_name,\n" +
                "`data`['order_id'] order_id,\n" +
                "`data`['user_id'] user_id,\n" +
                "`data`['origin_amount'] origin_amount,\n" +
                "`data`['coupon_reduce'] coupon_reduce,\n" +
                "`data`['final_amount'] final_amount,\n" +
                "`data`['create_time'] create_time,\n" +
                "`data`['update_time'] update_time,\n" +
                "ts \n" +
                "from\n" +
                "`topic_db`\n" +
                "where\n" +
                "`table` = 'order_detail' and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail", orderDetail);
//        orderDetail.execute().print();


        Table orderInfo = tableEnv.sqlQuery("select\n" +
                "`data`['id'] id,\n" +
                "`data`['user_id'] user_id,\n" +
                "`data`['origin_amount'] origin_amount,\n" +
                "`data`['coupon_reduce'] coupon_reduce,\n" +
                "`data`['final_amount'] final_amount,\n" +
                "`data`['order_status'] order_status,\n" +
                "`data`['out_trade_no'] out_trade_no,\n" +
                "`data`['trade_body'] trade_body,\n" +
                "`data`['session_id'] session_id,\n" +
                "`data`['province_id'] province_id,\n" +
                "`data`['create_time'] create_time,\n" +
                "`data`['expire_time'] expire_time,\n" +
                "`data`['update_time'] update_time,\n" +
                "ts \n" +
                "from\n" +
                "topic_db\n" +
                "where\n" +
                "`table` = 'order_info' and `type` = 'insert'");
        tableEnv.createTemporaryView("order_info", orderInfo);
//        orderInfo.execute().print();

        Table table_log = tableEnv.sqlQuery("select\n" +
                "`common`['sc'] sc,\n" +
                "`common`['sid'] sid,\n" +
                "`common`['uid'] uid\n" +
                "from\n" +
                "page_log\n" +
                "where\n" +
                "`page`['last_page_id'] = 'order'"
        );

        tableEnv.createTemporaryView("log", table_log);
//        table_log.execute().print();

        Table resultTable = tableEnv.sqlQuery("select\n" +
                "od.id id,\n" +
                "od.course_id course_id,\n" +
                "od.course_name course_name,\n" +
                "od.order_id order_id,\n" +
                "od.user_id user_id,\n" +
                "od.origin_amount origin_amount,\n" +
                "od.coupon_reduce coupon_reduce,\n" +
                "od.final_amount final_amount,\n" +
                "oi.order_status order_status,\n" +
                "oi.session_id session_id,\n" +
                "oi.province_id province_id,\n" +
                "oi.ts ts,\n" +
                "log.sc sc\n" +
                "from\n" +
                "order_detail od\n" +
                "join\n" +
                "order_info oi\n" +
                "on od.order_id = oi.id\n" +
                "join\n" +
                "`log`\n" +
                "on oi.session_id = log.sid");
//        resultTable.execute().print();
        tableEnv.createTemporaryView("result_table", resultTable);

        tableEnv.executeSql("create table dwd_trade_order_detail(\n" +
                "id string,\n" +
                "course_id string,\n" +
                "course_name string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "origin_amount string,\n" +
                "coupon_reduce string,\n" +
                "final_amount string,\n" +
                "order_status string,\n" +
                "session_id string,\n" +
                "province_id string,\n" +
                "ts string,\n" +
                "sc string\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_trade_order_detail"));
        tableEnv.executeSql("insert into dwd_trade_order_detail select * from result_table");

    }
}
