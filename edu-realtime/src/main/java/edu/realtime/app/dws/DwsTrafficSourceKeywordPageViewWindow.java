package edu.realtime.app.dws;

import edu.realtime.bean.KeywordBean;
import edu.realtime.app.func.KeywordUDTF;
import edu.realtime.util.ClickHouseUtil;
import edu.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow {
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

        // TODO 3.读取pge_log数据
        /**
         * page>>>>>>
         * {"common":{"sc":"1","ar":"31","uid":"23","os":"iOS 12.4.1","ch":"Appstore","is_new":"1","md":"iPhone 8","mid":"mid_248","vc":"v2.1.134","ba":"iPhone","sid":"b382ab80-419c-4cfe-a516-7ae86855d0ca"},
         * "page":{"page_id":"course_detail","item":"224","during_time":9069,"item_type":"course_id","last_page_id":"course_list"},
         * "ts":1662174426227}
         */
        String groupID = "dws_traffic_source_keyword_page_view_window";
        String topicName = "dwd_traffic_page_log";
        tableEnv.executeSql("create table page_log(\n" +
                "  `common`  MAP<String,String>,\n" +
                "  `page`  MAP<String,String>,\n" +
                "  `ts`   bigint,\n" +
                "  rt AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "  WATERMARK FOR rt AS rt - INTERVAL '2' SECOND \n" +
                ")" + KafkaUtil.getKafkaDDL(topicName, groupID));

        // TODO 4.过滤转换数据格式
        //"item_type" = "keyword" && laet_page_id = search  && item不为空
        Table filterTable = tableEnv.sqlQuery("select \n" +
                "  `page`['item'] keyword , \n" +
                "  `rt` \n" +
                "from page_log\n" +
                "where `page`['item_type'] = 'keyword' \n" +
                "and `page`['item'] is not null");
        tableEnv.createTemporaryView("filter_table", filterTable);
        // TODO 5.使用自定义UDTF函数
        //面向官网编程Application Development -> Table API & SQL -> Functions -> user-defined function -> 右侧table Functions
        /**
         * 1.自定义函数
         * 2.注册函数
         * 3.使用函数
         */
        tableEnv.createTemporarySystemFunction("analyze", KeywordUDTF.class);
        Table keywordTable = tableEnv.sqlQuery("select \n" +
                "  keyword,\n" +
                "  word,\n" +
                "  rt \n" +
                "from filter_table,\n" +
                " LATERAL TABLE(analyze(keyword))");
        tableEnv.createTemporaryView("keyword_table", keywordTable);

        // TODO 6.开窗
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "  word keyword,\n" +
                "  count(*) keyword_count,\n" +
                "  DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "  DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "  UNIX_TIMESTAMP()*1000 ts \n" +
                "from keyword_table \n" +
                "group by word,\n" +
                "TUMBLE(rt, INTERVAL '10' SECOND)");

        // TODO 7.写出到clickhouse，需要先将表转为流(需要注意字段类型和字段名必须完全一致)
        DataStream<KeywordBean> beanStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);
        beanStream.print(">>>>>>");

        beanStream.addSink(ClickHouseUtil.getClickHouseSinkFunc("insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?)"));

        env.execute(groupID);
    }
}
