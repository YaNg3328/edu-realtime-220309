package edu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import edu.realtime.app.func.DimAsyncFunction;
import edu.realtime.bean.CourseCategorySubjectCourseOrderBean;
import edu.realtime.util.ClickHouseUtil;
import edu.realtime.util.DateFormatUtil;
import edu.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DwsCourseCategorySubjectCourseOrderWindow {
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
        //获取dwd层订单表的数据
        DataStreamSource<String> kafkaStream = env.addSource(KafkaUtil.getKafkaConsumer("dwd_trade_order_detail",
                "dws_course_category_subject_course_order_window"));

        //转换数据结构为javabean
        SingleOutputStreamOperator<CourseCategorySubjectCourseOrderBean> beanStream = kafkaStream.map(str -> {
            JSONObject jsonObject = JSON.parseObject(str);
            Double finalAmount = jsonObject.getDouble("final_amount");
            String courseId = jsonObject.getString("course_id");
            String userId = jsonObject.getString("user_id");
            Long ts = jsonObject.getLong("ts");

            return CourseCategorySubjectCourseOrderBean.builder()
                    .amount(finalAmount)
                    .courseId(courseId)
                    .userIdSet(new HashSet<String>(Collections.singleton(userId)))
                    .orderCt(1L)
                    .ts(ts)
                    .build();
        });

        //关联维度字段
        SingleOutputStreamOperator<CourseCategorySubjectCourseOrderBean> courseInfoJoinStream = AsyncDataStream.unorderedWait(beanStream, new DimAsyncFunction<CourseCategorySubjectCourseOrderBean>(
                "DIM_COURSE_INFO") {
            @Override
            public String getKey(CourseCategorySubjectCourseOrderBean input) {
                return input.getCourseId();
            }

            @Override
            public void join(CourseCategorySubjectCourseOrderBean input, JSONObject obj) {
                input.setCourse(obj.getString("courseName"));
                input.setSubjectId(obj.getString("subjectId"));
            }
        }, 60 * 5L, TimeUnit.SECONDS);

        //设置水位线

        WindowedStream<CourseCategorySubjectCourseOrderBean, String, TimeWindow> windowWindowedStream =
                courseInfoJoinStream.assignTimestampsAndWatermarks(WatermarkStrategy.<CourseCategorySubjectCourseOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<CourseCategorySubjectCourseOrderBean>() {
                    @Override
                    public long extractTimestamp(CourseCategorySubjectCourseOrderBean element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                })).keyBy(bean -> bean.getCourseId()).window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        SingleOutputStreamOperator<CourseCategorySubjectCourseOrderBean> reduceStream = windowWindowedStream.reduce(new ReduceFunction<CourseCategorySubjectCourseOrderBean>() {
                                                                                                                           @Override
                                                                                                                           public CourseCategorySubjectCourseOrderBean reduce(CourseCategorySubjectCourseOrderBean value1, CourseCategorySubjectCourseOrderBean value2) throws Exception {
                                                                                                                               value1.setAmount(value1.getAmount() + value2.getAmount());
                                                                                                                               value1.setOrderCt(value1.getOrderCt() + value2.getOrderCt());
                                                                                                                               value1.getUserIdSet().addAll(value2.getUserIdSet());
                                                                                                                               System.out.println(value1.getPersonCt());
                                                                                                                               return value1;
                                                                                                                           }
                                                                                                                       }
                , new WindowFunction<CourseCategorySubjectCourseOrderBean, CourseCategorySubjectCourseOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<CourseCategorySubjectCourseOrderBean> input, Collector<CourseCategorySubjectCourseOrderBean> out) throws Exception {
                        CourseCategorySubjectCourseOrderBean bean = input.iterator().next();
                        bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        bean.setPersonCt((long) bean.getUserIdSet().size());
                        bean.setTs(System.currentTimeMillis());
                        out.collect(bean);
                    }
                }
        );

        //关联维度字段
        SingleOutputStreamOperator<CourseCategorySubjectCourseOrderBean> subjectInfoJoinStream = AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<CourseCategorySubjectCourseOrderBean>(
                "DIM_BASE_SUBJECT_INFO") {
            @Override
            public String getKey(CourseCategorySubjectCourseOrderBean input) {
                return input.getSubjectId();
            }

            @Override
            public void join(CourseCategorySubjectCourseOrderBean input, JSONObject obj) {
                input.setCategoryId(obj.getString("categoryId"));
                input.setSubject(obj.getString("subjectName"));

            }
        }, 60 * 5L, TimeUnit.SECONDS);

        SingleOutputStreamOperator<CourseCategorySubjectCourseOrderBean> categoryJoinStream = AsyncDataStream.unorderedWait(subjectInfoJoinStream, new DimAsyncFunction<CourseCategorySubjectCourseOrderBean>(
                "DIM_BASE_CATEGORY_INFO") {
            @Override
            public String getKey(CourseCategorySubjectCourseOrderBean input) {
                return input.getCategoryId();
            }

            @Override
            public void join(CourseCategorySubjectCourseOrderBean input, JSONObject obj) {
                input.setCategory(obj.getString("categoryName"));


            }
        }, 60 * 5L, TimeUnit.SECONDS);

        categoryJoinStream.addSink(ClickHouseUtil.getClickHouseSinkFunc("insert into " +
                "dws_course_category_subject_course_order_window values(?,?,?,?,?,?,?,?,?,?,?,?)"));
        env.execute();
    }

}
