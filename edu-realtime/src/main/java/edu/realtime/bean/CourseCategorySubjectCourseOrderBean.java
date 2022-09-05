package edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CourseCategorySubjectCourseOrderBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 类别
    String categoryId;
    String category;
    // 科目
    String subjectId;
    String subject;
    //课程
    String courseId;
    String course;


    @TransientSink
    Set<String> userIdSet;
    //下单金额
    Double amount;
    //下单次数
    Long orderCt;
    //下单人数
    Long personCt;

    // 时间戳
    Long ts;


}
