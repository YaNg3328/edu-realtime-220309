package edu.realtime.bean;

import java.util.Set;

public class DwsCourseCategorySubjectCourseOrderBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 类别
    String category;
    // 科目
    String subject;
    //  课程
    String course;
    // 用户ID
    @TransientSink
    Set<String> userIdSet;
    //金额
    Double amount;
    //用户数量
    Long uCt;
    // 时间戳
    Long ts;


}
