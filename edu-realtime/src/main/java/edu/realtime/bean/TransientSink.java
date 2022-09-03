package edu.realtime.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 自定义注解上面标记两个原注解
 * 1.表名注解用在什么位置
 * 2.表名注解在什么时候用
 * 3.具体可以参考 @Transient
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientSink {
}
