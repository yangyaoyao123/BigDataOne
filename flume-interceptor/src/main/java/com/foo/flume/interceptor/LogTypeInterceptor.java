package com.foo.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class LogTypeInterceptor implements Interceptor {


    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //获取Flume的数组（body）
        byte[] body = event.getBody();
        //给json数组转换位字符串
        String jsonStr = new String(body, Charset.forName("UTF-8"));
        //校验contains字符串中包含start
        String logType=" ";
        //判断日志类型 start是启动日志 其他11种为时间日志
        if(jsonStr.contains("start")){
             logType="start";
        }else{
             logType="event";
        }
        //获取flume消息头
        Map<String, String> heders = event.getHeaders();
        //将日志类型添加到flume的消息头中
        heders.put("logType",logType);


        return event;


    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
                intercept(event);
        }


        return events;
    }

    @Override
    public void close() {

    }
    public  static  class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
