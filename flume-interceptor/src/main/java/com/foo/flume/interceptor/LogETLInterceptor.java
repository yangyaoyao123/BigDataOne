package com.foo.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LogETLInterceptor implements Interceptor {
//初始化方法
    @Override
    public void initialize() {

    }
//一个事件
    @Override
    public Event intercept(Event event) {
        //获取传输过来的数据（消息体）
        String body = new String(event.getBody(), Charset.forName("UTF-8"));
        //此处body为原始数据 ，需要处理
        //if判断是否为自己想要的数据
        if(LogUtils.validateReportLog(body)){
            //通过了校验就是目标数据
            return  event;
        }
        //没有通过就不是目标数据
        return null;
    }
//一个事件的集合
    @Override
    public List<Event> intercept(List<Event> events) {

        List<Event> intercepted = new ArrayList<Event>(events.size());
       //循环List集合的event时间做校验
        for (Event event:events){
           //foreach循环调用intercept方法
           Event interceptEvent = intercept(event);
           if(interceptEvent!=null){
               intercepted.add(interceptEvent);
           }
       }
        return intercepted;
    }

    @Override
    public void close() {

    }
    public  static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
        //调用LogETLInterceptor方法
         return new LogETLInterceptor();
        }


        //上下文对象
        @Override
        public void configure(Context context) {

        }
    }


}
