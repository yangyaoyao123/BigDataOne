package com.foo.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LogUtils {
   //打印日志
    private static Logger logger=  LoggerFactory.getLogger(LogUtils.class);

    //编写校验方法
    public static boolean validateReportLog(String log) {
        //日志检查 正确的返回true 错误的返回false


        /*
        日志数据
        1577438183607|{"cm":{"ln":"-40.9","sv":"V2.3.0","os":"8.0.9","g":"H11OY8RC@gmail.com","mid":"m402","nw":"4G","l":"en","vc":"5","hw":"750*1134","ar":"MX","uid":"u801","t":"1577371191856","la":"-46.0","md":"Huawei-8","vn":"1.0.6","ba":"Huawei","sr":"Z"},"ap":"gmall","et":[{"ett":"1577412089589","en":"display","kv":{"newsid":"n415","action":"2","extend1":"2","place":"0","category":"61"}},{"ett":"1577376734627","en":"newsdetail","kv":{"entry":"1","newsid":"n025","news_staytime":"0","loading_time":"2","action":"4","showtype":"1","category":"100","type1":"325"}},{"ett":"1577348639178","en":"loading","kv":{"extend2":"","loading_time":"0","action":"3","extend1":"","type":"3","type1":"","loading_way":"1"}},{"ett":"1577363023891","en":"ad","kv":{"entry":"1","show_style":"3","action":"5","detail":"","source":"4","behavior":"1","content":"1","newstype":"2"}},{"ett":"1577376808915","en":"error","kv":{"errorDetail":"java.lang.NullPointerException\\n    at cn.lift.appIn.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\\n at cn.lift.dfdf.web.AbstractBaseController.validInbound","errorBrief":"at cn.lift.dfdf.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)"}},{"ett":"1577394160461","en":"favorites","kv":{"course_id":4,"id":0,"add_time":"1577349201155","userid":9}},{"ett":"1577375041541","en":"praise","kv":{"target_id":4,"id":2,"type":2,"add_time":"1577341910093","userid":9}}]}
         */
        try {

            //截取日志
            String[] logs = log.split("\\|");
            //根据|判断 首先校验总长度
            if (logs.length < 2) {
                return false;
            }
            //其次校验第一段是否为时间戳时间戳
            //判断截取的第一段时间戳是否长度为13且是否为数组
            //logs[0] 是第一段 时间戳
            if ((logs[0].length() != 13) || !NumberUtils.isDigits(logs[0])) {
                return false;
            }
            //再次校验 | 后第二段数据是否为正确的Json串
            //trim删除头尾的字符串然后使用startWith方法校验开头是否为 {  endsWith 校验结尾是否为 }
            if (!logs[1].trim().startsWith("{") || !logs[1].trim().endsWith("}")) {
                return false;
            }

        } catch (Exception e) {

            logger.error("error parse,message is : "+log);//抓取错误日志
            logger.error(e.getMessage());
             return false;
        }


        return  true;

    }
}
