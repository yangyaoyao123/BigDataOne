package com.foo.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {
    public String evaluate(String line,String  jsonkeysString){

        //字符缓冲区 存储数据
        StringBuilder sb=new StringBuilder();
        //切割传入过来的字段名
            String[] jsonkeys = jsonkeysString.split(",");
        //切割数据
        String[] logContents=line.split("\\|");
        //1577437388936  |
        //{'cm':{'ln':'-51.6','sv':'V2.4.9','os':'8.2.2','g':'4K6BRGL4@gmail.com','mid':'m037','nw':'3G','l':'pt','vc':'5','hw':'640*1136','ar':'MX','uid':'u008','t':'1577375231091','la':'20.8','md':'HTC-17','vn':'1.1.2','ba':'HTC','sr':'W'},'ap':'gmall','et':[{'ett':'1577416382120','en':'loading','kv':{'extend2':'','loading_time':'4','action':'3','extend1':'','type':'1','type1':'433','loading_way':'1'}},{'ett':'1577366126570','en':'ad','kv':{'entry':'1','show_style':'1','action':'4','detail':'325','source':'3','behavior':'1','content':'2','newstype':'6'}},{'ett':'1577386920949','en':'active_background','kv':{'active_source':'3'}},{'ett':'1577376802342','en':'error','kv':{'errorDetail':'java.lang.NullPointerException\\n    at cn.lift.appIn.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\\n at cn.lift.dfdf.web.AbstractBaseController.validInbound','errorBrief':'at cn.lift.appIn.control.CommandUtil.getInfo(CommandUtil.java:67)'}},{'ett':'1577425314731','en':'praise','kv':{'target_id':4,'id':8,'type':1,'add_time':'1577376182820','userid':7}}]}"
        //logContents是String类型用isBlank判断是否为空 ，字符串，制表符是了返回true 不是则是false
        if(logContents.length !=2 || StringUtils.isBlank(logContents[1])){
            return  "";
        }
        try {
            //JsonObject 方法  进行解析窃喜出来的第二段json
            // 1577437388936  |    {'cm':{'ln':'-51.6','sv':'V2.4.9','os':'8.2.2','g':'4K6BRGL4@gmail.com','mid':'m037','nw':'3G','l':'pt','vc':'5','hw':'640*1136','ar':'MX','uid':'u008','t':'1577375231091','la':'20.8','md':'HTC-17','vn':'1.1.2','ba':'HTC','sr':'W'},'ap':'gmall','et':[{'ett':'1577416382120','en':'loading','kv':{'extend2':'','loading_time':'4','action':'3','extend1':'','type':'1','type1':'433','loading_way':'1'}},{'ett':'1577366126570','en':'ad','kv':{'entry':'1','show_style':'1','action':'4','detail':'325','source':'3','behavior':'1','content':'2','newstype':'6'}},{'ett':'1577386920949','en':'active_background','kv':{'active_source':'3'}},{'ett':'1577376802342','en':'error','kv':{'errorDetail':'java.lang.NullPointerException\\n    at cn.lift.appIn.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\\n at cn.lift.dfdf.web.AbstractBaseController.validInbound','errorBrief':'at cn.lift.appIn.control.CommandUtil.getInfo(CommandUtil.java:67)'}},{'ett':'1577425314731','en':'praise','kv':{'target_id':4,'id':8,'type':1,'add_time':'1577376182820','userid':7}}]}"
            JSONObject jsonObject = new JSONObject(logContents[1]);
            //获取json cm的字段
            JSONObject base = jsonObject.getJSONObject("cm");
            //for循环 字段长度
            for(int i=0; i<jsonkeys.length;i++){
                //trim去除字段两边的空格
                String filedName = jsonkeys[i].trim();
                //判断 json cm 里面是否有 字段
                if(base.has(filedName)){
                    //字段追加到缓冲区 tab
                sb.append(base.getString(filedName)).append("\t");
                }else{
                    //没有就追加空
                    sb.append("").append("\t");
                }
            }
            sb.append(jsonObject.getString("et")).append("\t");
            sb.append(logContents[0]).append("\t");
        } catch (JSONException e) {
            e.printStackTrace();
        }

        return sb.toString();
    }
    public static void main(String[] args){
        //传入数据
        String line ="1577437388936|{'cm':{'ln':'-51.6','sv':'V2.4.9','os':'8.2.2','g':'4K6BRGL4@gmail.com','mid':'m037','nw':'3G','l':'pt','vc':'5','hw':'640*1136','ar':'MX','uid':'u008','t':'1577375231091','la':'20.8','md':'HTC-17','vn':'1.1.2','ba':'HTC','sr':'W'},'ap':'gmall','et':[{'ett':'1577416382120','en':'loading','kv':{'extend2':'','loading_time':'4','action':'3','extend1':'','type':'1','type1':'433','loading_way':'1'}},{'ett':'1577366126570','en':'ad','kv':{'entry':'1','show_style':'1','action':'4','detail':'325','source':'3','behavior':'1','content':'2','newstype':'6'}},{'ett':'1577386920949','en':'active_background','kv':{'active_source':'3'}},{'ett':'1577376802342','en':'error','kv':{'errorDetail':'java.lang.NullPointerException\\n    at cn.lift.appIn.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\\n at cn.lift.dfdf.web.AbstractBaseController.validInbound','errorBrief':'at cn.lift.appIn.control.CommandUtil.getInfo(CommandUtil.java:67)'}},{'ett':'1577425314731','en':'praise','kv':{'target_id':4,'id':8,'type':1,'add_time':'1577376182820','userid':7}}]}";
        //字段名
        String x=new BaseFieldUDF().evaluate(line,"mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la");
        System.out.println(x);
    }
}
