package com.foo.udtf;



import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;


public class EventJsonUDTF extends GenericUDTF {

//该方法中，我们将指定输出参数的名称和参数类型
    @Override
     public StructObjectInspector initialize(ObjectInspector[] argOIs) throws
            UDFArgumentException { //initalize()方法通过Constants的静态属性获得列的所有属性字符串
        List<String> fieldNames = new ArrayList<String>();
        //先定义List<ObjectInspector>列表，在静态代码块中ObjectInspectorFactory工厂方法将基本类型通过反射构造出对应的ObjectInspector
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        //添加输出的参数名称
        fieldNames.add("event_name");
        //类型
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        //添加输出的参数名称
        fieldNames.add("event_json");
        //类型
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        //设置输出格式的名称和类名
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }
    @Override
    public void process(Object[] objects) {
        //获取传入的et
//        [{"ett":"1577416382120","en":"loading","kv":{"extend2":"","loading_time":"4","action":"3","extend1":"","type":"1","type1":"433","loading_way":"1"}},
//        {"ett":"1577366126570","en":"ad","kv":{"entry":"1","show_style":"1","action":"4","detail":"325","source":"3","behavior":"1","content":"2","newstype":"6"}},
//        {"ett":"1577386920949","en":"active_background","kv":{"active_source":"3"}},
//        {"ett":"1577376802342","en":"error","kv":{"errorDetail":"java.lang.NullPointerException\n    at cn.lift.appIn.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\n at cn.lift.dfdf.web.AbstractBaseController.validInbound","errorBrief":"at cn.lift.appIn.control.CommandUtil.getInfo(CommandUtil.java:67)"}},{"ett":"1577425314731","en":"praise","kv":{"target_id":4,"id":8,"type":1,"add_time":"1577376182820","userid":7}}]	1577437388936
        //没有切割就是0
        String input = objects[0].toString();
        //isBlank判断是否为空 ，字符串，制表符是了返回true 不是则是false 如果为空则直接过滤
        if(StringUtils.isBlank(input)){
            return;
        }else{
            try {
                //不为空开始 获取有即可事件
                JSONArray ja = new JSONArray(input);
                if(ja==null){//判断json是否为空
                    return;
                }
                //循环遍历每一个事件
                for(int i=0;i<ja.length();i++){
                    //创建一个数组 长度为2 来存储事件名称和事件内容
                    String[] result = new String[2];
                    try {
                        // 取出每个的事件名称
                        result[0]=ja.getJSONObject(i).getString("en");
                        // 取出每个的事件内容
                        result[1]=ja.getString(i);

                    } catch (Exception e) {
                       continue;
                    }
                    //将结果返回到result数组
                    forward(result);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    @Override
    public void close() throws HiveException {

    }
}
