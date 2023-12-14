package com.lxf.Join;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * 关联维表join有三种方式
 * 预加载维表
 * 热存储维表
 * 广播维表
 */
public class LookUpJoinApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Tuple3<String, String, Long>> user_stream = env.socketTextStream("localhost", 3456)
                .map(new MapFunction<String, Tuple3<String, String, Long>>() { //（用户名称，id,事件戳）
                    @Override
                    public Tuple3<String, String, Long> map(String value) throws Exception {
                        String[] split = value.split(",");
                        String user = split[0];
                        String c_id = split[1];
                        Long c_time = Long.parseLong(split[2]);
                        return Tuple3.of(user,c_id,c_time);
                    }
                });
        user_stream.map(new PerLoadDimFun()).print();

        env.socketTextStream("localhost", 1234)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String value) throws Exception {
                        String[] split = value.split(",");
                        return Tuple2.of(split[0], split[1]);
                    }
                });

        env.execute();
    }
    //todo 预加载维表至内存，适合小数据量及数据更新不频繁的数据
    //维表数据可以读取自外部表例如mysql、redis、hbase等，通过richsourcefunction来实现
    public static class PerLoadDimFun extends RichMapFunction<Tuple3<String, String, Long>,Tuple3<String, String, String>>{
        Map<String,String> dim = new HashMap<>();
        @Override
        public void open(Configuration parameters) throws Exception {
            dim.put("001","北京");
            dim.put("002","上海");
            dim.put("003","深圳");
        }

        @Override
        public Tuple3<String, String, String> map(Tuple3<String, String, Long> value) throws Exception {
            String c_name = null;
            if(dim.containsKey(value.f1)){
                c_name = dim.get(value.f1);
            }
            return Tuple3.of(value.f0,value.f1,c_name);
        }
    }


}
