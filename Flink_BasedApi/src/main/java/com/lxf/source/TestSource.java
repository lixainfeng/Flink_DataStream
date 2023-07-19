package com.lxf.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestSource {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment.createLocalEnvironment();
        StreamExecutionEnvironment.createRemoteEnvironment("",2222,"");
        env.setParallelism(3);
        //test01(env);
        test02(env);
        env.execute();
    }
    public static void test01(StreamExecutionEnvironment env){
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 9633);
        System.out.println(socketStream.getParallelism());
        System.out.println(socketStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !"ss".equals(value);
            }
        }).getParallelism());
    }
    public static void test02(StreamExecutionEnvironment env){
        DataStreamSource<Integer> elements = env.fromElements(1, 2, 3, 4);
        System.out.println(elements.getParallelism());
        System.out.println(elements.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value >= 2;
            }
        }).getParallelism());
    }
}
