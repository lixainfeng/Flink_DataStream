package com.lxf.partition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestParaPartition {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        test01(env);
        env.execute();
    }
    public static void test01(StreamExecutionEnvironment env){
        DataStreamSource<String> elements = env.fromElements("zhangsan", "lisi", "wangwu");
        elements.map(new MapFunction<String, Tuple2<String,String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return Tuple2.of("value is: ", value);
            }
        }).partitionCustom(new TestPartition(),1)
                .map(new MapFunction<Tuple2<String, String>, String>() {
                    @Override
                    public String map(Tuple2<String, String> value) throws Exception {
                        System.out.println("当前线程是："+Thread.currentThread().getId()+"   value is : "+value.f1);
                        return value.f1;
                    }
                }).broadcast()
                .print();
    }
}
