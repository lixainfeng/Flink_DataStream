package com.lxf.api;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class TestCoFlatMapFun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //定义元素集合
        DataStreamSource<String> stream1 = env.fromElements("1,2,3");
        DataStreamSource<String> stream2 = env.fromElements("a|b|c");
        stream1.connect(stream2).flatMap(new CoFlatMapFunction<String, String, String>() {
            @Override
            public void flatMap1(String s, Collector<String> collector) throws Exception {
                String[] splits = s.split(",");
                for(String words : splits){
                    collector.collect(words);
                }
            }

            @Override
            public void flatMap2(String s, Collector<String> collector) throws Exception {
                String[] splits = s.split("\\|");
                for(String words : splits){
                    collector.collect(words);
                }
            }
        }).print();
        env.execute();
    }
}
