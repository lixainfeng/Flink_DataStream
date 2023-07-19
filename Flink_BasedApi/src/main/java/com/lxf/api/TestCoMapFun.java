package com.lxf.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class TestCoMapFun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9633);
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9634);

        SingleOutputStreamOperator<Integer> stream3 = stream2.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return Integer.parseInt(s);
            }
        });
        /**
         * CoMapFunction可以实现ConnectedStreams流转换成DataStream流
         */
        stream1.connect(stream3).map(new CoMapFunction<String, Integer, String>() {
            //comap中第一个参数为流一数据类型，第二个为流二数据类型，第三个为返回值类型
            @Override
            public String map1(String s) throws Exception {
                return s.toUpperCase();
            }

            @Override
            public String map2(Integer value) throws Exception {
                return value * 10+ "";
            }
        }).print();
        env.execute();
    }
}
