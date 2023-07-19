package com.lxf.api;

import com.lxf.model.Access;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestKeyByWithPoJo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        test1(env);
        env.execute();
    }
    private static void test1(StreamExecutionEnvironment env){
        DataStreamSource<String> source= env
                .readTextFile("/Users/shuofeng/IdeaProjects/Test/flink-datastream/src/main/java/com/lxf/data/1.txt");
        SingleOutputStreamOperator<Access> mapStream = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                String[] split = value.split(",");
                Long time = Long.parseLong(split[0]);
                String name = split[1];
                Integer count = Integer.parseInt(split[2]);
                return new Access(time, name, count);
            }
        });
        KeyedStream<Access, String> keyedStream = mapStream.keyBy(new KeySelector<Access, String>() {
            @Override
            public String getKey(Access value) throws Exception {
                return value.getName();
            }
        });
        keyedStream.sum("conut").print();
    }
}
