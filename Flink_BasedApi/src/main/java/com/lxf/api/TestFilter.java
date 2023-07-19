package com.lxf.api;

import com.lxf.model.Access;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestFilter {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //ReadFileMap(env);
        FilterTest(env);
        env.execute("com.lxf.test.test");
    }

    private static void ReadFileMap(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env
                .readTextFile("/Users/shuofeng/IdeaProjects/Test/flink-datastream/src/main/java/com/lxf/data/1.txt");
        SingleOutputStreamOperator<Access> mapStream = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String s) throws Exception {
                String[] value = s.split(",");
                Long time = Long.parseLong(value[0]);
                String name = value[1];
                Integer count = Integer.parseInt(value[2]);
                return new Access(time, name, count);
            }
        });
        mapStream.filter(new FilterFunction<Access>() {
            @Override
            public boolean filter(Access access) throws Exception {
                return access.getConut() > 2400;
            }
        }).print();
    }
    private static void FilterTest(StreamExecutionEnvironment env){
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5);
        source.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value >= 3;
            }
        }).print();
    }
}
