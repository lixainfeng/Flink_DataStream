package com.lxf.api;

import com.lxf.model.Access;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * map算子，作用在流上每一个数据元素上
 */
public class TestMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ReadFileMap(env);
        //CollectMap(env);
        env.execute("com.lxf.test.test");
    }

    private static void ReadFileMap(StreamExecutionEnvironment env) {
        DataStreamSource<String> source= env
                .readTextFile("/Users/shuofeng/IdeaProjects/Test/flink-datastream/src/main/java/com/lxf/data/1.txt");
        SingleOutputStreamOperator<Access> mapStream = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String s) throws Exception {
                String[] value = s.split(",");
                Long time = Long.parseLong(value[0]);
                String name = value[1];
                Integer count = Integer.parseInt(value[2]);
                return new Access(time,name,count);
            }
        });
        mapStream.print();
    }
    private static void CollectMap(StreamExecutionEnvironment env){
        ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);
        DataStreamSource<Integer> source = env.fromCollection(list);
        source.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        }).print();
    }
}
