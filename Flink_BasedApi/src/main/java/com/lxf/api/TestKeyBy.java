package com.lxf.api;

import com.lxf.model.Access;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TestKeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyByTest(env);
        env.execute();
    }

    private static void KeyByTest(StreamExecutionEnvironment env) {
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
        //过时方法
        //mapStream.keyBy("name").sum("conut").print();

        //简写方法
        //mapStream.keyBy(x -> x.getName()).sum("conut").print();

        mapStream.keyBy(new KeySelector<Access, String>() {
            @Override
            public String getKey(Access access) throws Exception {
                return access.getName();
            }
        }).sum("conut").print();
    }

    private static void KeyByTest1(StreamExecutionEnvironment env){
        DataStreamSource<String> source = env.fromElements("flink","spark","flink");
        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> out) throws Exception {
                String[] split = s.split("，");
                for (String value : split) {
                    out.collect(value);
                }
            }
        })
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return  Tuple2.of(value,1);
                    }
                })
                //过时方法
                //.keyBy("f0").print();

                //简写方法
                //.keyBy(x -> x.f0).print();
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }).print();
    }
}
