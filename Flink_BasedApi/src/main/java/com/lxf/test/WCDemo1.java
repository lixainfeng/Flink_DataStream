package com.lxf.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 从socket端口获取数据进行词频统计
 */
public class WCDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 9633);
        //flatmap算子将一行行数据按照逗号分割成一个个单词（pk,pk,flink -> pk pk flink）
        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] value = s.split(",");
                for (String words : value){
                    collector.collect(words);
                }
            }
        })
                //map算子将为每一个单词赋值为1（pk pk flink -> (pk,1) (pk,1) (flink,1)）
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return Tuple2.of(s,1);
                    }
                })
                //keyby算子将按照相同的单词进行分组
                .keyBy(x -> x.f0)

                //keyby算子将按照相同的单词进行分组
                //.keyBy(x -> x.f0).sum(1).print();

                //reduce
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0,value1.f1+value2.f1);
                    }
                }).print();
        env.execute();
    }
}
