package com.lxf.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class StateApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        test01(env);
        env.execute();
    }
    /**使用valuestate实现平均数*/
    public static void test01(StreamExecutionEnvironment env) throws Exception {
        ArrayList<Tuple2<Long,Long>> list = new ArrayList<>();
        list.add(Tuple2.of(1L,5L));
        list.add(Tuple2.of(2L,7L));
        list.add(Tuple2.of(1L,4L));
        list.add(Tuple2.of(2L,3L));
        list.add(Tuple2.of(1L,8L));
        list.add(Tuple2.of(2L,2L));
        env.fromCollection(list)
                .keyBy(x -> x.f0)
                .flatMap(new AvgWithValueState())
                .print();


                /*.flatMap(new FlatMapFunction<Tuple2<Long, Long>, String>() {
                    @Override
                    public void flatMap(Tuple2<Long, Long> value, Collector<String> collector) throws Exception {
                        String valuess   = value.f0+"===>"+value.f1;
                        collector.collect(valuess);
                    }
                }).print().setParallelism(1);*/
    }
}
