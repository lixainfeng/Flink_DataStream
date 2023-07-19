package com.lxf.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        test07(env);
        env.execute();
    }

    /**
     * 不带keybey的流用windowall
     */
    public static void test01(StreamExecutionEnvironment env){
        env.socketTextStream("localhost",9633)
                .map(new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String s) throws Exception {
                        return Integer.parseInt(s);
                    }
                })
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(0)
                .print();
    }

    /**
     * 带keybey的流用window
     */
    public static void test02(StreamExecutionEnvironment env){
        env.socketTextStream("localhost",9633)
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return Tuple2.of(s,1);
                    }
                })
                .keyBy(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();
    }

    /**
     * windowfunction增量处理方式：来一条处理一条
     */
    public static void test03(StreamExecutionEnvironment env){
        env.socketTextStream("localhost",9633)
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return Tuple2.of(s,1);
                    }
                })
                .keyBy(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() { /** 实现增量计算**/
                @Override
                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                    return Tuple2.of(value1.f0,value1.f1+value2.f1);
                }
                })
                .print();
    }

    /**
     * processFunction全量缓存
     */
    public static void test04(StreamExecutionEnvironment env) {
        env.socketTextStream("localhost",9633)
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return Tuple2.of("sf",Integer.parseInt(s));
                    }
                })
                .keyBy(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessFun())
                .print();
    }

    /**
     * 基于processTime的会话窗口
     */
    public static void test05(StreamExecutionEnvironment env) {
        env.socketTextStream("localhost",9633)
                .map(new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String s) throws Exception {
                        return Integer.parseInt(s);
                    }
                })
                .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(0)
                .print();
    }

    /**
     * 基于ProcessingTime的Non-Keyed和Keyed的滑动窗口
     */
    public static void test06(StreamExecutionEnvironment env) {
        DataStreamSink<Integer> nokeyedstream = env.socketTextStream("localhost", 9633)
                .map(new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String s) throws Exception {
                        return Integer.parseInt(s);
                    }
                })
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(0)
                .print();


        DataStreamSink<Tuple2<String, Integer>> keyedstream = env.socketTextStream("localhost", 9633)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return Tuple2.of(s, 1);
                    }
                })
                .keyBy(x -> x.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(1)
                .print();
    }

    /**
     * 基于AggregateFunction的滚动窗口
     */
    public static void test07(StreamExecutionEnvironment env){
        env.socketTextStream("localhost",9633)
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] splits = s.split(",");
                        return Tuple2.of(splits[0],Long.parseLong(splits[1]));
                    }
                })
                .keyBy(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AverageAggregate())
                .print();
    }
}
