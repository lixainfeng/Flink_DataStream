package com.lxf.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WmEventSessionApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        test01(env);
        env.execute();
    }
    public static void test01(StreamExecutionEnvironment env) throws Exception {
        SingleOutputStreamOperator<String> lines = env.socketTextStream("localhost", 9633)
                /**数据形式：时间字段，单词，次数
                 * 最大允许延迟时间，0秒意味着没有延迟
                 * eventTime抽取时间字段
                 */
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String s) {
                        return Long.parseLong(s.split(",")[0]);
                    }
                });
        lines.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] splits = s.split(",");
                return Tuple2.of(splits[1], Integer.parseInt(splits[2]));
            }
        }).keyBy(x -> x.f0)
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        System.out.println("key => " + value1.f0 + ", value => " + (value1.f1+value2.f1));
                        return Tuple2.of(value1.f0,value1.f1+value2.f1);
                    }
                })
                .print();
    }
}
