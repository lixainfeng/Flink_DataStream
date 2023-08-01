package com.lxf.watermark;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class EventTimeApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        test2(env);
        env.execute();
    }
    public static void test(StreamExecutionEnvironment env){
        SingleOutputStreamOperator<String> stream = env.socketTextStream("localhost", 9527)
                /**
                 * flink1.12时间类型默认是eventtime，不需要指定
                 * 在使用基于event的窗口函数时，必须在前面指定watermark
                 * 这里设置watermark为0，即表示不接收延迟数据
                 */
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(String s) {
                        return Long.parseLong(s.split(",")[0]);
                    }
                });

        stream.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split(",");
                return Tuple2.of(split[1],Integer.parseInt(split[2]));
            }
        })
        .keyBy(x -> x.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))//滚动窗口，每5秒一次
        .sum(1)
        .print();
    }
    public static void test2(StreamExecutionEnvironment env){
        OutputTag<Tuple2<String, Integer>> outputTag = new OutputTag<Tuple2<String, Integer>>("late-data") {};
        DataStreamSource<String> stream = env.socketTextStream("localhost", 9527);
        SingleOutputStreamOperator<Tuple2<String, Integer>> window_stream =
                stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String s) {
                return Long.parseLong(s.split(",")[0]);
            }
        })
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String[] splits = s.split(",");
                        return Tuple2.of(splits[1], Integer.parseInt(splits[2]));
                    }
                })
                .keyBy(x -> x.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(6))) //滚动窗口
                .sideOutputLateData(outputTag)//延迟数据放入侧输出流里
                .allowedLateness(Time.seconds(3))//最大允许延迟3秒
                .sideOutputLateData(outputTag)//延迟之外的数据保存在侧输出流
                .sum(1);

        window_stream.print();
        DataStream<Tuple2<String, Integer>> sideOutput = window_stream.getSideOutput(outputTag);
        sideOutput.printToErr();

    }
}
