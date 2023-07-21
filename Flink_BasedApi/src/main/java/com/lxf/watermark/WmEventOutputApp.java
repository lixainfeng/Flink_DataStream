package com.lxf.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class WmEventOutputApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//flink1.12默认即是EventTime
        test01(environment);
        environment.execute();
    }
    public static void test01(StreamExecutionEnvironment environment){
        OutputTag<Tuple2<String, Integer>> outputTag = new OutputTag<Tuple2<String, Integer>>("late-data") {};
        DataStreamSource<String> stream = environment.socketTextStream("localhost", 9627);
        /**
         * 设置water的水位线为5
         */
        SingleOutputStreamOperator<String> window_stream = stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(5)) {
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
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) //滚动窗
                .sideOutputLateData(outputTag)//延迟数据放入侧输出流里
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        System.out.println("key => " + value1.f0 + ", value => " + (value1.f1 + value2.f1));
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }, new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<String> collector) throws Exception {
                        FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");//获取传进来的格式化时间
                        for (Tuple2<String, Integer> tuple2 : iterable) {
                            collector.collect("[" + format.format(context.window().getStart()) + "==>" + format.format(context.window().getEnd()) + "], " + tuple2.f0 + "==>" + tuple2.f1);

                        }
                    }
                });
        window_stream.print();
        DataStream<Tuple2<String, Integer>> sideOutput = window_stream.getSideOutput(outputTag);//获取到侧输出流里的数据
        sideOutput.printToErr();
    }
}
