package com.lxf.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WmEventAPP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        test01(env);
        env.execute();
    }
    public static void test01(StreamExecutionEnvironment env){
        SingleOutputStreamOperator<String> lines = env.socketTextStream("localhost", 9633)
                /**数据形式：时间字段，单词，次数
                 * 最大允许延迟时间，0秒意味着没有延迟
                 * eventTime抽取时间字段
                 */
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
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
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))//[0,5000),[5000,10000),但这边会记录重复数据
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() { //增量缓存方式
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        System.out.println("key => " + value1.f0 + ", value => " + (value1.f1+value2.f1));
                        return Tuple2.of(value1.f0,value1.f1+value2.f1);
                    }
                }, new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() { //全量缓存方式
                    FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");//获取传进来的格式化时间
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<String> collector) throws Exception {

                        for (Tuple2<String, Integer> tuple2 : iterable) {
                            collector.collect("["+format.format(context.window().getStart())+"==>"+format.format(context.window().getEnd())+"], "+tuple2.f0+"==>"+tuple2.f1);

                        }
                    }
                }).print();


    }
}
