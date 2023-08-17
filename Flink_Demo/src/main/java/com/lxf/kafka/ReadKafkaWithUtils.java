package com.lxf.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;


public class ReadKafkaWithUtils {
    public static void main(String[] args) throws Exception {
        // /Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_Demo/src/main/java/com/lxf/data/MyFlink.properties
        DataStream<String> stream = FlinkUtils.createKafkaStreamV1(args);
        //FlinkUtils.createKafkaStreamV2(args, SimpleStringSchema.class);
        SingleOutputStreamOperator<Tuple2<String, Integer>> source = stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] value = s.split(",");
                for (String words : value) {
                    collector.collect(words);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        }).keyBy(x -> x.f0).sum(1);
        source.print("From Kafka:");

        //TODO... 结果写入redis中
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").build();
        source.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                return Tuple2.of(value.f0,value.f1);
            }
        }).addSink(new RedisSink<Tuple2<String, Integer>>(conf,new TestRedisSink()));
        FlinkUtils.env.execute();

    }
}
