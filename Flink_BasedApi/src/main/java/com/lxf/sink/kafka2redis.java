package com.lxf.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class kafka2redis {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ReadRedis(env);
        env.execute();
    }

    /**
     * 读kafka数据
     */
    public static void ReadRedis(StreamExecutionEnvironment env) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "110.40.173.142:9092");
        properties.setProperty("group.id", "com.lxf.test.test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<Tuple2<String, Integer>> streams = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        }).keyBy(x -> x.f0).sum(1);

        //TODO... 结果写入redis中
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("42.192.196.73").build();
        streams.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                return Tuple2.of(value.f0,value.f1);
            }
        }).addSink(new RedisSink<Tuple2<String, Integer>>(conf,new TestRedisSink()));
    }
}
