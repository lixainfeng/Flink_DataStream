package com.lxf.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Readkafka {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //kafka配置相关
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","110.40.173.142:9092");
        prop.setProperty("group.id","test1");
        prop.setProperty("enable.auto.commit", "false");
        prop.setProperty("auto.offset.reset", "earliest");
        String topic = "test";

        //checkpoint相关
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("file:/Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_Demo/src/main/java/com/lxf/data/checkpoint/"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                Time.of(5, TimeUnit.SECONDS) // 间隔
        ));

        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),prop));
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
        source.print();

        env.socketTextStream("localhost", 9527)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        if(value.contains("pk")) throw new RuntimeException("把PK哥拉黑");
                        return value.toUpperCase();
                    }
                }).print("From Socket:");
        env.execute();
    }
}
