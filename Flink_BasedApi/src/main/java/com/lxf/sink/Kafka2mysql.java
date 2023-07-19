package com.lxf.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class Kafka2mysql {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Readkafka(env);
        //ReadSocket(env);
        env.execute();
    }

    /**
     * 读取端口数据
     */
    private static void ReadSocket(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9876);
        SingleOutputStreamOperator<Tuple2<String, Integer>> streams = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).keyBy(x -> x.f0).sum(1);
        streams.addSink(new TestMysqlSink2());
    }


    /**
     * 读kafka数据
     */
    public static void Readkafka(StreamExecutionEnvironment env){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "110.40.173.142:9092");
        properties.setProperty("group.id", "com.lxf.test.test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<String>("test",new SimpleStringSchema(),properties));
        SingleOutputStreamOperator<Tuple2<String, Integer>> streams = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        }).keyBy(x -> x.f0).sum(1);
        //自定义sink实现
        streams.addSink(new TestMysqlSink2());
    }
}
