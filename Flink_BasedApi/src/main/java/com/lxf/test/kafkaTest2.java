package com.lxf.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class kafkaTest2 {
    public static void main(String[] args) throws Exception{
        //创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        test1(env);
        env.execute("com.lxf.test.kafkaTest2");
    }

    /**
     * 对从kafka读取到的数据进行词频统计然后再输出到kafka中
     * @param env
     */
    public static void test1(StreamExecutionEnvironment env) {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","110.40.173.142:9092");
        prop.setProperty("group.id","com.lxf.test.test");
        prop.setProperty("auto.offset.reset", "latest");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<String>("com.lxf.test.test",new SimpleStringSchema(),prop));
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

        source.map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                return value.f0+","+value.f1;
            }
        }).addSink(new FlinkKafkaProducer<String>("test1",new SimpleStringSchema(),prop));

    }
}
