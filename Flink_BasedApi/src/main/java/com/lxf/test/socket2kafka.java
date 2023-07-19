package com.lxf.test;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class socket2kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Socket2Kafka(env);
        env.execute();
    }

    public static void Socket2Kafka(StreamExecutionEnvironment env){
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","172.16.101.50:9092,172.16.104.110:9092,172.16.104.111:9092");
        prop.setProperty("group.id","com.lxf.test.ailin_test1");
        prop.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> stream = env.socketTextStream("localhost", 9633);
        stream.addSink(new FlinkKafkaProducer<String>("com.lxf.test.ailin_test1",new SimpleStringSchema(),prop));

        DataStream<String> stream2 = env
                .addSource(new FlinkKafkaConsumer<String>("com.lxf.test.ailin_test1",new SimpleStringSchema(),prop));
        stream2.print();
    }
}
