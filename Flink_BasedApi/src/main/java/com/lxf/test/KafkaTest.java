package com.lxf.test;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaTest {
    public static void main(String[] args)  throws Exception{
        //创建flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //kafka参数
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "110.40.173.142:9092");
        //prop.setProperty("group.id", "flink-group");
        String inputTopic = "com.lxf.test.test";
        String outputTopic = "WordCount";

        //source
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<String>(inputTopic, new SimpleStringSchema(), prop);
        DataStream<String> stream = env.addSource(consumer);

        //Transformations
        //使用Flink算子对输入流的文本进行操作
        //按空格切词、计数、分区、设置时间窗口、聚合
        DataStream<Tuple2<String,Integer>> wc = stream.flatMap(
                (String line, Collector<Tuple2<String, Integer>> collector) ->{
                    String[] tokens = line.split("\\s");
                    // 输出结果 (word, 1)
                    for (String token : tokens) {
                        if (token.length() > 0) {
                            collector.collect(new Tuple2<>(token, 1));
                        }
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);
        // Sink
        wc.print();

        // execute
        env.execute("kafka streaming word count");
    }
}
