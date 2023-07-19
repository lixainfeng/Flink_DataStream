package com.lxf.sink;

import com.lxf.model.Access;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

public class SInkApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source= env
                .readTextFile("/Users/shuofeng/IdeaProjects/Test/flink-datastream/src/main/java/com/lxf/data/1.txt");
        SingleOutputStreamOperator<Access> mapStream = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String s) throws Exception {
                String[] value = s.split(",");
                Long time = Long.parseLong(value[0]);
                String name = value[1];
                Integer count = Integer.parseInt(value[2]);
                return new Access(time,name,count);
            }
        });
        SingleOutputStreamOperator<Access> resoult = mapStream.keyBy(new KeySelector<Access, String>() {
            @Override
            public String getKey(Access access) throws Exception {
                return access.getName();
            }
        }).sum("conut");

        resoult.print();

        //TODO... 结果写入redis中
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("42.192.196.73").build();
        resoult.map(new MapFunction<Access, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Access access) throws Exception {
                return Tuple2.of(access.getName(),access.getConut());
            }
        }).addSink(new RedisSink<Tuple2<String, Integer>>(conf,new TestRedisSink()));


        //TODO... 结果写入mysql中
        //resoult.addSink(new TestMysqlSink());

        env.execute();
    }
}
