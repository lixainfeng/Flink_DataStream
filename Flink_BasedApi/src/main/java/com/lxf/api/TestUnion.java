package com.lxf.api;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * union算子，可以实现多个流之间的合并，也可以实现自己和自己合并
 */
public class TestUnion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9633);
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9634);
        stream1.union(stream2).print();
        //stream1.union(stream1).print();

        env.execute();
    }
}
