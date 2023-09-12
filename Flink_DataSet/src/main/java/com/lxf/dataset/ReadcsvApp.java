package com.lxf.dataset;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class ReadcsvApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("/Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_DataSet/src/main/java/com/lxf/data");
        source.print();
        env.execute();

    }
}

