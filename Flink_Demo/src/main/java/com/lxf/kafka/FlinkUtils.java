package com.lxf.kafka;


import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FlinkUtils {
    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();




    public static DataStream<String> createKafkaStreamV1(String[] arg) throws IOException {
        ParameterTool tool = ParameterTool.fromPropertiesFile(arg[0]); //将配置文件加载到configurations里面去
        String groupId = tool.get("group.id", "test1");//获取groupid，没有的话就传一个默认值 这是可填的
        String servers = tool.getRequired("bootstrap.servers"); //这是必填的
        //如果获取多个topic需要进行分割后放入list集合中
        //List<String> topics = Arrays.asList(tool.getRequired("kafka.input.topics").split(","));
        String topic = tool.getRequired("kafka.input.topics");
        String autoCommit = tool.get("enable.auto.commit", "false");
        String offsetRest = tool.get("auto.offset.reset", "earliest");

        int checkpointInterval = tool.getInt("checkpoint.interval", 5000);
        String checkpointPath = tool.get("checkpoint.path","file:/Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_Demo/src/main/java/com/lxf/data/checkpoint/");


        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend(checkpointPath));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                Time.of(5, TimeUnit.SECONDS) // 间隔
        ));

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",servers);
        prop.setProperty("group.id",groupId);
        prop.setProperty("enable.auto.commit", autoCommit);
        prop.setProperty("auto.offset.reset", offsetRest);

        return env.addSource(new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),prop));
    }


    //通过范型封装kafka反序列化schema，使他变得更加通用
    public static <T> DataStream<T> createKafkaStreamV2(String[] arg,Class<? extends DeserializationSchema<T>> deser) throws Exception {
        ParameterTool tool = ParameterTool.fromPropertiesFile(arg[0]); //将配置文件加载到configurations里面去
        String groupId = tool.get("group.id", "test1");//获取groupid，没有的话就传一个默认值 这是可填的
        String servers = tool.getRequired("bootstrap.servers"); //这是必填的
        //如果获取多个topic需要进行分割后放入list集合中
        //List<String> topics = Arrays.asList(tool.getRequired("kafka.input.topics").split(","));
        String topic = tool.getRequired("kafka.input.topics");
        String autoCommit = tool.get("enable.auto.commit", "false");
        String offsetRest = tool.get("auto.offset.reset", "earliest");

        int checkpointInterval = tool.getInt("checkpoint.interval", 5000);
        String checkpointPath = tool.get("checkpoint.path","file:/Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_Demo/src/main/java/com/lxf/data/checkpoint/");


        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend(checkpointPath));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                Time.of(5, TimeUnit.SECONDS) // 间隔
        ));

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",servers);
        prop.setProperty("group.id",groupId);
        prop.setProperty("enable.auto.commit", autoCommit);
        prop.setProperty("auto.offset.reset", offsetRest);

        return env.addSource(new FlinkKafkaConsumer<T>(topic,deser.newInstance(),prop));
    }

    //通过范型封装kafka反序列化schema，使他变得更加通用
    public static <T> DataStream<T> createKafkaStreamV3(String[] arg,Class<? extends KafkaDeserializationSchema<T>> deser) throws Exception {
        ParameterTool tool = ParameterTool.fromPropertiesFile(arg[0]); //将配置文件加载到configurations里面去
        String groupId = tool.get("group.id", "sftest01");//获取groupid，没有的话就传一个默认值 这是可填的
        String servers = tool.getRequired("bootstrap.servers"); //这是必填的
        //如果获取多个topic需要进行分割后放入list集合中
        //List<String> topics = Arrays.asList(tool.getRequired("kafka.input.topics").split(","));
        String topic = tool.getRequired("kafka.input.topics");
        String autoCommit = tool.get("enable.auto.commit", "false");
        String offsetRest = tool.get("auto.offset.reset", "earliest");

        int checkpointInterval = tool.getInt("checkpoint.interval", 5000);
        String checkpointPath = tool.get("checkpoint.path","file:/Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_Demo/src/main/java/com/lxf/data/checkpoint/");


        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend(checkpointPath));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                Time.of(5, TimeUnit.SECONDS) // 间隔
        ));

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",servers);
        prop.setProperty("group.id",groupId);
        prop.setProperty("enable.auto.commit", autoCommit);
        prop.setProperty("auto.offset.reset", offsetRest);

        return env.addSource(new FlinkKafkaConsumer<T>(topic,deser.newInstance(),prop));
    }

}
