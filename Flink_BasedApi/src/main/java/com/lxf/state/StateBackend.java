package com.lxf.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class StateBackend {
    public static void main(String[] args) throws Exception{
        /**
         * flink jar提交集群执行命令,将jar包放在lib目录下
         * 在bin目录下:./flink run -c com.lxf.state.StateBackend -s /Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_BasedApi/src/main/java/com/lxf/data/7256e98b3ec6fe887ee15fbedd91202a/chk-43 ～/lib/Flink_BasedApi-1.0-SNAPSHOT.jar
         */
        //Configuration configuration = new Configuration();
        //configuration.setString("execution.savepoint.path", "/Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_BasedApi/src/main/java/com/lxf/data/7256e98b3ec6fe887ee15fbedd91202a/chk-43/");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.enableCheckpointing(5000);
        // 确认 checkpoints 之间的时间会进行 500 ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);

        // Checkpoint 必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // 允许两个连续的 checkpoint 错误
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);

        // 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 使用 externalized checkpoints，这样 checkpoint 在作业取消后仍就会被保留
        /**
         * ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION: 当作业被取消时，保留外部的checkpoint。注意，在此情况下，您必须手动清理checkpoint状态
         * ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 当作业被取消时，删除外部化的checkpoint。只有当作业失败时，检查点状态才可用。
         */
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //env.setStateBackend(new FsStateBackend("file:/Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_BasedApi/src/main/java/com/lxf/data/checkpoint/"));


        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                Time.of(5, TimeUnit.SECONDS) // 间隔
        ));


        test01(env);
        env.execute();
    }

    private static void test01(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("110.40.173.142", 9633);
        source.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                if(s.contains("DK")){
                    throw new RuntimeException("ERROR....");
                }else {
                    return s.toLowerCase();
                }
            }
        }).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(",");
                for (String s1 : split) {
                    collector.collect(s1);
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s,1);
            }
        }).keyBy(x -> x.f0)
                .sum(1)
                .print();
    }
}
