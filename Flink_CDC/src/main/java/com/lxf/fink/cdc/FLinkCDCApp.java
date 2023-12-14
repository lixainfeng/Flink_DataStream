package com.lxf.fink.cdc;


import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class FLinkCDCApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
        env.setParallelism(1);
        /**
         * .startupOptions(StartupOptions.latest()) 参数配置
         *   1.initial() 全量扫描并且继续读取最新的binlog 最佳实践是第一次使用这个
         *   2.earliest() 从binlog的开头开始读取 就是啥时候开的binlog就从啥时候读
         *   3.latest() 从最新的binlog开始读取
         *   4.specificOffset(String specificOffsetFile, int specificOffsetPos) 指定offset读取
         *   5.timestamp(long startupTimestampMillis) 指定时间戳读取
         *
         */
        DebeziumSourceFunction<String> mySqlSource = MySQLSource.<String>builder()
                .hostname("60.204.204.25")
                .port(3306)
                .databaseList("test") // set captured database
                .tableList("test.test1") // set captured table
                .username("root")
                .password("123456")
                .startupOptions(StartupOptions.latest()) //初始化操作，可以定义数据从哪里开始读取。默认重启任务读取全量数据
                //.deserializer(new StringDebeziumDeserializationSchema()) // 反序列化器，可以定义输出json格式
                .deserializer(new CustomDebeziumDeserializationSchema()) //自定义反序列化器
                .build();
        env.addSource(mySqlSource).print();
        env.execute();

        /**
         * 打包好的jar需要提前放在lib目录下
         * flink集群启动命令：
         * bin/flink run -c com.lxf.fink.cdc.FLinkCDCApp ./../lib/flink-cdc-jar-with-dependencies.jar
         * ./flink run -c com.lxf.fink.cdc.FLinkCDCApp -s /usr/local/soft/flink-checkpoints/c7997db3f4a3dcbe78403dd7211481a3/chk-23 ./../lib/flink-cdc-jar-with-dependencies.jar
         */
    }
}
