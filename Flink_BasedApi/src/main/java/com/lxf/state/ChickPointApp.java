package com.lxf.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class ChickPointApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * 不开启checkpoint：不重启
         *
         * 开启了checkpoint
         * 1) 没有配置重启策略重试次数：Integer.MAX_VALUE
         * 2) 如果配置了重启策略，就使用我们配置的重启策略覆盖默认的
         *
         * 重启策略的配置方式：
         * 1) 在code里
         * 2) 在flink_conf.yaml配置文件里
         */
        //开启checkpoint 默认是exactly once模式
        env.enableCheckpointing(5000);

        // 自定义设置我们需要的重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                Time.of(5, TimeUnit.SECONDS) // 间隔
        ));

        /**
         * MemoryStateBackend
         * key/value形式存储在task memory中
         * 默认情况下，每个独立的状态大小限制是 5 MB
         * 聚合后的状态必须能够放进 JobManager 的内存中
         * 场景适用于本地开发，状态很小的 Job
         * 异步快照默认是开启的,false 来关闭异步快照
         */
        //new MemoryStateBackend(10,false);

        /**
         * FsStateBackend
         * FsStateBackend 将正在运行中的状态数据保存在 TaskManager 的内存中。CheckPoint 时，将状态快照写入到配置的文件系统目录中。
         * 少量的元数据信息存储到 JobManager 的内存中（高可用模式下，将其写入到 CheckPoint 的元数据文件中）
         * 异步快照默认是开启的,false 来关闭异步快照
         * 适用于状态比较大、窗口比较长、key/value 状态比较大的 Job及所有高可用的场景
         */
        //new FsStateBackend("path", false);

        /**
         * RocksDBStateBackend
         * RocksDBStateBackend 将正在运行中的状态数据保存在 RocksDB 数据库中，RocksDB 数据库默认将数据存储在 TaskManager 的数据目录。
         * CheckPoint 时，整个 RocksDB 数据库被 checkpoint 到配置的文件系统目录中。 少量的元数据信息存储到 JobManager 的内存中
         * （高可用模式下，将其存储到 CheckPoint 的元数据文件中）
         * RocksDBStateBackend 只支持异步快照
         * 状态非常大、窗口非常长、key/value状态非常大的Job及所有高可用的场景
         * 使用 RocksDBStateBackend 将会使应用程序的最大吞吐量降低。 所有的读写都必须序列化、反序列化操作，这个比基于堆内存的 state backend 的效率要低很多
         * RocksDBStateBackend 是目前唯一支持增量 CheckPoint 的 State Backend
         */
        test02(env);

        env.execute();

    }

    private static void test01(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9633);
        source.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if(value.contains("pk")){
                    throw new RuntimeException("ERROR....");
                }else {
                    return value.toLowerCase();
                }
            }
        }).print();
    }

    private static void test02(StreamExecutionEnvironment env){
        DataStreamSource<String> source = env.socketTextStream("localhost", 9633);
        source.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                if(s.contains("pk")){
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
