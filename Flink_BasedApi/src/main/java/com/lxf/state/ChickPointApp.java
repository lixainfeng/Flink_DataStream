package com.lxf.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class ChickPointApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * 不开启checkpoint：不重启
         *
         * 开启了checkpoint
         * 1) 没有配置重启策略：Integer.MAX_VALUE
         * 2) 如果配置了重启策略，就使用我们配置的重启策略覆盖默认的
         *
         * 重启策略的配置：
         * 1) code
         * 2) yaml
         */
        //开启checkpoint 默认是exactly once模式
        env.enableCheckpointing(5000);

        // 自定义设置我们需要的重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                Time.of(5, TimeUnit.SECONDS) // 间隔
        ));


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

        env.execute();

    }
}
