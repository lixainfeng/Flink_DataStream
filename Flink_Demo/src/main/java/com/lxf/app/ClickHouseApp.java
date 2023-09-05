package com.lxf.app;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.jdbc.JdbcSink;


public class ClickHouseApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("localhost",9527)
                .map(new MapFunction<String, Tuple3<String,String,String>>() {
                    @Override
                    public Tuple3<String, String, String> map(String value) throws Exception {
                        String[] split = value.split(",");
                        return Tuple3.of(split[0].trim(),split[1].trim(),split[2].trim());
                    }
                })
                .addSink(JdbcSink.sink(
                        "insert into sf_test1 values (?,?,?)",
                (ps,t) -> {
                    ps.setString(1,t.f0);
                    ps.setString(2,t.f1);
                    ps.setString(3,t.f2);
                },
                //设置每隔多少秒刷写一批数据进入clickhouse,不然按照默认批次写入，在结果表看不到数据
                JdbcExecutionOptions.builder().withBatchSize(3).withBatchIntervalMs(4000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://60.204.204.25:8123/sftest")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()
                ));
        env.execute();
    }
}
