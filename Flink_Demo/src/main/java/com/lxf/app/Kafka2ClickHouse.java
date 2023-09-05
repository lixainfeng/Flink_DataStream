package com.lxf.app;

import com.alibaba.fastjson.JSON;
import com.lxf.kafka.FlinkUtils;
import com.lxf.kafka.SFKafkaDeserializationSchema;
import com.lxf.model.AccessV2;
import com.lxf.utils.ProvinceMapFunV2;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;

public class Kafka2ClickHouse {
    public static void main(String[] args) throws Exception {
        DataStream<Tuple2<String, String>> source = FlinkUtils.createKafkaStreamV3(args, SFKafkaDeserializationSchema.class);
        FastDateFormat instances = FastDateFormat.getInstance("yyyyMMdd-HH");
        source.map(new MapFunction<Tuple2<String, String>, AccessV2>() {
            @Override
            public AccessV2 map(Tuple2<String, String> value) throws Exception {
                AccessV2 accessV2 = JSON.parseObject(value.f1, AccessV2.class);
                accessV2.id = value.f0;
                long time = accessV2.time;
                String[] splits = instances.format(time).split("-");
                String day = splits[0];
                String hour = splits[1];
                accessV2.day = day;
                accessV2.hour = hour;
                return accessV2;
            }
        }).filter(x -> x != null)
                .filter(new FilterFunction<AccessV2>() {
                    @Override
                    public boolean filter(AccessV2 value) throws Exception {
                        return "startup".equals(value.event);
                    }
                })
                .map(new ProvinceMapFunV2())
                .addSink(JdbcSink.sink(
                "insert into sf_event  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (pstmt,x) -> {
                    pstmt.setString(1, x.id);
                    pstmt.setString(2, x.device);
                    pstmt.setString(3, x.deviceType);
                    pstmt.setString(4, x.os);
                    pstmt.setString(5, x.event);
                    pstmt.setString(6, x.net);
                    pstmt.setString(7, x.channel);
                    pstmt.setString(8, x.uid);
                    pstmt.setInt(9, x.nu);
                    pstmt.setString(10, x.ip);
                    pstmt.setLong(11, x.time);
                    pstmt.setString(12, x.version);
                    pstmt.setString(13, x.province);
                    pstmt.setString(14, x.city);
                    pstmt.setString(15, x.day);
                    pstmt.setString(16, x.hour);
                    pstmt.setLong(17, System.currentTimeMillis());
                },
                //设置每隔多少秒刷写一批数据进入clickhouse,不然按照默认批次写入，在结果表看不到数据
                JdbcExecutionOptions.builder().withBatchSize(50).withBatchIntervalMs(4000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://60.204.204.25:8123/sftest")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()
        ));
        FlinkUtils.env.execute();
    }
}
