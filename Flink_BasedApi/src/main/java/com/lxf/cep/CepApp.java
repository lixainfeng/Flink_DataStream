package com.lxf.cep;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

//统计同一个ip连续出现两次登录失败的
public class CepApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.fromElements(
                "001,78.89.90.2,success,1622689918",
                "002,110.111.112.113,failure,1622689952",
                "002,110.111.112.113,failure,1622689953",
                "002,110.111.112.113,failure,1622689954",
                "002,193.114.45.13,success,1622689959",
                "002,137.49.24.26,failure,1622689958"
        );
        KeyedStream<Tuple4<String, String, String, Long>, String> stream1 = stream.map(new MapFunction<String, Tuple4<String, String, String, Long>>() {
            @Override
            public Tuple4<String, String, String, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                String id = split[0];
                String ip = split[1];
                String state = split[2];
                Long ts = Long.parseLong(split[3]);
                return Tuple4.of(id, ip, state, ts);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple4<String, String, String, Long>>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(Tuple4<String, String, String, Long> element) {
                return element.f3 * 1000;
            }
        }).keyBy(x -> x.f0);

        //定义匹配规则
        Pattern<Tuple4<String, String, String, Long>, Tuple4<String, String, String, Long>> pattern = Pattern.<Tuple4<String, String, String, Long>>begin("start")
                .where(new SimpleCondition<Tuple4<String, String, String, Long>>() {
                    @Override
                    public boolean filter(Tuple4<String, String, String, Long> value) throws Exception {
                        return value.f2.equals("failure");
                    }
                }).next("middle")
                .where(new SimpleCondition<Tuple4<String, String, String, Long>>() {
                    @Override
                    public boolean filter(Tuple4<String, String, String, Long> value) throws Exception {
                        return value.f2.equals("failure");
                    }
                }).within(Time.seconds(3));//指定在三秒内发生

        //把规则作用在数据流上
        PatternStream<Tuple4<String, String, String, Long>> patternStream = CEP.pattern(stream1, pattern);

        //提取符合规则的数据
        patternStream.select(new PatternSelectFunction<Tuple4<String, String, String, Long>, Tuple4<String, Long, Long, String>>() {
            @Override
            public Tuple4<String, Long, Long, String> select(Map<String, List<Tuple4<String, String, String, Long>>> map) throws Exception {
                Tuple4<String, String, String, Long> start = map.get("start").get(0);
                Tuple4<String, String, String, Long> last = map.get("middle").get(0);
                String userid = start.f0;
                Long first = start.f3;
                Long second = last.f3;
                String msg = "出现连续登录失败。。。";
                return Tuple4.of(userid,first,second,msg);
            }
        }).print();

        env.execute();

    }
}
