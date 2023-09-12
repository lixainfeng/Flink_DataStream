package com.lxf.app;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.util.List;
import java.util.Map;


/**
 * 模式检测
 * 检测数据流中出现连续登录失败的用户
 */
public class CEPApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KeyedStream<model, String> stream = env.fromElements("001,78.89.90.2,success,1622689918",
                "002,110.111.112.113,failure,1622689952",
                "002,110.111.112.113,failure,1622689953",
                "002,110.111.112.113,failure,1622689954",
                "002,193.114.45.13,success,1622689959",
                "002,137.49.24.26,failure,1622689958")
                .map(new MapFunction<String, model>() {
                    @Override
                    public model map(String value) throws Exception {
                        model model = new model();
                        String[] split = value.split(",");
                        model.id = split[0];
                        model.ip = split[1];
                        model.state = split[2];
                        model.times = Long.parseLong(split[3]);
                        return model;
                    }
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<model>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(model element) {
                        return element.times * 1000;
                    }
                }).keyBy(x -> x.id);
        Pattern<model, model> pattern = Pattern.<model>begin("start").where(
                new SimpleCondition<model>() {
                    @Override
                    public boolean filter(model value) throws Exception {
                        return value.state.equals("failure");
                    }
                }
        ).next("middle").where( //定义严格连续
                new SimpleCondition<model>() {
                    @Override
                    public boolean filter(model value) throws Exception {
                        return value.state.equals("failure");
                    }
                }
        ).within(Time.seconds(3));
        PatternStream<model> stream1 = CEP.pattern(stream, pattern);
        stream1.select(new PatternSelectFunction<model, model_msg>() {
            @Override
            public model_msg select(Map<String, List<model>> map) throws Exception {
                model start = map.get("start").get(0);
                model next = map.get("middle").get(0);
                model_msg model_msg = new model_msg();
                model_msg.id = start.id;
                model_msg.first = start.times;
                model_msg.second = next.times;
                model_msg.msg = "连续登录两次。。。";
                return model_msg;
            }
        }).print();

        env.execute();
    }

    public static class model{

        @Override
        public String toString() {
            return "model{" +
                    "id='" + id + '\'' +
                    ", ip='" + ip + '\'' +
                    ", state='" + state + '\'' +
                    ", times=" + times +
                    '}';
        }

        public  String id;
        public  String ip;
        public  String state;
        public  Long times;
    }

    public static class model_msg{

        @Override
        public String toString() {
            return "model{" +
                    "id='" + id + '\'' +
                    ", ip='" + first + '\'' +
                    ", state='" + second + '\'' +
                    ", times=" + msg +
                    '}';
        }

        public  String id;
        public  Long first;
        public  Long second;
        public  String msg;
    }
}
