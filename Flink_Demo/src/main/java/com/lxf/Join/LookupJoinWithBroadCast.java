package com.lxf.Join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class LookupJoinWithBroadCast {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> user_stream = env.socketTextStream("localhost", 3456)
                .map(new MapFunction<String, Tuple3<String, String, Long>>() { //（用户名称，id,事件戳）
                    @Override
                    public Tuple3<String, String, Long> map(String value) throws Exception {
                        String[] split = value.split(",");
                        String user = split[0];
                        String c_id = split[1];
                        Long c_time = Long.parseLong(split[2]);
                        return Tuple3.of(user,c_id,c_time);
                    }
                });
        SingleOutputStreamOperator<Tuple2<String, String>> city_stream = env.socketTextStream("localhost", 3457)
                .map(new MapFunction<String, Tuple2<String, String>>() { //（城市id，名称）
                    @Override
                    public Tuple2<String, String> map(String value) throws Exception {
                        String[] split = value.split(",");
                        String c_id = split[0];
                        String c_name = split[1];
                        return Tuple2.of(c_id, c_name);
                    }
                });
        MapStateDescriptor descriptor = new MapStateDescriptor("broad1", String.class, String.class);
        BroadcastStream<Tuple2<String, String>> broadcastStream = city_stream.broadcast(descriptor); //将city_stream进行广播

        SingleOutputStreamOperator<Tuple3<String, String, String>> process = user_stream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<Tuple3<String, String, Long>, Tuple2<String, String>, Tuple3<String, String, String>>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> value, ReadOnlyContext ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(descriptor);
                        String c_name = "";
                        if (broadcastState.contains(value.f1)) {
                            c_name = broadcastState.get(value.f1);
                        }
                        out.collect(Tuple3.of(value.f0, value.f1, c_name));
                    }

                    @Override
                    public void processBroadcastElement(Tuple2<String, String> value, Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
                        System.out.println("收到广播数据。。。。");
                        ctx.getBroadcastState(descriptor).put(value.f0, value.f1);
                    }
                });

        process.print();


        env.execute();
    }
}
