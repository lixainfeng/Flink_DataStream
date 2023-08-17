package com.lxf.app;

import com.alibaba.fastjson.JSON;
import com.lxf.model.Access;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


/**
 * 取topN
 */
public class TopN {
    public static void main(String[] args) throws java.lang.Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.readTextFile("/Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_Demo/src/main/java/com/lxf/data/access.log");
        SingleOutputStreamOperator<Access> cleanstream = stream.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) {
                try {
                    Access access = JSON.parseObject(value, Access.class);
                    return access;
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }).filter(x -> x != null)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Access>(Time.seconds(20)) {
                    @Override
                    public long extractTimestamp(Access element) {
                        return element.time; //取文件中字段time作为时间时间的时间戳
                    }
                })
                .filter(new FilterFunction<Access>() {
                    @Override
                    public boolean filter(Access value) throws Exception {
                        return !"startup".equals(value.event);
                    }
                });

        // todo --> tuple3<name,category,event>
        cleanstream.keyBy(new KeySelector<Access, Tuple3<String,String,String>>() {
            @Override
            public Tuple3<String, String, String> getKey(Access value) throws Exception {
                return Tuple3.of(value.event,value.product.category,value.product.name);
            }
        }).window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)));
        env.execute();
    }
}
