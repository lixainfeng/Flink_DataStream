package com.lxf.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lxf.utils.StringTimeToTimeStamp;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * udaf：多行输入一行输出,聚合函数，分为聚合函数和表聚合函数
 * 聚合函数：继承AggregateFunction,把一个表（一行或者多行，每行可以有一列或者多列）聚合成一个标量值
 * 表聚合函数：TableAggregateFunction,把一个表（一行或者多行，每行有一列或者多列）聚合成另一张表，结果中可以有多行多列
 * 计算5秒窗口内平均每人消费情况
 */
public class UdafApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableenv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple5<String, Long, String, String, Double>> source
                = env.readTextFile("/Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_SQL/src/main/java/com/lxf/data/1.txt")
                .map(new MapFunction<String, Tuple5<String, Long, String, String, Double>>() {
                    @Override
                    public Tuple5<String, Long, String, String, Double> map(String value) throws Exception {
                        JSONObject json = JSON.parseObject(value);
                        String userid = json.getString("userID");
                        Long timestamps = new StringTimeToTimeStamp().strtime_timestamp(json.getString("eventTime"));
                        String times = json.getString("eventTime");
                        String productid = json.getString("productID");
                        Double price = Double.parseDouble(json.getString("productPrice"));
                        return Tuple5.of(userid, timestamps, times, productid, price);
                    }
                });
        SingleOutputStreamOperator<Tuple5<String, Long, String, String, Double>> stream
                = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple5<String, Long, String, String, Double>>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Tuple5<String, Long, String, String, Double> value) {
                return value.f1;
            }
        });
        /**
         * 在创建临时试图的时候需要注意两点
         * 1、流中元素位置要匹配，必须一一对应，中间不能少字段
         * 2、rowtime可以指定流中time或者timestamp类型字段，且位置也要对应。这里的rowtime实际上就是事件时间
         */
        tableenv.createTemporaryView("model",stream,$("userid"),$("timestamps").rowtime(),$("times"),$("productid"),$("price"));
        tableenv.createTemporaryFunction("avg_price",new UdafWithSum());
        String sql = "select " +
                "TUMBLE_START(timestamps,interval '5' second) as win_start," +
                "TUMBLE_END(timestamps,interval '5' second) as win_end," +
                "sum(price) as moneys, " +
                "count(userid) as counts, " +
                "avg_price(price) as avgs " +
                "from model group by TUMBLE(timestamps,interval '5' second)";
        Table table = tableenv.sqlQuery(sql);
        tableenv.toRetractStream(table, Row.class).filter(x->x.f0).print();

        env.execute();
    }
}
