package com.lxf.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class WinWithTabApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableenv = StreamTableEnvironment.create(env);

        /**
         * 五秒一个窗口按照名称分组统计消费总数
         */
        DataStreamSource<String> source = env.fromElements(
                //time name book pay
                "1000,sf,spark,35",
                "2000,zy,hive,36",
                "4000,sf,spark,35",
                "6000,zy,flink,42",
                "8000,sf,hadoop,48",
                "10000,zy,cdh,29"
        );
        SingleOutputStreamOperator<Tuple4<Long, String, String, Double>> stream = source.map(new MapFunction<String, Tuple4<Long, String, String, Double>>() {
            @Override
            public Tuple4<Long, String, String, Double> map(String value) throws Exception {
                String[] split = value.split(",");
                Long times = Long.parseLong(split[0]);
                String name = split[1];
                String book = split[2];
                Double money = Double.parseDouble(split[3]);
                return Tuple4.of(times, name, book, money);
            } //设置水位线，允许延迟触发窗口为0秒
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple4<Long, String, String, Double>>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Tuple4<Long, String, String, Double> value) {
                return value.f0;
            }
        });
        //TableApiWithWindow(tableenv, stream);
        SqlWithWindow(tableenv, stream);

        env.execute();
    }

    private static void TableApiWithWindow(StreamTableEnvironment tableenv, SingleOutputStreamOperator<Tuple4<Long, String, String, Double>> stream) {
        //将流注册成一张表
        Table table = tableenv.fromDataStream(stream, $("times"),$("name"), $("book"), $("money"), $("rowtimes").rowtime())
                .window(Tumble.over(lit(5).seconds())
                        .on($("rowtimes"))//这里指定的是eventTime
                        .as("wintime")
                ).groupBy($("name"), $("wintime"))
                .select($("name"), $("money").sum().as("total"), $("wintime").start(), $("wintime").end());

        tableenv.toRetractStream(table, Row.class).filter(x -> x.f0).print();
    }
    private static void SqlWithWindow(StreamTableEnvironment tableenv, SingleOutputStreamOperator<Tuple4<Long, String, String, Double>> stream) {
        //将流注册成一张表
        tableenv.createTemporaryView("shopping",stream, $("times"),$("name"), $("book"), $("money"), $("rowtimes").rowtime());
           Table table =  tableenv.sqlQuery("select " +
                    "TUMBLE_START(rowtimes,interval '5' second) as win_start," +
                    "TUMBLE_END(rowtimes,interval '5' second) as win_end," +
                    "name," +
                    "sum(money) as moneys " +
                    "from shopping group by TUMBLE(rowtimes,interval '5' second),name");

        tableenv.toRetractStream(table, Row.class).filter(x -> x.f0).print();
    }
}
