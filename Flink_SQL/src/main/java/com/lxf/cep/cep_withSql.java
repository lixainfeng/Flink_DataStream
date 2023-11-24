package com.lxf.cep;

import com.lxf.utils.convert_time;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

//求一支股票连续下降区间
public class cep_withSql {
    public static void main(String[] args)  throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Tuple3<String, Long, Double>> stream = env.fromElements(
                "ACME,01-Apr-11 10:00:00,12",
                "ACME,01-Apr-11 10:00:01,17",
                "ACME,01-Apr-11 10:00:02,19",
                "ACME,01-Apr-11 10:00:03,21",
                "ACME,01-Apr-11 10:00:04,25",
                "ACME,01-Apr-11 10:00:05,18",
                "ACME,01-Apr-11 10:00:06,15",
                "ACME,01-Apr-11 10:00:07,14",
                "ACME,01-Apr-11 10:00:08,24",
                "ACME,01-Apr-11 10:00:09,25",
                "ACME,01-Apr-11 10:00:10,19"
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((element, timestamp) -> convert_time.change_time(element.split(",")[1]))  //新版本写法
        ).map(new MapFunction<String, Tuple3<String, Long, Double>>() {
            @Override
            public Tuple3<String, Long, Double> map(String value) throws Exception {
                String[] s = value.split(",");
                String symbol = s[0];
                Long rowtime = convert_time.change_time(s[1]);
                Double price = Double.parseDouble(s[2]);
                return Tuple3.of(symbol, rowtime, price);
            }
        });

        tableEnv.createTemporaryView("Ticker",stream,$("symbol"),$("rowtime").rowtime(), $("price"));
        String sql = "SELECT symbol," +
                "timestampadd(hour,8,start_tstamp)as start_tstamp," +
                "timestampadd(hour,8,bottom_tstamp)as bottom_tstamp," +
                "timestampadd(hour,8,end_tstamp)as end_tstamp\n" +
                "FROM Ticker\n" +
                "    MATCH_RECOGNIZE (\n" +
                "        PARTITION BY symbol\n" +
                "        ORDER BY rowtime\n" +
                "        MEASURES\n" +
                "            START_ROW.rowtime AS start_tstamp,\n" +
                "            LAST(PRICE_DOWN.rowtime) AS bottom_tstamp,\n" +
                "            LAST(PRICE_UP.rowtime) AS end_tstamp\n" +
                "        ONE ROW PER MATCH\n" +
                "        AFTER MATCH SKIP TO LAST PRICE_UP\n" +
                "        PATTERN (START_ROW PRICE_DOWN+ PRICE_UP)\n" +
                "        DEFINE\n" +
                "            PRICE_DOWN AS\n" +
                "                (LAST(PRICE_DOWN.price, 1) IS NULL AND PRICE_DOWN.price < START_ROW.price) OR\n" +
                "                    PRICE_DOWN.price < LAST(PRICE_DOWN.price, 1),\n" +
                "            PRICE_UP AS\n" +
                "                PRICE_UP.price > LAST(PRICE_DOWN.price, 1)\n" +
                "    ) MR";
        Table table = tableEnv.sqlQuery(sql);
        tableEnv.toRetractStream(table, Row.class).filter(x -> x.f0).print();
        env.execute();
    }
}
