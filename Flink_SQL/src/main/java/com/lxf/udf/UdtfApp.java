package com.lxf.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * udtf:表值函数，输入一行通过拆分返回多列数据
 *
 * 传入一个json，通过表值函数解析成表字段
 */
public class UdtfApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableenv = StreamTableEnvironment.create(env);
        SingleOutputStreamOperator<Tuple4<String, String, String, Integer>> source
                = env.readTextFile("/Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_SQL/src/main/java/com/lxf/data/1.txt")
                .map(new MapFunction<String, Tuple4<String, String, String, Integer>>() {
                    @Override
                    public Tuple4<String, String, String, Integer> map(String value) throws Exception {
                        JSONObject json = JSON.parseObject(value);
                        String userid = json.getString("userID");
                        String times = json.getString("eventTime");
                        String productid = json.getString("productID");
                        Integer price = Integer.parseInt(json.getString("productPrice"));
                        return Tuple4.of(userid, times, productid, price);
                    }
                });
        tableenv.createTemporaryView("model",source,$("userid"),$("times"),$("productid"),$("price"));
        tableenv.createTemporaryFunction("split_filed",new UdtfWithFiledPaser());
        Table table = tableenv.sqlQuery("select times,days,hours from model, lateral table(split_filed(times)) as t(days,hours)");
        tableenv.toAppendStream(table, Row.class).print();

        env.execute();

    }
}
