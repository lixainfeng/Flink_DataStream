package com.lxf.udf;

import com.lxf.utils.GetRandomIp;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * udf:标量函数，输入一行返回一行
 * 随机输入10个ip解析：省份-城市
 */
public class UdfApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableenv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<String> source = env.fromCollection(GetRandomIp.getRandomIp())
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return value.toString().trim();
                    }
                });

        tableenv.createTemporaryView("model",source,$("ip"));
        tableenv.createTemporaryFunction("ip_parser",new UdfWithIpPaser());
        Table table = tableenv.sqlQuery("select ip, ip_parser(ip) from model");
        tableenv.toAppendStream(table, Row.class).print();

        env.execute();
    }
}
