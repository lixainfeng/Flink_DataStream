package com.lxf.TableAndSql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class TableAndSqlTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableenv = StreamTableEnvironment.create(env);
        TupleFun(env, tableenv);
        env.execute("TableAndSqlTest");
    }

    private static void TupleFun(StreamExecutionEnvironment env, StreamTableEnvironment tableenv) {
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> stream =
                env.readTextFile("/Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_BasedApi/src/main/java/com/lxf/data/1.txt")
                .map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
                    @Override
                    public Tuple3<Long, String, Integer> map(String value) throws Exception {
                        String[] values = value.split(",");
                        Long time = Long.parseLong(values[0]);
                        String name = values[1];
                        Integer count = Integer.parseInt(values[2]);
                        return Tuple3.of(time, name, count);
                    }
                });
        Table table = tableenv.fromDataStream(stream,$("time"),$("name"),$("count"));
        tableenv.createTemporaryView("access",table);
        Table query = tableenv.sqlQuery("select name,sum(conut) as counts from access group by name");
        tableenv.toRetractStream(query, Row.class).filter(x -> x.f0).print();
    }
    private static void RowFun(StreamExecutionEnvironment env, StreamTableEnvironment tableenv) {
        env.readTextFile("/Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_BasedApi/src/main/java/com/lxf/data/1.txt")
                .map(new MapFunction<String, Row>() {
                    @Override
                    public Row map(String value) throws Exception {
                        String[] values = value.split(",");
                        Long time = Long.parseLong(values[0]);
                        String name = values[1];
                        Integer count = Integer.parseInt(values[2]);
                        return Row.of(time,name,count);
                    }
                });
    }
}
