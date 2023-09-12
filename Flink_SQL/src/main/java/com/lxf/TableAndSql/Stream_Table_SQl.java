package com.lxf.TableAndSql;

import com.lxf.model.Access;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Stream_Table_SQl {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableenv = StreamTableEnvironment.create(env);
        SingleOutputStreamOperator<Access> stream = env.readTextFile("/Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_BasedApi/src/main/java/com/lxf/data/1.txt")
                .map(new MapFunction<String, Access>() {
                    @Override
                    public Access map(String s) throws Exception {
                        String[] value = s.split(",");
                        Long time = Long.parseLong(value[0]);
                        String name = value[1];
                        Integer count = Integer.parseInt(value[2]);
                        return new Access(time, name, count);
                    }
                });
        //datastream ==> table
        Table table = tableenv.fromDataStream(stream);
        tableenv.createTemporaryView("access",table);
        //通过sql方式查询table
        //Table query = tableenv.sqlQuery("select * from access where name= 'wangwu'");

        Table query = tableenv.sqlQuery("select name,sum(conut) as counts from access group by name");

        //通过table api的方式查询table
       // Table query = table.select("*").where("domain='imooc.com'");
       // Table query = table.select($("name"),$("conut"));

        // table ==> datastream
       // tableenv.toAppendStream(query, Row.class).print();
        //tableenv.toAppendStream(query, Access.class).print("access:");

        tableenv.toRetractStream(query, Row.class).filter(x -> x.f0).print();

        /**
         * 数据要做聚合操作时有两种方式：
         * append模式：只支持insert操作，之前输出的结果不会做更新
         * retract模式：任何情况下都可以使用，可以对数据做更新
         *  retract下：对过期的数据（标识fales的数据）做回撤操作
         */

        env.execute("Stream_Table_SQl");
    }
}
