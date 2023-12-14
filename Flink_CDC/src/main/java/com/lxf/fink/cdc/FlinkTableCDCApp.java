package com.lxf.fink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkTableCDCApp {
    public static void main(String[] args) throws  Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableenv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        tableenv.executeSql("create table mysql_bin(" +
                "id INT primary key, " +
                "name STRING" +
                ") with(" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = '60.204.204.25'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = '123456'," +
                "'database-name' = 'test'," +
                "'table-oname' = 'test1'" +
                ")"
        );
        Table table = tableenv.sqlQuery("select * from mysql_bin");
        tableenv.toRetractStream(table, Row.class).print();

        env.execute();
    }
}
