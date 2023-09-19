package com.lxf.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class ConnectotApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableenv = StreamTableEnvironment.create(env);
        // BatchTableEnvironment tableenv = BatchTableEnvironment.create(env);
        // querywhitsql(tableenv);
        querywhittableapi(tableenv);
        env.execute();
    }

    // 通过tableapi的方式查询动态表
    private static void querywhittableapi(StreamTableEnvironment tableenv) {
        tableenv.connect(new FileSystem().path("/Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_BasedApi/src/main/java/com/lxf/data/1.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("timestamps", DataTypes.BIGINT())
                        .field("name", DataTypes.STRING())
                        .field("counts", DataTypes.INT())
                ).createTemporaryTable("usertab");
        Table usertab = tableenv.from("usertab");
        Table table = usertab.groupBy("name")
                .aggregate($("counts").sum().as("counts"))
                .select($("name"), $("counts"));
        tableenv.toRetractStream(table,Row.class).print();

    }
    // 通过sql的方式查询动态表
    private static void querywhitsql(StreamTableEnvironment tableenv) {
        String sourcetab = "create table usertab(" +
                "timestamps BIGINT," +
                "name String," +
                "counts Int" +
                ")with(" +
                "'connector' = 'filesystem'," +
                "'path' = 'file:///Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_BasedApi/src/main/java/com/lxf/data/1.txt'," +
                "'format' = 'csv'" +
                ")";
        String sql = "select name,sum(counts) as counts from usertab group by name";
        tableenv.executeSql(sourcetab);
        Table table = tableenv.sqlQuery(sql);

        tableenv.toRetractStream(table, Row.class).print();
    }

}
