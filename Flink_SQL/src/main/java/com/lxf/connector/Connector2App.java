package com.lxf.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class Connector2App {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()//声明流任务
                //.inBatchMode() //声明批任务
                .build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        String sourcetab = "create table usertab(" +
                "timestamps BIGINT," +
                "name String," +
                "counts Int" +
                ")with(" +
                "'connector' = 'filesystem'," +
                "'path' = 'file:///Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_BasedApi/src/main/java/com/lxf/data/1.txt'," +
                "'format' = 'csv'" +
                ")";
        String sinktab = "create table user_insert_tab(" +
                "name String," +
                "counts Int" +
                ")with(" +
                "'connector' = 'print'" +
                ")";
        String sql = "insert into user_insert_tab select name,sum(counts) as counts from usertab group by name";
        String sql1 = "select name,sum(counts) as counts from usertab group by name";
        tableEnvironment.executeSql(sourcetab);
        tableEnvironment.executeSql(sinktab);
        tableEnvironment.executeSql(sql);
        tableEnvironment.sqlQuery(sql1).getQueryOperation();
        //tableEnvironment.sqlQuery(sql1).printSchema(); //会先打印表结构

        /**
         * connector声明了print，那么他就不能作为sorce表去查询
         */

    }
}
