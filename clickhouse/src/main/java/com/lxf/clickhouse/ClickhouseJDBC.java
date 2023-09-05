package com.lxf.clickhouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ClickhouseJDBC {
    public static void main(String[] args) throws Exception{
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String url = "jdbc:clickhouse://60.204.204.25:8123/sftest";
        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery("select * from sf_log;");
        while(resultSet.next()){
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            System.out.println(id +"===="+name);
        }
        resultSet.close();
        stmt.close();
        conn.close();
    }
}
