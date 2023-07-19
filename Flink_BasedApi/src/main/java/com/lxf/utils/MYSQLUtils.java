package com.lxf.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MYSQLUtils {
    public static  String database;
    public static Connection getConnection(String database){
        try {
            Class.forName("com.mysql.jdbc.Driver");
            return DriverManager.getConnection("jdbc:mysql://localhost:3306/"+database, "root", "123456");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public static void close(Connection conn, PreparedStatement prep){
        if(conn != null){
            try {
                conn.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

        if(prep != null){
            try {
                prep.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }
}
