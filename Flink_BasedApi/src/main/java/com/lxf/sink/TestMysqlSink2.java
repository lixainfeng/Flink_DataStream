package com.lxf.sink;

import com.lxf.utils.MYSQLUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class TestMysqlSink2 extends RichSinkFunction<Tuple2<String, Integer>> {
    Connection conn;
    PreparedStatement updatepsmt;
    PreparedStatement insertpsmt;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = MYSQLUtils.getConnection("test");
        insertpsmt = conn.prepareStatement("insert into test1(name,count) values(?,?)");
        updatepsmt = conn.prepareStatement("update test1 set count = ? where name = ?");
    }

    @Override
    public void close() throws Exception {
        if(insertpsmt != null) insertpsmt.close();
        if(updatepsmt != null) updatepsmt.close();
        if(conn != null) conn.close();
    }

    @Deprecated
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        System.out.println("=====invoke====="+value.f0+"==>"+value.f1);
        updatepsmt.setInt(1, value.f1);
        updatepsmt.setString(2, value.f0);
        updatepsmt.execute();

        if(updatepsmt.getUpdateCount() == 0){ //获取当前结果的更新计数
            insertpsmt.setString(1, value.f0);
            insertpsmt.setInt(2, value.f1);
            insertpsmt.execute();
        }


    }
}
