package com.lxf.sink;

import com.lxf.model.Access;
import com.lxf.utils.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * 需求分析：
 * 1、通过读取本地文件，进行wordcount操作
 * 2、将最终结果sink到mysql库中
 * 3、要求mysql结果没有重复值（如果有数据就更新，没有数据就插入）
 */
public class TestMysqlSink extends RichSinkFunction<Access> {
    Connection conn;
    PreparedStatement updatepsmt;
    PreparedStatement insertpsmt;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = MysqlUtil.getConnection();
        insertpsmt = conn.prepareStatement("insert into test2(name,count) values(?,?)");
        updatepsmt = conn.prepareStatement("update test2 set count = ? where name = ?");
    }

    @Override
    public void close() throws Exception {
        if(insertpsmt != null) insertpsmt.close();
        if(updatepsmt != null) updatepsmt.close();
        if(conn != null) conn.close();
    }

    @Override
    public void invoke(Access value, Context context) throws Exception {
        System.out.println("=====invoke====="+value.getName()+"==>"+value.getConut());
        updatepsmt.setLong(1, value.getConut());
        updatepsmt.setString(2, value.getName());
        updatepsmt.execute();

        if(updatepsmt.getUpdateCount() == 0){
            insertpsmt.setString(1, value.getName());
            insertpsmt.setLong(2, value.getConut());
            insertpsmt.execute();
        }


    }
}
