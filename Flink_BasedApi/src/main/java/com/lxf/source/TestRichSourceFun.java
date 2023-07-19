package com.lxf.source;

import com.lxf.model.Student;
import com.lxf.utils.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 抽象类RichSourceFunction继承自AbstractRichFunction可以实现生命周期方法，
 * 实现了SourceFunction接口，所以该类只有单并行度。
 */
public class TestRichSourceFun extends RichSourceFunction<Student> {

    Connection conn;
    PreparedStatement prep;

    /**
     *初始化方法，也称生命周期方法
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        conn = MysqlUtil.getConnection();
        prep = conn.prepareStatement("select  * from student");
        System.out.println("----open----");//标识
    }


    @Override
    public void close() throws Exception {
        MysqlUtil.close(conn,prep);
    }

    @Override
    public void run(SourceContext<Student> sourceContext) throws Exception {
        ResultSet rs = prep.executeQuery();
        while(rs.next()){
            int id = rs.getInt("id");
            String name = rs.getString("name");
            int age = rs.getInt("age");
            sourceContext.collect(new Student(id,name,age));
        }
    }

    @Override
    public void cancel() {

    }
}
