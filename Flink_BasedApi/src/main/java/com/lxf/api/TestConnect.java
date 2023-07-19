package com.lxf.api;

import com.lxf.model.Student;
import com.lxf.source.TestSourceFun;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class TestConnect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Student> stream1 = env.addSource(new TestSourceFun());
        DataStreamSource<Student> stream2 = env.addSource(new TestSourceFun());

        SingleOutputStreamOperator<Tuple2<String, Student>> stream3 = stream2.map(new MapFunction<Student, Tuple2<String, Student>>() {
            @Override
            public Tuple2<String, Student> map(Student student) throws Exception {
                return Tuple2.of("flink", student);
            }
        });

        /**
         * union 可以实现多个流之间的数据合并，但数据结构必须相同
         * conect 实现两个流之间合并，但数据结构可以不同，比较灵活。
         */
        ConnectedStreams<Student, Tuple2<String, Student>> connect = stream1.connect(stream3);

        connect.map(new CoMapFunction<Student, Tuple2<String, Student>, String>() {
            @Override
            public String map1(Student student) throws Exception {
                return student.toString();
            }

            @Override
            public String map2(Tuple2<String, Student> value) throws Exception {
                return value.f0 + "===>" +value.f1.toString();
            }
        }).print();
        /*connect.map(new CoMapFunction<Student, Student, String>() {
            //处理流1的逻辑
            @Override
            public String map1(Student student) throws Exception {
                return student.toString();
            }

            //处理流2的逻辑
            @Override
            public String map2(Student student) throws Exception {
                return student.getName();
            }
        }).print();*/


        env.execute();
    }
}
