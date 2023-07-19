package com.lxf.partition;

import com.lxf.model.Student;
import com.lxf.source.TestParallelSounrceFun;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestMainPartition {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Student> studentDataStreamSource = env.addSource(new TestParallelSounrceFun());
        env.setParallelism(3);
        studentDataStreamSource.map(new MapFunction<Student, Tuple2<String,Student>>() {
            @Override
            public Tuple2<String, Student> map(Student student) throws Exception {
                return Tuple2.of(student.getName(),student);
            }
        }).partitionCustom(new TestPartition(),0)//根据下标为0的字段进行分区？
                .map(new MapFunction<Tuple2<String, Student>, Student>() {
                    @Override
                    public Student map(Tuple2<String, Student> value) throws Exception {
                        System.out.println("current thread id is:" + Thread.currentThread().getId()+ ", value is:" + value.f1);
                        return value.f1;
                    }
                }).print();

        env.execute();


    }
}
