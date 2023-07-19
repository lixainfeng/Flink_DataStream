package com.lxf.source;

import com.lxf.model.Student;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class sourceMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        test01(env);
        //test02(env);
        //test03(env);
        //test04(env);
        env.execute();
    }

    /**
     *测试SourceFunction
     */
    public static void test01(StreamExecutionEnvironment env){
        DataStreamSource<Student> studentDataStreamSource = env.addSource(new TestSourceFun());
        studentDataStreamSource.print();
        env.setParallelism(2);
        System.out.println("全局并行度为："+env.getParallelism());
        System.out.println("当前SourceFunction的并行度为："+studentDataStreamSource.getParallelism());//打印当前SourceFunction的并行度
    }

    /**
     *测试RichSourceFunction
     */
    public static void test02(StreamExecutionEnvironment env){
        DataStreamSource<Student> studentDataStreamSource = env.addSource(new TestRichSourceFun());
        studentDataStreamSource.print();
        System.out.println(studentDataStreamSource.getParallelism());//打印当前SourceFunction的并行度
    }

    /**
     *测试TestParallelSounrceFun
     */
    public static void test03(StreamExecutionEnvironment env){
        DataStreamSource<Student> studentDataStreamSource = env.addSource(new TestParallelSounrceFun());
        studentDataStreamSource.setParallelism(4);
        studentDataStreamSource.print();
        System.out.println(studentDataStreamSource.getParallelism());//打印当前SourceFunction的并行度
    }
    /**
     *测试TestRichParallelSourceFun
     */
    public static void test04(StreamExecutionEnvironment env){
        DataStreamSource<Student> studentDataStreamSource = env.addSource(new TestRichParallelSourceFun());
        studentDataStreamSource.setParallelism(3);//想当于打印3个task，通过4个线程输出。
        studentDataStreamSource.print();
        System.out.println(studentDataStreamSource.getParallelism());//打印当前SourceFunction的并行度
    }
}
