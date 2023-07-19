package com.lxf.source;

import com.lxf.model.Student;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 *ParallelSourceFunction接口继承自SourceFunction类，但可以设置并行度
 */
public class TestParallelSounrceFun implements ParallelSourceFunction<Student> {

    boolean flag = true;
    @Override
    public void run(SourceContext<Student> sourceContext) throws Exception {

        String[] arrs = {"zhangsna","lisi","wangwu"};
        Random random = new Random();

        while(flag){
            for (int i = 0; i < 10; i++) {
                Student student = new Student();
                student.setId(123);
                student.setName(arrs[random.nextInt(arrs.length)]);
                student.setAge(random.nextInt()+100);
                sourceContext.collect(student);
            }
            Thread.sleep(5000);
        }

    }

    @Override
    public void cancel() {
        flag = false;
    }
}
