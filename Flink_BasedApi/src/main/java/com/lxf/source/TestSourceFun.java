package com.lxf.source;

import com.lxf.model.Student;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * sourceFunction:单并行度，并且无法修改
 */
public class TestSourceFun implements SourceFunction<Student> {

    boolean flag = true;
    @Override
    public void run(SourceContext<Student> sourceContext) throws Exception {

        String[] arrs = {"zhangsna","lsi","wangwu"};
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

