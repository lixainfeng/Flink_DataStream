package com.lxf.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class test {
    public static void main(String[] args) throws IOException {
        File file = new File("/Users/shuofeng/IdeaProjects/Test/flink-datastream/src/main/java/com/lxf/data/1.txt");
        FileInputStream fis=new FileInputStream(file);
        int result=fis.read();
        System.out.println(result);
    }
}
